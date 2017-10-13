#include "tpch1.h"

extern text *cstring_to_text(const char *s);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(runtpch1);

Datum *values;
bool *nulls;
double function_call_time;

static File DoOpenFile(char *filePath)
{
    int fileFlags = O_RDONLY | PG_BINARY;
    int fileMode = 0400; /* File mode is S_IRUSR 00400 user has read permission */
    File file;
    file = PathNameOpenFile(filePath, fileFlags, fileMode);
    
    return file;
}

static void BeginScan(ParquetFormatScan *scan)
{
    Oid     relid;

    projs[4] = true;
    projs[5] = true;
    projs[6] = true;
    projs[7] = true;
    projs[8] = true;
    projs[9] = true;
    projs[10] = true;
    
    relid = RelnameGetRelid(relname);
    scan->rel = heap_open(relid, AccessShareLock);
    scan->pqs_tupDesc = RelationGetDescr(scan->rel);
    scan->hawqAttrToParquetColChunks = (int*)palloc0(scan->pqs_tupDesc->natts * sizeof(int));
    scan->rowGroupReader.columnReaderCount = 0;
}

static int GetSegFileInfo(int relid, int *segno, int64 *eof)
{
    char    sql[BUFFER_SIZE] = {'\0'};
    int     cnt;
    int     ret;
    int     proc;

    snprintf(sql, sizeof(sql), "SELECT * FROM pg_aoseg.pg_paqseg_%d", relid);
   
    SPI_connect();
    ret = SPI_exec(sql, 0);
    proc = SPI_processed;
    if (ret > 0 && SPI_tuptable != NULL) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        char    buf[BUFFER_SIZE];
        int     i, j;
        
        for (i = 0; i < proc; i++) {
            HeapTuple aosegTuple = SPI_tuptable->vals[i];
            Form_pg_aoseg pa;
            pa = (Form_pg_aoseg) GETSTRUCT(aosegTuple);
            segno[i] = pa->segno;
            eof[i] = (int64) pa->eof;
            elog(NOTICE, "segno[%d]=%d, eof[%d]=%ld", i, segno[i], i, eof[i]);
        }
    }
    SPI_finish();

    return proc;
}

static void ReadFileMetadata(ParquetFormatScan *scan, int segno, int64 eof)
{
    int32   fileSegNo;

    scan->segFile = (SegFile *) palloc0(sizeof(SegFile));
    MakeAOSegmentFileName(scan->rel, segno, -1, &fileSegNo, scan->segFile->filePath);
    scan->segFile->file = DoOpenFile(scan->segFile->filePath);
    scan->segFile->fileHandlerForFooter = DoOpenFile(scan->segFile->filePath);
    readParquetFooter(scan->segFile->fileHandlerForFooter, &(scan->segFile->parquetMetadata),
            &(scan->segFile->footerProtocol), eof, scan->segFile->filePath);
    ValidateParquetSegmentFile(scan->pqs_tupDesc, scan->hawqAttrToParquetColChunks,
            scan->segFile->parquetMetadata);
    scan->segFile->rowGroupCount = scan->segFile->parquetMetadata->blockCount;
    scan->segFile->rowGroupProcessedCount = 0;
    elog(NOTICE, "%s, file=%d, fileHandlerForFooter=%d", scan->segFile->filePath, scan->segFile->file, scan->segFile->fileHandlerForFooter);
}

static void ReadRowGroupInfo(ParquetFormatScan *scan)
{
    int colReaderNum = 0;

    readNextRowGroupInfo(scan->segFile->parquetMetadata, scan->segFile->footerProtocol);

    /*initialize parquet column reader: get column reader numbers*/
    for(int i = 0; i < scan->pqs_tupDesc->natts; i++){
        if(projs[i] == false){
            continue;
        }
        colReaderNum += scan->hawqAttrToParquetColChunks[i];
    }

    /*if not first row group, but column reader count not equals to previous row group,
     *report error*/
    if(scan->rowGroupReader.columnReaderCount == 0){
        scan->rowGroupReader.columnReaders = (ParquetColumnReader *)palloc0
                (colReaderNum * sizeof(ParquetColumnReader));
        scan->rowGroupReader.columnReaderCount = colReaderNum;
    }
    else if(scan->rowGroupReader.columnReaderCount != colReaderNum){
        ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
                errmsg("row group column information not compatible with previous"
                        "row groups in file %s for relation %s",
                        scan->segFile->filePath, relname)));
    }

    /*initialize parquet column readers*/
    scan->rowGroupReader.rowCount = scan->segFile->parquetMetadata->currentBlockMD->rowCount;
    scan->rowGroupReader.rowRead = 0;

    /*initialize individual column reader, by passing by parquet column chunk information*/
    int hawqColIndex = 0;
    int parquetColIndex = 0;
    for(int i = 0; i < scan->pqs_tupDesc->natts; i++){
        /*the number of parquet column chunks for this hawq attribute*/
        int parquetColChunkNum = scan->hawqAttrToParquetColChunks[i];

        /*if no projection, just skip this column*/
        if(projs[i] == false){
            parquetColIndex += parquetColChunkNum;
            continue;
        }

        for(int j = 0; j < parquetColChunkNum; j++){
            /*get the column chunk metadata information*/
            if (scan->rowGroupReader.columnReaders[hawqColIndex + j].columnMetadata == NULL) {
                scan->rowGroupReader.columnReaders[hawqColIndex + j].columnMetadata =
                        palloc0(sizeof(struct ColumnChunkMetadata_4C));
            }
            memcpy(scan->rowGroupReader.columnReaders[hawqColIndex + j].columnMetadata,
                    &(scan->segFile->parquetMetadata->currentBlockMD->columns[parquetColIndex + j]),
                    sizeof(struct ColumnChunkMetadata_4C));
        }
        hawqColIndex += parquetColChunkNum;
        parquetColIndex += parquetColChunkNum;
    }
}

static void ReadColumnData(ParquetColumnReader *columnReader, File file)
{
    struct ColumnChunkMetadata_4C* columnChunkMetadata = columnReader->columnMetadata;

    int64 firstPageOffset = columnChunkMetadata->firstDataPage;

    int64 columnChunkSize = columnChunkMetadata->totalSize;

    if ( columnChunkSize > MaxAllocSize ) 
    {
        ereport(ERROR,
                (errcode(ERRCODE_GP_INTERNAL_ERROR),
                 errmsg("parquet storage read error on reading column %s due to too large column chunk size: " INT64_FORMAT,
                         columnChunkMetadata->colName, columnChunkSize)));
    }

    int64 actualReadSize = 0;

    /*reuse the column reader data buffer to avoid memory re-allocation*/
    if(columnReader->dataLen == 0)
    {
        columnReader->dataLen = columnChunkSize < BUFFER_SIZE_LIMIT_BEFORE_SCALED ?
                                columnChunkSize * BUFFER_SCALE_FACTOR :
                                MaxAllocSize-1;

        columnReader->dataBuffer = (char*) palloc0(columnReader->dataLen);
    }
    else if(columnReader->dataLen < columnChunkSize)
    {
        columnReader->dataLen = columnChunkSize < BUFFER_SIZE_LIMIT_BEFORE_SCALED ?
                                columnChunkSize * BUFFER_SCALE_FACTOR :
                                MaxAllocSize-1;

        columnReader->dataBuffer = (char*) repalloc(columnReader->dataBuffer, columnReader->dataLen);
        memset(columnReader->dataBuffer, 0, columnReader->dataLen);
    }

    char *buffer = columnReader->dataBuffer;

    int64 numValuesInColumnChunk = columnChunkMetadata->valueCount;

    int64 numValuesProcessed = 0;

    /*seek to the beginning of the column chunk*/
    int64 seekResult = FileSeek(file, firstPageOffset, SEEK_SET);
    if (seekResult != firstPageOffset)
    {
        ereport(ERROR,
                (errcode_for_file_access(),
                 errmsg("file seek error to position " INT64_FORMAT ": %s", firstPageOffset, strerror(errno)),
                 errdetail("%s", HdfsGetLastError())));
    }

    /*recursively read, until get the total column chunk data out*/
    while(actualReadSize < columnChunkSize)
    {
        /*read out all the buffer of the column chunk*/
        int columnChunkLen = FileRead(file, buffer + actualReadSize, columnChunkSize - actualReadSize);
        if (columnChunkLen < 0) {
            ereport(ERROR,
                    (errcode_for_file_access(),
                            errmsg("parquet storage read error on reading column %s ", columnChunkMetadata->colName),
                            errdetail("%s", HdfsGetLastError())));
        }
        actualReadSize += columnChunkLen;
    }

    /*only if first column reader set, just need palloc the data pages*/
    if(columnReader->dataPageCapacity == 0)
    {
        columnReader->dataPageCapacity = DAFAULT_DATAPAGE_NUM_PER_COLUMNCHUNK;
        columnReader->dataPageNum = 0;
        columnReader->dataPages = (ParquetDataPage)palloc0
            (columnReader->dataPageCapacity *sizeof(struct ParquetDataPage_S));
    }

    /* read all the data pages of the column chunk */
    while(numValuesProcessed < numValuesInColumnChunk)
    {
        ParquetPageHeader pageHeader;
        ParquetDataPage dataPage;

        uint32_t header_size = (char *) columnReader->dataBuffer + columnChunkSize - buffer;
        if (readPageMetadata((uint8_t*) buffer, &header_size, /*compact*/1, &pageHeader) < 0)
        {
            ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
                errmsg("thrift deserialize failure on reading page header of column %s ",
                        columnChunkMetadata->colName)));
        }

        buffer += header_size;

        /*just process data page now*/
        if(pageHeader->page_type != DATA_PAGE){
            if(pageHeader->page_type == DICTIONARY_PAGE) {
                ereport(ERROR, (errcode(ERRCODE_GP_INTERNAL_ERROR),
                                errmsg("HAWQ does not support dictionary page type resolver for Parquet format in column \'%s\' ",
                                        columnChunkMetadata->colName)));
            }
            buffer += pageHeader->compressed_page_size;
            continue;
        }

        if(columnReader->dataPageNum == columnReader->dataPageCapacity)
        {
            columnReader->dataPages = (ParquetDataPage)repalloc(
                    columnReader->dataPages,
                    2 * columnReader->dataPageCapacity * sizeof(struct ParquetDataPage_S));
            
            memset(columnReader->dataPages + columnReader->dataPageCapacity, 0,
                   columnReader->dataPageCapacity * sizeof(struct ParquetDataPage_S));

            columnReader->dataPageCapacity *= 2;
        }

        dataPage = &(columnReader->dataPages[columnReader->dataPageNum]);
        dataPage->header    = pageHeader;

        dataPage->data      = (uint8_t *) buffer;
        buffer += pageHeader->compressed_page_size;

        numValuesProcessed += pageHeader->num_values;
        columnReader->dataPageNum++;
    }

    columnReader->currentPageValueRemained = 0; /* indicate to read next page */
    columnReader->dataPageProcessed = 0;

    if (columnChunkMetadata->r > 0)
    {
        consume(columnReader);
    }    
}

static void ReadRowGroupData(ParquetFormatScan *scan)
{
    /*scan the file to get next row group data*/
    ParquetColumnReader *columnReaders = scan->rowGroupReader.columnReaders;
    File file = scan->segFile->file;
    for(int i = 0; i < scan->rowGroupReader.columnReaderCount; i++){
        ReadColumnData(&(columnReaders[i]), file);
    }
    scan->segFile->rowGroupProcessedCount++;
}

static void ReadNextRowGroup(ParquetFormatScan *scan)
{
    ReadRowGroupInfo(scan);
    ReadRowGroupData(scan);
}

static void ReadTupleFromRowGroup(ParquetFormatScan *scan)
{
    /*
     * get the next item (tuple) from the row group
     */
    scan->rowGroupReader.rowRead++;


    int colReaderIndex = 0;
    for(int i = 0; i < scan->pqs_tupDesc->natts; i++)
    {
        if(projs[i] == false)
        {
            nulls[i] = true;
            continue;
        }

        ParquetColumnReader *nextReader =
            &scan->rowGroupReader.columnReaders[colReaderIndex];
        int hawqTypeID = scan->pqs_tupDesc->attrs[i]->atttypid;

        if(scan->hawqAttrToParquetColChunks[i] == 1)
        {
            ParquetColumnReader_readValue(nextReader, &values[i], &nulls[i], hawqTypeID);
        }
        else
        {
            /*
             * Because there are some memory reused inside the whole column reader, so need
             * to switch the context from PerTupleContext to rowgroup->context
             */
            switch(hawqTypeID)
            {
                case HAWQ_TYPE_POINT:
                    ParquetColumnReader_readPoint(nextReader, &values[i], &nulls[i]);
                    break;
                case HAWQ_TYPE_PATH:
                    ParquetColumnReader_readPATH(nextReader, &values[i], &nulls[i]);
                    break;
                case HAWQ_TYPE_LSEG:
                    ParquetColumnReader_readLSEG(nextReader, &values[i], &nulls[i]);
                    break;
                case HAWQ_TYPE_BOX:
                    ParquetColumnReader_readBOX(nextReader, &values[i], &nulls[i]);
                    break;
                case HAWQ_TYPE_CIRCLE:
                    ParquetColumnReader_readCIRCLE(nextReader, &values[i], &nulls[i]);
                    break;
                case HAWQ_TYPE_POLYGON:
                    ParquetColumnReader_readPOLYGON(nextReader, &values[i], &nulls[i]);
                    break;
                default:
                    /* TODO array type */
                    /* TODO UDT */
                    Insist(false);
                    break;
            }
        }
        colReaderIndex += scan->hawqAttrToParquetColChunks[i];
    }

    /*
     * construct a row tuple
     */
    read_tuples[total_tuples_num].l_quantity = DatumGetFloat8(values[4]);
    read_tuples[total_tuples_num].l_extendedprice = DatumGetFloat8(values[5]);
    read_tuples[total_tuples_num].l_discount = DatumGetFloat8(values[6]);
    read_tuples[total_tuples_num].l_tax = DatumGetFloat8(values[7]);

    /*
     * More details can refer to printtup()
     */
    char    *tmp;
    int     len;
    char    *dptr = DatumGetPointer(values[8]);
    tmp = VARDATA(dptr);
    len = VARSIZE(dptr) - VARHDRSZ;
    read_tuples[total_tuples_num].l_returnflag = tmp[0];

    dptr = DatumGetPointer(values[9]);
    tmp = VARDATA(dptr);
    read_tuples[total_tuples_num].l_linestatus = tmp[0];

    /*
     * We should set the value of 'typoutput' and 'typisvarlena' through below way:
     * getTypeOutputInfo(scan->pqs_tupDesc->attrs[8]->atttypid, &typoutput, &typisvarlena);
     * But it need to scan the table: 'pg_type' each time. So we hard-code here to save time.
     */
    Oid     typoutput = 1085;
    bool    typisvarlena = false;
    struct timeval t_begin, t_end;
    gettimeofday(&t_begin, NULL);
    tmp = OidOutputFunctionCall(typoutput, values[10]);
    memcpy(read_tuples[total_tuples_num].l_shipdate, tmp, 10);
    gettimeofday(&t_end, NULL);
    function_call_time += (t_end.tv_sec - t_begin.tv_sec) * 1000000.0 + (t_end.tv_usec - t_begin.tv_usec);

/*
    elog(NOTICE, "read_tuples[%d].l_quantity=%lf", total_tuples_num, read_tuples[total_tuples_num].l_quantity);
    elog(NOTICE, "read_tuples[%d].l_extendedprice=%lf", total_tuples_num, read_tuples[total_tuples_num].l_extendedprice);
    elog(NOTICE, "read_tuples[%d].l_discount=%lf", total_tuples_num, read_tuples[total_tuples_num].l_discount);
    elog(NOTICE, "read_tuples[%d].l_tax=%lf", total_tuples_num, read_tuples[total_tuples_num].l_tax);
    elog(NOTICE, "read_tuples[%d].l_returnflag=%c", total_tuples_num, read_tuples[total_tuples_num].l_returnflag);
    elog(NOTICE, "read_tuples[%d].l_linestatus=%c", total_tuples_num, read_tuples[total_tuples_num].l_linestatus);
    elog(NOTICE, "read_tuples[%d].l_shipdate=%s", total_tuples_num, read_tuples[total_tuples_num].l_shipdate);
*/

    total_tuples_num ++;
}

static void ReadTuplesFromRowGroup(ParquetFormatScan *scan)
{
    while (scan->rowGroupReader.rowRead < scan->rowGroupReader.rowCount){
        ReadTupleFromRowGroup(scan);   
    }
    
    ParquetRowGroupReader_FinishedScanRowGroup(&scan->rowGroupReader);
}

static void FinishReadSegFile(ParquetFormatScan *scan)
{
    /* file is onpened in ReadFileMetadata() */
    Assert(scan->segFile->file != -1);
    FileClose(scan->segFile->file);
    scan->segFile->file = -1;
    Assert(scan->segFile->fileHandlerForFooter!= -1);
    FileClose(scan->segFile->fileHandlerForFooter);
    scan->segFile->fileHandlerForFooter = -1;
    /*free parquetMetadata and footerProtocol*/
    //endDeserializerFooter(scan.segFile->parquetMetadata, &(scan.segFile->footerProtocol));
    freeFooterProtocol(scan->segFile->footerProtocol);
    scan->segFile->footerProtocol = NULL;
    if (scan->segFile->parquetMetadata != NULL) {
        freeParquetMetadata(scan->segFile->parquetMetadata);
        scan->segFile->parquetMetadata = NULL;
    }
    free(scan->segFile);
}

static void EndScan(ParquetFormatScan *scan)
{
    //free(scan->hawqAttrToParquetColChunks);
    heap_close(scan->rel, AccessShareLock);
}

static void ReadDataFromLineitem()
{
    ParquetFormatScan scan;
    int segNum;
    int segno[MAX_SEG_NUM];
    int64 eof[MAX_SEG_NUM];

    total_tuples_num = 0;
    
    BeginScan(&scan);
    segNum = GetSegFileInfo(scan.rel->rd_id, segno, eof);
    values = palloc0(sizeof(lineitem_for_query1)*40);
    nulls = (bool *) palloc0(sizeof(bool) * scan.pqs_tupDesc->natts);
    function_call_time = 0;
    for (int i = 0; i < segNum; i++) {
        ReadFileMetadata(&scan, segno[i], eof[i]);
        for (int j = 0 ; j < scan.segFile->rowGroupCount; j++) {
            ReadNextRowGroup(&scan);
            ReadTuplesFromRowGroup(&scan);
        }
    }
    elog(NOTICE, "function_call_time = %lf ms", function_call_time/1000.0);
    EndScan(&scan);
}

int hashCode(char key1, char key2) {
    return key1 * 256 + key2;
}

void insert(char key1, char key2, data_for_query1 data) {

    //get the hash
    int hashIndex = hashCode(key1, key2);

    // Calc sum(l_quantity)
    hashArray[hashIndex].key1 = key1;
    hashArray[hashIndex].key2 = key2;
    hashArray[hashIndex].data.lineitem_data = data.lineitem_data;

    hashArray[hashIndex].data.sum_qty += data.lineitem_data.l_quantity;
    // Calc sum(l_extendedprice)
    hashArray[hashIndex].data.sum_base_price += data.lineitem_data.l_extendedprice;
    // Calc l_extendedprice * (1 - l_discount)
    hashArray[hashIndex].data.temp = data.lineitem_data.l_extendedprice * (1 - data.lineitem_data.l_discount);
    hashArray[hashIndex].data.sum_disc_price += hashArray[hashIndex].data.temp;
    // Calc l_extendedprice * (1 - l_discount) * (1 + l_tax)
    hashArray[hashIndex].data.sum_charge += hashArray[hashIndex].data.temp * (1 + data.lineitem_data.l_tax);
    // Calc sum(l_discount) (later will divide by count to make avg())
    hashArray[hashIndex].data.sum_discount += data.lineitem_data.l_discount;
    // Calc count() within the group
    hashArray[hashIndex].data.count++;
}

void display() {

    int i = 0;

    result_num = 0;
    for (i = 0; i < SIZE; i++) {
        if (hashArray[i].key1 != 0 && hashArray[i].key2 != 0) {
            /*
            elog(NOTICE,"l_returnflag:%18c\n, l_linestatus:%18c\n , sum(l_quantity):%18lf\n , "
                    "sum(base_price):%18lf\n , sum(disc_price):%18lf\n , sum(charge):%18lf\n ,"
                    "avg(l_quantity):%18lf\n , avg(base_price):%18lf\n , avg(discount):%18lf\n , "
                    "count_order:%18lf \n",
                    hashArray[i]->data.lineitem_data.l_returnflag,
                    hashArray[i]->data.lineitem_data.l_linestatus,
                    hashArray[i]->data.sum_qty,
                    hashArray[i]->data.sum_base_price,
                    hashArray[i]->data.sum_disc_price,
                    hashArray[i]->data.sum_charge,
                    hashArray[i]->data.sum_qty / hashArray[i]->data.count,
                    hashArray[i]->data.sum_base_price / hashArray[i]->data.count,
                    hashArray[i]->data.sum_discount / hashArray[i]->data.count,
                    hashArray[i]->data.count);
            */
            results[result_num] = hashArray[i];
            result_num ++;
        }
    }
}


void destroy_hashArray() {

    for (int i = 0; i < SIZE; i++) {
        hashArray[i].key1 = 0;
        hashArray[i].key2 = 0;
        hashArray[i].data.sum_qty = 0;
        hashArray[i].data.sum_base_price = 0;
        hashArray[i].data.temp = 0;
        hashArray[i].data.sum_disc_price = 0;
        hashArray[i].data.sum_charge = 0;
        hashArray[i].data.sum_discount = 0;
        hashArray[i].data.count = 0;
        hashArray[i].data.lineitem_data.l_quantity = 0;
        hashArray[i].data.lineitem_data.l_extendedprice = 0;
        hashArray[i].data.lineitem_data.l_discount = 0;
        hashArray[i].data.lineitem_data.l_tax = 0;
        hashArray[i].data.lineitem_data.l_returnflag = 0;
        hashArray[i].data.lineitem_data.l_linestatus = 0;
        memset(hashArray[i].data.lineitem_data.l_shipdate, 0, 11);
    }

}

static void tpch_query1() {

    struct DataItem entry;
    entry.key1 = 0;
    entry.key2 = 0;
    entry.data.sum_qty = 0;
    entry.data.sum_base_price = 0;
    entry.data.temp = 0;
    entry.data.sum_disc_price = 0;
    entry.data.sum_charge = 0;
    entry.data.sum_discount = 0;
    entry.data.count = 0;

    for (int i = 0; i < total_tuples_num; i++) {
        entry.data.lineitem_data = read_tuples[i];
        // Insert into hash array.
        insert(read_tuples[i].l_returnflag, read_tuples[i].l_linestatus, entry.data);

    }
    display();
}

Datum runtpch1(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx    = NULL;
    Datum            result;
    MemoryContext    oldcontext = NULL;
    HeapTuple        tuple      = NULL;
    lineitem_for_query1	*args;

    if (SRF_IS_FIRSTCALL())
    {

        funcctx = SRF_FIRSTCALL_INIT();

        /* Switch context when allocating stuff to be used in later calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        args = palloc0(sizeof(struct lineitem_for_query1));
        funcctx->user_fctx = args;

        TupleDesc tupledesc = CreateTemplateTupleDesc(
                                    TMP_COLUMNS,
                                    false);
        TupleDescInitEntry(tupledesc, (AttrNumber) 1, "l_returnflag", TEXTOID,  -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 2, "l_linestatus", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 3, "sum(l_quantity)", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 4, "sum(base_price)", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 5, "sum(disc_price)", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 6, "sum(charge)", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 7, "avg(l_quantity)", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 8, "avg(base_price)", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 9, "avg(discount)", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 10, "count_order", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupledesc);

        ReadDataFromLineitem();

		tpch_query1();

        funcctx->max_calls = result_num;

        /* Return to original context when allocating transient memory */
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls)
    {
        Datum       values[TMP_COLUMNS];
        bool        nulls[TMP_COLUMNS];
        char        buf[BUFFER_SIZE] = {'\0'};
        int         i;

        for (i=0;i<TMP_COLUMNS;i++)
        {
            nulls[i] = false;
        }

        args = funcctx->user_fctx;

        i = funcctx->call_cntr;
        snprintf(buf, sizeof(buf), "%c", results[i].data.lineitem_data.l_returnflag);
        values[0] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%c", results[i].data.lineitem_data.l_linestatus);
        values[1] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.sum_qty);
        values[2] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.sum_base_price);
        values[3] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.sum_disc_price);
        values[4] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.sum_charge);
        values[5] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.sum_qty / results[i].data.count);
        values[6] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.sum_base_price / results[i].data.count);
        values[7] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.sum_discount / results[i].data.count);
        values[8] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i].data.count);
        values[9] = PointerGetDatum(cstring_to_text(buf));
        
        /* Build and return the tuple. */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else {
        destroy_hashArray();
        SRF_RETURN_DONE(funcctx);
    }
}
