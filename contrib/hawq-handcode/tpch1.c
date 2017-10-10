#include "tpch1.h"

extern text *cstring_to_text(const char *s);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(runtpch1);

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
    int32   fileSegNo;

    projs[4] = true;
    projs[5] = true;
    projs[6] = true;
    projs[7] = true;
    projs[8] = true;
    projs[9] = true;
    projs[10] = true;
    
    relid = RelnameGetRelid(relname);
    scan->rel = heap_open(relid, AccessShareLock);
    scan->segFile = (SegFile *) palloc0(sizeof(SegFile));
    MakeAOSegmentFileName(scan->rel, 1, -1, &fileSegNo, scan->segFile->filePath);
    scan->pqs_tupDesc = RelationGetDescr(scan->rel);
    scan->hawqAttrToParquetColChunks = (int*)palloc0(scan->pqs_tupDesc->natts * sizeof(int));
    scan->rowGroupReader.columnReaderCount = 0;
}

static void ReadFileMetadata(ParquetFormatScan *scan)
{
    /*
     * Now, the eof value is hard-coded, it should be the length of data file on hdfs.
     */
    int64 eof = 37558;
    scan->segFile->file = DoOpenFile(scan->segFile->filePath);
    readParquetFooter(scan->segFile->file, &(scan->segFile->parquetMetadata),
            &(scan->segFile->footerProtocol), eof, scan->segFile->filePath);
    ValidateParquetSegmentFile(scan->pqs_tupDesc, scan->hawqAttrToParquetColChunks,
            scan->segFile->parquetMetadata);
    scan->segFile->rowGroupCount = scan->segFile->parquetMetadata->blockCount;
    scan->segFile->rowGroupProcessedCount = 0;
}

static void EndScan(ParquetFormatScan *scan)
{
    heap_close(scan->rel, AccessShareLock);
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

    Datum *values = palloc0(sizeof(lineitem_for_query1)*40);
    bool *nulls = (bool *) palloc0(sizeof(bool) * scan->pqs_tupDesc->natts);

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
     * We should set the value of 'typoutput' and 'typisvarlena' through below way:
     * getTypeOutputInfo(scan->pqs_tupDesc->attrs[8]->atttypid, &typoutput, &typisvarlena);
     * But it need to scan the table: 'pg_type' each time. So we hard-code here to save time.
     */
    Oid         typoutput = 1045;
    bool        typisvarlena = false;
    char *tmp = OidOutputFunctionCall(typoutput, values[8]);
    read_tuples[total_tuples_num].l_returnflag = tmp[0];
    tmp = OidOutputFunctionCall(typoutput, values[9]);
    read_tuples[total_tuples_num].l_linestatus = tmp[0];
    typoutput = 1085;
    tmp = OidOutputFunctionCall(typoutput, values[10]);
    strncpy(read_tuples[total_tuples_num].l_shipdate, tmp, 10);

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

static void ReadDataFromLineitem()
{
    ParquetFormatScan scan;

    BeginScan(&scan);
    ReadFileMetadata(&scan);
    for (int i = 0 ; i < scan.segFile->rowGroupCount; i++) {
        ReadNextRowGroup(&scan);
        ReadTuplesFromRowGroup(&scan);
    }
    /* file is onpened in ReadFileMetadata() */
    FileClose(scan.segFile->file);
    EndScan(&scan);
}

int hashCode(char key1, char key2) {
    return key1 * 256 + key2;
}

void insert(char key1, char key2, data_for_query1 data) {

    struct DataItem *item = (struct DataItem*) malloc(sizeof(struct DataItem));
    item->data = data;
    item->key1 = key1;
    item->key2 = key2;

    //get the hash
    int hashIndex = hashCode(key1, key2);

    //move in array until an empty or deleted cell
    while (hashArray[hashIndex] != NULL && hashArray[hashIndex]->key1 != -1
            && hashArray[hashIndex]->key2 != -1) {
        //go to next cell
        ++hashIndex;

        //wrap around the table
        hashIndex %= SIZE;
    }

    hashArray[hashIndex] = item;
}

void display() {
    int i = 0;

    for (i = 0; i < SIZE; i++) {
        if (hashArray[i] != NULL) {
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

static void tpch_query1() {
    struct DataItem * entry = (struct DataItem*) malloc(sizeof(struct DataItem));
    entry->key1 = -1;
    entry->key2 = -1;
    entry->data.lineitem_data.l_quantity = -1;
    entry->data.lineitem_data.l_extendedprice = -1;
    entry->data.lineitem_data.l_discount = -1;
    entry->data.lineitem_data.l_tax = -1;
    entry->data.sum_qty = -1;
    entry->data.sum_base_price = -1;
    entry->data.temp = -1;
    entry->data.sum_disc_price = -1;
    entry->data.sum_charge = -1;
    entry->data.sum_discount = -1;
    entry->data.count = -1;

    for (int i = 0; i < total_tuples_num; i++) {
        entry->data.lineitem_data = read_tuples[i];
        // Calc sum(l_quantity)
        entry->data.sum_qty += read_tuples[i].l_quantity;
        // Calc sum(l_extendedprice)
        entry->data.sum_base_price += read_tuples[i].l_extendedprice;
        // Calc l_extendedprice * (1 - l_discount)
        entry->data.temp = read_tuples[i].l_extendedprice * (1 - read_tuples[i].l_discount);
        entry->data.sum_disc_price += entry->data.temp;
        // Calc l_extendedprice * (1 - l_discount) * (1 + l_tax)
        entry->data.sum_charge += entry->data.temp * (1 + read_tuples[i].l_tax);
        // Calc sum(l_discount) (later will divide by count to make avg())
        entry->data.sum_discount += read_tuples[i].l_discount;
        // Calc count() within the group
        entry->data.count++;
        // Insert into hash table;
        insert(read_tuples[i].l_returnflag, read_tuples[i].l_linestatus, entry->data);

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
        snprintf(buf, sizeof(buf), "%c", results[i]->data.lineitem_data.l_returnflag);
        values[0] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%c", results[i]->data.lineitem_data.l_linestatus);
        values[1] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.sum_qty);
        values[2] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.sum_base_price);
        values[3] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.sum_disc_price);
        values[4] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.sum_charge);
        values[5] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.sum_qty / results[i]->data.count);
        values[6] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.sum_base_price / results[i]->data.count);
        values[7] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.sum_discount / results[i]->data.count);
        values[8] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%lf", results[i]->data.count);
        values[9] = PointerGetDatum(cstring_to_text(buf));
        
        /* Build and return the tuple. */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else {
        SRF_RETURN_DONE(funcctx);
    }
}
