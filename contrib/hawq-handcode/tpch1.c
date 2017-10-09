#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "cdb/cdbparquetfooterprocessor.h"
#include "cdb/cdbparquetfooterserializer.h"
#include "access/parquetmetadata_c++/MetadataInterface.h"
#include "cdb/cdbparquetrowgroup.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

#include "snappy-c.h"
#include "zlib.h"

extern text *cstring_to_text(const char *s);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(runtpch1);


#define BUFFER_SCALE_FACTOR	1.2
#define BUFFER_SIZE_LIMIT_BEFORE_SCALED ((Size) ((MaxAllocSize) * 1.0 / (BUFFER_SCALE_FACTOR)))
#define BUFFER_SIZE 1024
#define TMP_COLUMNS 2
#define relname "lineitem"
#define MAX_TUPLE_NUM 10000000

typedef struct lineitem_for_query1{
    double  l_quantity;
    double  l_extendedprice;
    double  l_discount;
    double  l_tax;
    char    l_returnflag;
    char    l_linestatus;
    char    l_shipdate[11];
} lineitem_for_query1;

lineitem_for_query1 read_tuples[MAX_TUPLE_NUM];
int	total_tuples_num = 0;
bool projs[16] = {0};

typedef struct SegFile{
    char    filePath[1000];
    File    file;
    ParquetMetadata parquetMetadata;
    CompactProtocol *footerProtocol;
    int     rowGroupCount;
    int     rowGroupProcessedCount;
} SegFile;

typedef struct ParquetFormatScan{
    Relation    rel;
    SegFile    *segFile;
    TupleDesc   pqs_tupDesc;
    int         *hawqAttrToParquetColChunks;
    ParquetRowGroupReader   rowGroupReader;
} ParquetFormatScan;

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

/**
 * get parquet column chunks number for a hawq attribute
 */
static int getColumnChunkNumForHawqAttr(struct FileField_4C* hawqAttr){
	int resultNum = 0;
	if(hawqAttr->num_children == 0){
		return 1;
	}
	else{
		for(int i = 0; i < hawqAttr->num_children; i++){
			resultNum += getColumnChunkNumForHawqAttr(&(hawqAttr->children[i]));
		}
		return resultNum;
	}
}

static void
readRepetitionAndDefinitionLevels(ParquetColumnReader *reader)
{
    if (reader->currentPage->repetition_level_reader)
    {
        reader->repetitionLevel =
            RLEDecoder_ReadInt(reader->currentPage->repetition_level_reader);
    }

    if (reader->currentPage->definition_level_reader)
    {
        reader->definitionLevel =
            RLEDecoder_ReadInt(reader->currentPage->definition_level_reader);
    }

    reader->currentPageValueRemained--;
}

/*
 * Decode raw data of chunk's current page into corresponding r/d/bool reader
 * and values_buffer, uncompress data if needed.
 */
static void
decodeCurrentPage(ParquetColumnReader *columnReader)
{
	ColumnChunkMetadata_4C *chunkmd;
	ParquetDataPage page;
	ParquetPageHeader header;
	uint8_t			*buf;	/* store uncompressed or decompressed page data */
	MemoryContext	oldContext;

	chunkmd	= columnReader->columnMetadata;
	page	= columnReader->currentPage;
	header	= page->header;

	oldContext = MemoryContextSwitchTo(columnReader->memoryContext);

	/*----------------------------------------------------------------
	 * Decompress raw data. After this, buf & page->data points to
	 * uncompressed/decompressed data.
	 *----------------------------------------------------------------*/
	if (chunkmd->codec == UNCOMPRESSED)
	{
		buf = page->data;
	}
	else
	{
		/*
		 * make `buf` points to decompressed buffer,
		 * which should be large enough for uncompressed data.
		 */
		if (chunkmd->r > 0)
		{
			/* repeatable column creates decompression buffer for each page */
			buf = palloc0(header->uncompressed_page_size);
		}
		else
		{
			/* non-repeatable column reuses pageBuffer for decompression */
			if (columnReader->pageBuffer == NULL)
			{
				columnReader->pageBufferLen = header->uncompressed_page_size * BUFFER_SCALE_FACTOR;
				columnReader->pageBuffer = palloc0(columnReader->pageBufferLen);
			}
			else if (columnReader->pageBufferLen < header->uncompressed_page_size)
			{
				columnReader->pageBufferLen = header->uncompressed_page_size * BUFFER_SCALE_FACTOR;
				columnReader->pageBuffer = repalloc(columnReader->pageBuffer, columnReader->pageBufferLen);
			}
			buf = (uint8_t *) columnReader->pageBuffer;
		}

		/*
		 * call corresponding decompress routine
		 */
		switch (chunkmd->codec)
		{
			case SNAPPY:
			{
				//size_t uncompressedLen;
				//if (snappy_uncompressed_length((char *) page->data,
				//							   header->compressed_page_size,
				//							   &uncompressedLen) != SNAPPY_OK)
				//{
				//	ereport(ERROR,
				//			(errcode(ERRCODE_GP_INTERNAL_ERROR),
				//			 errmsg("invalid snappy compressed data for column %s, page number %d",
				//					chunkmd->colName, columnReader->dataPageProcessed)));
				//}

				//Insist(uncompressedLen == header->uncompressed_page_size);

				//if (snappy_uncompress((char *) page->data,		header->compressed_page_size,
				//					  (char *) buf,				&uncompressedLen) != SNAPPY_OK)
				//{
				//	ereport(ERROR,
				//			(errcode(ERRCODE_GP_INTERNAL_ERROR),
				//			 errmsg("failed to decompress snappy data for column %s, page number %d, "
				//					"uncompressed size %d, compressed size %d",
				//					chunkmd->colName, columnReader->dataPageProcessed,
				//					header->uncompressed_page_size, header->compressed_page_size)));
				//}

				//page->data = buf;
				break;
			}
			case GZIP:
			{
				//int ret;
				///* 15(default windowBits for deflate) + 16(ouput GZIP header/tailer) */
				//const int windowbits = 31;

				//z_stream stream;
				//stream.zalloc	= Z_NULL;
				//stream.zfree	= Z_NULL;
				//stream.opaque	= Z_NULL;
				//stream.avail_in	= header->compressed_page_size;
				//stream.next_in	= (Bytef *) page->data;

				//ret = inflateInit2(&stream, windowbits);
				//if (ret != Z_OK)
				//{
				//	ereport(ERROR,
				//			(errcode(ERRCODE_GP_INTERNAL_ERROR),
				//			 errmsg("zlib inflateInit2 failed: %s", stream.msg)));
				//}

				//size_t uncompressedLen = header->uncompressed_page_size;

				//stream.avail_out = uncompressedLen;
				//stream.next_out  = (Bytef *) buf;
				//ret = inflate(&stream, Z_FINISH);
				//if (ret != Z_STREAM_END)
				//{
				//	ereport(ERROR,
				//			(errcode(ERRCODE_GP_INTERNAL_ERROR),
				//			 errmsg("zlib inflate failed: %s", stream.msg)));

				//}
				///* should fill all uncompressed_page_size bytes */
				//Assert(stream.avail_out == 0);

				//inflateEnd(&stream);

				//page->data = buf;
				break;
			}
			case LZO:
				/* TODO */
				Insist(false);
				break;
			default:
				Insist(false);
				break;
		}
	}

	/*----------------------------------------------------------------
	 * get r/d/value part
	 *----------------------------------------------------------------*/
	if(chunkmd->r != 0)
	{
		int num_repetition_bytes = /*le32toh(*/ *((uint32_t *) buf) /*)*/;
		buf += 4;

		page->repetition_level_reader = (RLEDecoder *) palloc0(sizeof(RLEDecoder));
		RLEDecoder_Init(page->repetition_level_reader,
						widthFromMaxInt(chunkmd->r),
						buf,
						num_repetition_bytes);

		buf += num_repetition_bytes;
	}

	if(chunkmd->d != 0)
	{
		int num_definition_bytes = /*le32toh(*/ *((uint32_t *) buf) /*)*/;
		buf += 4;

		page->definition_level_reader = (RLEDecoder *) palloc0(sizeof(RLEDecoder));
		RLEDecoder_Init(page->definition_level_reader,
						widthFromMaxInt(chunkmd->d),
						buf,
						num_definition_bytes);

		buf += num_definition_bytes;
	}

	if (chunkmd->type == BOOLEAN)
	{
		page->bool_values_reader = (ByteBasedBitPackingDecoder *)
				palloc0(sizeof(ByteBasedBitPackingDecoder));
		BitPack_InitDecoder(page->bool_values_reader, buf, /*bitwidth=*/1);
	}
	else
	{
		page->values_buffer = buf;
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * End the current value, move to next r/d/value.
 * Should be called after current value is read.
 */
static void
consume(ParquetColumnReader *columnReader)
{
	/* make sure we have values to read in current page */
	if (columnReader->currentPageValueRemained == 0)
	{
		if (columnReader->dataPageProcessed >= columnReader->dataPageNum)
		{
			/* next r must be 0 when reached chunk end */
			columnReader->repetitionLevel = 0;
			return;
		}

		/* read next page */
		columnReader->currentPage = &columnReader->dataPages[columnReader->dataPageProcessed];
		decodeCurrentPage(columnReader);

		columnReader->currentPageValueRemained = columnReader->currentPage->header->num_values;
		columnReader->dataPageProcessed++;
	}

	readRepetitionAndDefinitionLevels(columnReader);
}

/**
 * Validate the correctness of parquet file, judge whether it corresponds to hawq table
 * @projs			the projection of the scan
 * @hawqTupDescs	the tuple description of hawq table
 * @parquetMetadata	parquet metadata read from parquet file
 */
static bool ValidateParquetSegmentFile(TupleDesc hawqTupDescs,
		int *hawqAttrToParquetColChunks, ParquetMetadata parquetMetadata)
{
	int numHawqAttrs = hawqTupDescs->natts;
	if(numHawqAttrs != parquetMetadata->fieldCount){
		return false;
	}

	for(int i = 0; i < numHawqAttrs; i++){
		hawqAttrToParquetColChunks[i] = getColumnChunkNumForHawqAttr(&(parquetMetadata->pfield[i]));
	}

	return true;
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

    elog(NOTICE, "read_tuples[%d].l_quantity=%lf", total_tuples_num, read_tuples[total_tuples_num].l_quantity);
    elog(NOTICE, "read_tuples[%d].l_extendedprice=%lf", total_tuples_num, read_tuples[total_tuples_num].l_extendedprice);
    elog(NOTICE, "read_tuples[%d].l_discount=%lf", total_tuples_num, read_tuples[total_tuples_num].l_discount);
    elog(NOTICE, "read_tuples[%d].l_tax=%lf", total_tuples_num, read_tuples[total_tuples_num].l_tax);
    elog(NOTICE, "read_tuples[%d].l_returnflag=%c", total_tuples_num, read_tuples[total_tuples_num].l_returnflag);
    elog(NOTICE, "read_tuples[%d].l_linestatus=%c", total_tuples_num, read_tuples[total_tuples_num].l_linestatus);
    elog(NOTICE, "read_tuples[%d].l_shipdate=%s", total_tuples_num, read_tuples[total_tuples_num].l_shipdate);

    total_tuples_num ++;
}

static void ReadTuplesFromRowGroup(ParquetFormatScan *scan)
{
    while (scan->rowGroupReader.rowRead < scan->rowGroupReader.rowCount){
    		elog(NOTICE, "rowRead=%d, rowCount=%d", scan->rowGroupReader.rowRead, scan->rowGroupReader.rowCount);
        ReadTupleFromRowGroup(scan);   
    }
    
    ParquetRowGroupReader_FinishedScanRowGroup(&scan->rowGroupReader);
}

static void ReadDataFromLineitem()
{
    ParquetFormatScan scan;

    BeginScan(&scan);
    ReadFileMetadata(&scan);
    elog(NOTICE, "rowGroupCount=%d", scan.segFile->rowGroupCount);
    for (int i = 0 ; i < scan.segFile->rowGroupCount; i++) {
        ReadNextRowGroup(&scan);
        ReadTuplesFromRowGroup(&scan);
    }
    EndScan(&scan);
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
        TupleDescInitEntry(tupledesc, (AttrNumber) 1,  "id",   TEXTOID,  -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 2,  "text", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupledesc);

        funcctx->max_calls = 6;

        ReadDataFromLineitem();

        /* Return to original context when allocating transient memory */
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls)
    {
        Datum       values[TMP_COLUMNS];
        bool        nulls[TMP_COLUMNS];
        char        buf[BUFFER_SIZE] = {'\0'};

        for (int i=0;i<TMP_COLUMNS;i++)
        {
            nulls[i] = false;
        }

        args = funcctx->user_fctx;

        snprintf(buf, sizeof(buf), "%d", 1);
        //values[0] = Int32GetDatum(args->id);
        //values[1] = CStringGetDatum(args->text);
        values[0] = PointerGetDatum(cstring_to_text(buf));
        values[1] = PointerGetDatum(cstring_to_text("aaa"));
        /* Build and return the tuple. */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else {
        SRF_RETURN_DONE(funcctx);
    }
}
