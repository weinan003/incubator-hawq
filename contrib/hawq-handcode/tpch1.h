#ifndef __TPCH1__
#define __TPCH1__

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
#include "../backend/access/parquet/parquetam.c"
#include "../backend/cdb/cdbparquetcolumn.c"

#define SIZE 256 * 256

typedef struct lineitem_for_query1{
    double  l_quantity;
    double  l_extendedprice;
    double  l_discount;
    double  l_tax;
    char    l_returnflag;
    char    l_linestatus;
    char    l_shipdate[11];
} lineitem_for_query1;

typedef struct data_for_query1{
    lineitem_for_query1 lineitem_data;
    double sum_qty;
    double sum_base_price;
    double temp;
    double sum_disc_price;
    double sum_charge;
    double sum_discount;
    double count;
} data_for_query1;

struct DataItem {
   data_for_query1 data;
   char key1;
   char key2;
} DataItem;

struct DataItem* hashArray[SIZE];
struct DataItem* results[SIZE];
struct DataItem* dummyItem;
struct DataItem* item;

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

#define BUFFER_SCALE_FACTOR	1.2
#define BUFFER_SIZE_LIMIT_BEFORE_SCALED ((Size) ((MaxAllocSize) * 1.0 / (BUFFER_SCALE_FACTOR)))
#define BUFFER_SIZE 1024
#define TMP_COLUMNS 10
#define relname "lineitem"
#define MAX_TUPLE_NUM 10000000

lineitem_for_query1 read_tuples[MAX_TUPLE_NUM];
int	total_tuples_num = 0;
int	result_num = 0;
bool projs[16] = {0};

#endif
