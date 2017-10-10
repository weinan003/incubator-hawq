typedef struct lineitem_for_query1{
    double  l_quantity;
    double  l_extendedprice;
    double  l_discount;
    double  l_tax;
    char    l_returnflag;
    char    l_linestatus;
    char    l_shipdate[11];
} lineitem_for_query1;

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
#define TMP_COLUMNS 2
#define relname "lineitem"
#define MAX_TUPLE_NUM 10000000

lineitem_for_query1 read_tuples[MAX_TUPLE_NUM];
int	total_tuples_num = 0;
bool projs[16] = {0};

