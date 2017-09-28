#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

extern text *cstring_to_text(const char *s);

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(runtpch1);

#define BUFFER_SIZE 1024
#define relname "lineitem"
#define TUPLE_NUM 10000000

typedef struct lineitem_for_query1{
    double  l_quantity;
    double  l_extendedprice;
    double  l_discount;
    double  l_tax;
    char    l_returnflag;
    char    l_linestatus;
    char    l_shipdate[11];
} lineitem_for_query1;

lineitem_for_query1 read_tuples[TUPLE_NUM];
bool project[16] = {0};

typedef struct SegFile{
    char    filePath[1000];
    File    file;
    ParquetMetadata parquetMetadata;
    CompactProtocol *footerProtocol;
} SegFile;

typedef struct ParquetScan{
    Relation    rel;
    SegFile    *segFile;
} ParquetScan;

static File DoOpenFile(char *filePath)
{
    int fileFlags = O_RDONLY | PG_BINARY;
    int fileMode = 0400; /* File mode is S_IRUSR 00400 user has read permission */
    File file;
    file = PathNameOpenFile(filePath, fileFlags, fileMode);
    
    return file;
}

static void BeginScan(ParquetScan *scan)
{
    Oid     relid;
    int32   fileSegNo;

    project[4] = 1;
    project[5] = 1;
    project[6] = 1;
    project[7] = 1;
    project[8] = 1;
    project[9] = 1;
    project[10] = 1;
    
    relid = RelnameGetRelid(relname);
    scan->rel = heap_open(relid, AccessShareLock);
    scan->segFile = (SegFile *) malloc(sizeof(SegFile));
    MakeAOSegmentFileName(scan->rel, 1, -1, &fileSegNo, scan->segFile->filePath);
    scan->segFile->file = DoOpenFile(scan->segFile->filePath);
}

static void ReadFileMeatadata(ParquetScan scan)
{
    readParquetFooter(scan->segFile->file, &(scan->segFile->parquetMetadata),
            &(scan->segFile->footerProtocol), logicalEof, filePathName);
}

static void EndScan(Relation rel)
{
    heap_close(rel);
}


static void ReadRowGroupInfo()
{
}

static void ReadRowGroupData()
{
}

static bool ReadNextRowGroup()
{
}

static void ReadDataFromLineitem()
{
    ParquetScan scan;

    BeginScan(&scan);
    ReadFileMetadata(&scan);
    EndScan(&scan);
}

Datum runtpch1(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx    = NULL;
    Datum            result;
    MemoryContext    oldcontext = NULL;
    HeapTuple        tuple      = NULL;
    struct tpch1_args*  args;

    if (SRF_IS_FIRSTCALL())
    {

        funcctx = SRF_FIRSTCALL_INIT();

        /* Switch context when allocating stuff to be used in later calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        args = palloc0(sizeof(struct tpch1_args));
        funcctx->user_fctx = args;

        TupleDesc tupledesc = CreateTemplateTupleDesc(
                                    TMP_COLUMNS,
                                    false);
        TupleDescInitEntry(tupledesc, (AttrNumber) 1,  "id",   TEXTOID,  -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 2,  "text", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupledesc);

        funcctx->max_calls = 6;

        /* Return to original context when allocating transient memory */
        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls)
    {
        Datum       values[TMP_COLUMNS];
        bool        nulls[TMP_COLUMNS];
        char        buf[TMP_BUFFER] = {'\0'};

        for (int i=0;i<TMP_COLUMNS;i++)
        {
            nulls[i] = false;
        }

        args = funcctx->user_fctx;
        args->id = funcctx->call_cntr;
        snprintf(args->text, sizeof(args->text), "%s", "aaa");

        snprintf(buf, sizeof(buf), "%d", args->id);
        //values[0] = Int32GetDatum(args->id);
        //values[1] = CStringGetDatum(args->text);
        values[0] = PointerGetDatum(cstring_to_text(buf));
        values[1] = PointerGetDatum(cstring_to_text(args->text));
        /* Build and return the tuple. */
        elog(NOTICE, "values[0]=%p", values[0]);
        elog(NOTICE, "values[1]=%p", values[1]);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else {
        SRF_RETURN_DONE(funcctx);
    }
}
