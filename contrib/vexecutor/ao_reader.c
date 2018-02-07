#include "ao_reader.h"
#include "tuple_batch.h"
static TupleTableSlot *
AppendOnlyVScan(ScanState *scanState);

TupleTableSlot *ExecAppendOnlyVScan(ScanState *node)
{
    ScanState *scanState = node;

    if (scanState->scan_state == SCAN_INIT ||
        scanState->scan_state == SCAN_DONE)
    {
        BeginScanAppendOnlyRelation(scanState);
    }

    MemTuple *batch = AppendOnlyVScan(scanState);
    //TODO:reorganize MemTupe To TupleBatch structure

    if (batchTuple[0] == NULL && !scanState->ps.delayEagerFree)
    {
        EndScanAppendOnlyRelation(scanState);
    }

    return NULL;
}

static MemTuple *
AppendOnlyVScan(ScanState *scanState)
{
    Assert(IsA(scanState, TableScanState) ||
           IsA(scanState, DynamicTableScanState));
    AppendOnlyScanState *node = (AppendOnlyScanState *)scanState;

    AppendOnlyScanDesc scandesc;
    Index		scanrelid;
    EState	   *estate;
    ScanDirection direction;
    //这里面的slot实际上是完全无用的，存在的意义只是为了可以调用原有的接口
    //如果在后面做的话，我们需要自己写一版可以不将MemTuple存入slot中的版本
    TupleTableSlot *slot;

    Assert((node->ss.scan_state & SCAN_SCAN) != 0);
    /*
     * get information from the estate and scan state
     */
    estate = node->ss.ps.state;
    scandesc = node->aos_ScanDesc;
    scanrelid = ((AppendOnlyScan *) node->ss.ps.plan)->scan.scanrelid;
    direction = estate->es_direction;
    slot = node->ss.ss_ScanTupleSlot;

    MemTuple* batch = palloc0(sizeof(MemTuple) * BATCH_SIZE);
    for(int i = 0;i < BATCH_SIZE; ++i)
    {
        batch[i] = appendonly_getnext(scandesc,direction,slot);
    }

    return batch;
}


//static MemTuple *
//ExecAppendOnlyScanRelation(AppendOnlyScanState *node)
//{
//
//    ExprContext *econtext;
//    List        *qual;
//    int         tupleNum;
//    ProjectionInfo *projInfo;
//    TupleTableSlot *slot;
//    AppendOnlyScanDesc desc;
//    MemTuple *batchTuple;
//    VarBlockHeader       *header;
//    /*
//     * Fetch data from node
//     */
//    qual = node->ss.ps.qual;
//    projInfo = node->ss.ps.ps_ProjInfo;
//    desc = node->aos_ScanDesc;
//    slot = node->ss.ss_ScanTupleSlot;
//
//    while(!getNextBlock(desc))
//        if(desc->aos_done_all_splits) return NULL;
//
//    header = desc->executorReadBlock.varBlockReader.header;
//    tupleNum = VarBlockGet_itemCount(header);
//
//    batchTuple = palloc0(sizeof(MemTuple*) * tupleNum);
//
//    for(int i = 0;i < tupleNum;i ++)
//    {
//        batchTuple[i] = AppendOnlyExecutorReadBlock_ScanNextTuple(&desc->executorReadBlock,desc->aos_nkeys,desc->aos_key,slot);
//    }
//    slot->PRIVATE_tts_memtuple = tupleNum;
//        //return AppendOnlyScanNext(node);
//    return batchTuple;
//}
