#include "ao_reader.h"
#include "tuple_batch.h"
#include "parquet_reader.h"
#include "nodes/execnodes.h"
#include "debug.h"
#include "vexecQual.h"
#include <assert.h>

static TupleBatch ao_globalTB;
static TupleTableSlot *
AppendOnlyVScan(ScanState *scanState);

static TupleTableSlot *
ExecAppendOnlyScanRelation(ScanState *scanState);

ParquetScanOpaqueData *opaque = NULL;
TupleTableSlot *ExecAppendOnlyVScan(ScanState *node)
{
    ScanState *scanState = node;

    if (scanState->scan_state == SCAN_INIT ||
        scanState->scan_state == SCAN_DONE)
    {
        BeginScanAppendOnlyRelation(scanState);

        opaque = palloc(sizeof(ParquetScanOpaqueData));
        opaque->ncol = scanState->ss_currentRelation->rd_att->natts;
        opaque->proj = palloc0(sizeof(bool) * opaque->ncol);

        GetNeededColumnsForScan((Node *)scanState->ps.plan->targetlist, opaque->proj, opaque->ncol);
        GetNeededColumnsForScan((Node *)scanState->ps.plan->qual, opaque->proj, opaque->ncol);

        int i = 0;
        for (i = 0; i < opaque->ncol; i++)
            if (opaque->proj[i]) break;

        if (i == opaque->ncol)
            opaque->proj[0] = true;

        ao_globalTB = createMaxTupleBatch(opaque->ncol,
                                          scanState->ss_currentRelation->rd_att,
                                          opaque->proj);
    }
    resetTupleBatch(ao_globalTB);
    scanState->ss_ScanTupleSlot->PRIVATE_tts_data = (void *)ao_globalTB;

    TupleTableSlot *slot = ExecAppendOnlyScanRelation(scanState);

    if (!TupIsNull(slot))
    {
        //Gpmon_M_Incr_Rows_Out(GpmonPktFromTableScanState(node));
        //CheckSendPlanStateGpmonPkt(&scanState->ps);
    }
    else if (!scanState->ps.delayEagerFree)
    {
        EndScanAppendOnlyRelation(scanState);
        //TODO:
        //clean opaque
        //clean globalTB
    }
    return slot;
}

static TupleTableSlot *
ExecAppendOnlyScanRelation(ScanState *node)
{
    //TupleTableSlot *slot = AppendOnlyVScan(scanState);
    ExprContext *econtext;
    List       *qual;
    ProjectionInfo *projInfo;

    /*
     * Fetch data from node
     */
    qual = node->ps.qual;
    projInfo = node->ps.ps_ProjInfo;

    /*
     * If we have neither a qual to check nor a projection to do, just skip
     * all the overhead and return the raw scan tuple.
     */
    if (!qual && !projInfo)
        return AppendOnlyVScan(node);

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.
     */
    econtext = node->ps.ps_ExprContext;
    ResetExprContext(econtext);

    /*
     * get a tuple from the access method loop until we obtain a tuple which
     * passes the qualification.
     */
    for (;;)
    {
        TupleTableSlot *slot;

        CHECK_FOR_INTERRUPTS();

        slot = AppendOnlyVScan(node);

        /*
         * if the slot returned by the accessMtd contains NULL, then it means
         * there is nothing more to scan so we just return an empty slot,
         * being careful to use the projection result slot so it has correct
         * tupleDesc.
         */
        if (TupIsNull(slot))
        {
            assert(1);
            if (projInfo)
                return ExecClearTuple(projInfo->pi_slot);
            else
                return slot;
        }

        /*
         * place the current tuple into the expr context
         */
        econtext->ecxt_scantuple = slot;

        /*
         * check that the current tuple satisfies the qual-clause
         *
         * check for non-nil qual here to avoid a function call to ExecQual()
         * when the qual is nil ... saves only a few cycles, but they add up
         * ...
         */
        if (!qual || VExecQual(qual, econtext, false))
        {
            /*
             * Found a satisfactory scan tuple.
             */
            if (projInfo)
            {
                /*
                 * Form a projection tuple, store it in the result tuple slot
                 * and return it.
                 */
                return ExecVProject(slot, projInfo, NULL);
            }
            else
            {
                /*
                 * Here, we aren't projecting, so just return scan tuple.
                 */
                return slot;
            }
        }

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);
    }

}

static TupleTableSlot *
AppendOnlyVScan(ScanState *scanState)
{
    TupleTableSlot *slot = scanState->ss_ScanTupleSlot;
    TupleBatch tb = (TupleBatch)slot->PRIVATE_tts_data;
    int row = 0;

    for(;row < BATCH_SIZE;row ++)
    {
        AppendOnlyScanNext(scanState);

        slot = scanState->ss_ScanTupleSlot;
        if(TupIsNull(slot))
            break;

        for(int i = 0;i < opaque->ncol; i ++)
        {
            if(opaque->proj[i])
                tb->columnDataArray[i]->values[row] = slot_getattr(slot,i + 1, &(tb->columnDataArray[i]->nulls[row]));
        }

        VarBlockHeader *header = ((AppendOnlyScanState*)scanState)->aos_ScanDesc->executorReadBlock.varBlockReader.header;
        if(row + 1 == VarBlockGet_itemCount(header))
            break;
    }
    tb->nrow = row + 1;
    //TupSetVirtualTupleNValid(slot, opaque->ncol);
    return slot;
}

