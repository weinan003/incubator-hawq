#include "parquet_reader.h"

static TupleTableSlot *ExecParquetScanRelation(ScanState *node);
static TupleTableSlot *ParquetVScanNext(ScanState *scanState);

TupleTableSlot *
ExecParquetVScan(TableScanState *node)
{
    ScanState *scanState = (ScanState *)node;

    if (scanState->scan_state == SCAN_INIT ||
        scanState->scan_state == SCAN_DONE)
    {
        BeginScanParquetRelation(scanState);
    }

    //TupleTableSlot *slot = ExecParquetScanRelation(scanState);
    TupleTableSlot *slot = ParquetVScanNext(scanState);

    if (!TupIsNull(slot))
    {
        //Gpmon_M_Incr_Rows_Out(GpmonPktFromTableScanState(node));
        //CheckSendPlanStateGpmonPkt(&scanState->ps);
    }

    else if (!scanState->ps.delayEagerFree)
    {
        EndScanParquetRelation(scanState);
    }

    return slot;
}

TupleTableSlot *
ExecParquetScanRelation(ScanState *node)
{
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
        return ParquetVScanNext(node);

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

        slot = ParquetVScanNext(node);

        /*
         * if the slot returned by the accessMtd contains NULL, then it means
         * there is nothing more to scan so we just return an empty slot,
         * being careful to use the projection result slot so it has correct
         * tupleDesc.
         */
        if (TupIsNull(slot))
        {
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
        if (!qual || ExecQual(qual, econtext, false))
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
                return ExecProject(projInfo, NULL);
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

TupleTableSlot *
ParquetVScanNext(ScanState *scanState)
{
	Assert(IsA(scanState, TableScanState) || IsA(scanState, DynamicTableScanState));
	ParquetScanState *node = (ParquetScanState *)scanState;
	Assert(node->opaque != NULL && node->opaque->scandesc != NULL);

	parquet_getnext(node->opaque->scandesc, node->ss.ps.state->es_direction, node->ss.ss_ScanTupleSlot);
	return node->ss.ss_ScanTupleSlot;
}
