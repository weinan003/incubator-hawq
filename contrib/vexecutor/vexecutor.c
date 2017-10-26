#include "vexecutor.h"
#include "parquet_reader.h"

PG_MODULE_MAGIC;

static exec_agg_hook_type PreviousExecAggHook = NULL;

/*
 * hook function
 */
static TupleTableSlot *VExecAgg(AggState *node);

/*
 * vectorized function for agg
 */
// ExecAgg for non-hashed case.
static TupleTableSlot *vagg_retrieve_direct(AggState *aggstate);
static TupleTableSlot *vagg_retrieve_scalar(AggState *aggstate);
static void initialize_vaggregates(AggState *aggstate,
									AggStatePerAgg peragg,
									AggStatePerGroup pergroup,
									MemoryManagerContainer *mem_manager);
static void advance_vaggregates(AggState *aggstate, 
									AggStatePerGroup pergroup,
									MemoryManagerContainer *mem_manager);
static void finalize_vaggregates(AggState *aggstate, AggStatePerGroup pergroup);


/*
 * _PG_init is called when the module is loaded. In this function we save the
 * previous utility hook, and then install our hook to pre-intercept calls to
 * the copy command.
 */
void 
_PG_init(void)
{
	PreviousExecAggHook = exec_agg_hook;
	exec_agg_hook = VExecAgg;
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void 
_PG_fini(void)
{
	exec_agg_hook = PreviousExecAggHook;
}

TupleTableSlot *
VExecAgg(AggState *node)
{
	if (node->agg_done)
	{
		ExecEagerFreeAgg(node);
		return NULL;
	}

	if (((Agg *) node->ss.ps.plan)->aggstrategy != AGG_PLAIN)
	{
		goto fallback;	
	}
	else
	{
		return vagg_retrieve_direct(node);
	}

fallback:
	elog(WARNING, "unsupport vectorized plan");
	return ExecAgg(node);
}

// ExecAgg for non-hashed case.
TupleTableSlot *
vagg_retrieve_direct(AggState *aggstate)
{
	if (aggstate->agg_done)
	{
		return NULL;
	}

	switch(aggstate->aggType)
	{
		case AggTypeScalar:
			return vagg_retrieve_scalar(aggstate);

		case AggTypeGroup:
		case AggTypeIntermediateRollup:
		case AggTypeFinalRollup:
		default:
			insist_log(false, "invalid Agg node: type %d", aggstate->aggType);
	}
	return NULL;
}

// Compute the scalar aggregates.
TupleTableSlot *
vagg_retrieve_scalar(AggState *aggstate)
{
	AggStatePerAgg peragg = aggstate->peragg;
   	AggStatePerGroup pergroup = aggstate->pergroup ;

	initialize_vaggregates(aggstate, peragg, pergroup, &(aggstate->mem_manager));

	/*
	 * We loop through input tuples, and compute the aggregates.
	 */
	while (!aggstate->agg_done)
   	{
		ExprContext *tmpcontext = aggstate->tmpcontext;
		/* Reset the per-input-tuple context */
		ResetExprContext(tmpcontext);
		PlanState *outerPlan = outerPlanState(aggstate);
		TupleTableSlot *outerslot = ExecParquetVScan((TableScanState *)outerPlan);
		if (TupIsNull(outerslot))
		{
			aggstate->agg_done = true;
			break;
		}
		//Gpmon_M_Incr(GpmonPktFromAggState(aggstate), GPMON_QEXEC_M_ROWSIN);
		//CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

		elog(LOG, "print tuple for debugging");
		print_slot(outerslot);

		tmpcontext->ecxt_scantuple = outerslot;
		advance_vaggregates(aggstate, pergroup, &(aggstate->mem_manager));
	}

	finalize_vaggregates(aggstate, pergroup);

	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	Agg *node = (Agg*)aggstate->ss.ps.plan;
	econtext->grouping = node->grouping;
	econtext->group_id = node->rollupGSTimes;
	/* Check the qual (HAVING clause). */
	if (ExecQual(aggstate->ss.ps.qual, econtext, false))
	{
		//Gpmon_M_Incr_Rows_Out(GpmonPktFromAggState(aggstate));
		//CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);

		/*
		 * Form and return a projection tuple using the aggregate results
		 * and the representative input tuple.
		 */
		return ExecProject(aggstate->ss.ps.ps_ProjInfo, NULL);
	}
	return NULL;
}

/*
 * Initialize all aggregates for a new group of input values.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 *
 * Note that the memory allocation is done through provided memory manager.
 */
void
initialize_vaggregates(AggState *aggstate,
                      AggStatePerAgg peragg,
                      AggStatePerGroup pergroup,
                      MemoryManagerContainer *mem_manager)
{

}

/*
 * Advance all the aggregates for one input tuple.  The input tuple
 * has been stored in tmpcontext->ecxt_scantuple, so that it is accessible
 * to ExecEvalExpr.  pergroup is the array of per-group structs to use
 * (this might be in a hashtable entry).
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
void
advance_vaggregates(AggState *aggstate, AggStatePerGroup pergroup,
                   MemoryManagerContainer *mem_manager)
{

}

/*
 * finalize_aggregates
 *   Compute the final value for all aggregate functions.
 */
void
finalize_vaggregates(AggState *aggstate, AggStatePerGroup pergroup)
{

}
