#include "vexecutor.h"
#include "parquet_reader.h"
#include "debug.h"
#include "tuple_batch.h"
#include "catalog/namespace.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "executor/executor.h"
#include "executor/execHHashagg.h"
#include "executor/nodeAgg.h"
#include "../backend/executor/execHHashagg.c"
#include "vexecQual.h"

PG_MODULE_MAGIC;

//batch hashagg group linklist header
typedef struct GroupData {
	HashAggEntry *entry;	// pointer to agg_hash_entry
	int idx; 				// pointer to idx_list
} GroupData;

//batch hashagg group data
struct BatchAggGroupData{
	GroupData 	group_header[BATCH_SIZE];	//group linklist header
	int 		group_idx;					//current group header index
	int 		group_cnt;					//group header count

	//linklist elements
	//each item's index is the index to columnData
	//each item's value is the next pointer, -1 is the end of a linklist
	int 		idx_list[BATCH_SIZE];
};


static exec_agg_hook_type PreviousExecAggHook = NULL;
static init_agg_hook_type PreviousInitAggHook = NULL;

/*
 * hook function
 */
static TupleTableSlot *VExecAgg(AggState *node);
static AggState * VExecInitAgg(Agg *node, EState *estate, int eflags);

/*
 * extern function
 */
//extern TupleTableSlot *agg_retrieve_hash_table(AggState *aggstate);
static TupleTableSlot *vagg_retrieve_hash_table(AggState *aggstate);

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
static void advance_vtransition_function(AggState *aggstate, AggStatePerAgg peraggstate,
	AggStatePerGroup pergroupstate,
	FunctionCallInfoData *fcinfo,
	const char *funcName);

static void finalize_vaggregates(AggState *aggstate, AggStatePerGroup pergroup);
static void finalize_vaggregate(AggState *aggstate,
	AggStatePerAgg peraggstate,
	AggStatePerGroup pergroupstate,
	Datum *resultVal,
	bool *resultIsNull);

// ExecAgg for hashed case.
static bool vagg_hash_initial_pass(AggState *aggstate);

// plain agg
static Datum int4_sum_vec_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb);
static Datum int8inc_any_vec_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb);

// group agg
static Datum int4_sum_vec_group_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb);
static Datum int8inc_any_vec_group_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb);


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

	PreviousInitAggHook = init_agg_hook;
	init_agg_hook = VExecInitAgg;
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void
_PG_fini(void)
{
	exec_agg_hook = PreviousExecAggHook;
	init_agg_hook = PreviousInitAggHook;
	//exec_scan_hook = PreviousExecScanHook;
}

static AggState *
VExecInitAgg(Agg *node, EState *estate, int eflags) {
	elog(NOTICE, "call VExecInitAgg");

	AggState * aggstate = ExecInitAggWithHook(node, estate, eflags, VExecInitExpr);

	return aggstate;

}

TupleTableSlot *
VExecAgg(AggState *node)
{
	if (node->agg_done)
	{
		ExecEagerFreeAgg(node);
		return NULL;
	}

	elog(NOTICE, "call VExecAgg");

	if (((Agg *) node->ss.ps.plan)->aggstrategy == AGG_HASHED)
	{
		TupleTableSlot *tuple = NULL;
		bool streaming = ((Agg *) node->ss.ps.plan)->streaming;

		if (node->hhashtable == NULL)
		{
			bool tupremain;

			node->hhashtable = create_agg_hash_table(node);
			tupremain = vagg_hash_initial_pass(node);

			if ( streaming )
			{
				if ( tupremain )
					node->hhashtable->state = HASHAGG_STREAMING;
				else
					node->hhashtable->state = HASHAGG_END_OF_PASSES;
			}
			else
				node->hhashtable->state = HASHAGG_BETWEEN_PASSES;
		}

		for (;;)
		{
			if (!node->hhashtable->is_spilling)
			{
				tuple = vagg_retrieve_hash_table(node);
				node->agg_done = false;

				if (tuple != NULL)
					return tuple;
			}

			switch (node->hhashtable->state)
			{
			case HASHAGG_BETWEEN_PASSES:
				Assert(!streaming);
				if (agg_hash_next_pass(node))
				{
					node->hhashtable->state = HASHAGG_BETWEEN_PASSES;
					continue;
				}
				node->hhashtable->state = HASHAGG_END_OF_PASSES;
			case HASHAGG_END_OF_PASSES:
				node->agg_done = true;
				ExecEagerFreeAgg(node);
				return NULL;

			case HASHAGG_STREAMING:
				Assert(streaming);
				if ( !agg_hash_stream(node) )
					node->hhashtable->state = HASHAGG_END_OF_PASSES;
				continue;

			case HASHAGG_BEFORE_FIRST_PASS:
			default:
				elog(ERROR,"hybrid hash aggregation sequencing error");
			}
		}
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
	int rcounter = 0;
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

		TupleBatch tb = (TupleBatch)outerslot->PRIVATE_tts_data;
		rcounter += tb->nrow;

		tmpcontext->ecxt_scantuple = outerslot;
		for (int aggno = 0; aggno < aggstate->numaggs; aggno++)
		{
			AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
			VExecProject(peraggstate->evalproj, NULL);
			//slot_getallattrs(slot);
		}

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
 * ExecAgg for hashed case: retrieve groups from hash table
 */
TupleTableSlot *vagg_retrieve_hash_table(AggState *aggstate)
{
	ExprContext *econtext;
	ProjectionInfo *projInfo;
	Datum	   *aggvalues;
	bool	   *aggnulls;
	AggStatePerAgg peragg;
	AggStatePerGroup pergroup;
	TupleTableSlot *firstSlot;
	Agg		   *node = (Agg *) aggstate->ss.ps.plan;
	bool        input_has_grouping = node->inputHasGrouping;
	bool        is_final_rollup_agg =
		(node->lastAgg ||
		 (input_has_grouping && node->numNullCols == 0));

	/*
	 * get state info from node
	 */
	/* econtext is the per-output-tuple expression context */
	econtext = aggstate->ss.ps.ps_ExprContext;
	aggvalues = econtext->ecxt_aggvalues;
	aggnulls = econtext->ecxt_aggnulls;
	projInfo = aggstate->ss.ps.ps_ProjInfo;
	peragg = aggstate->peragg;

	firstSlot = aggstate->ss.ss_ScanTupleSlot;

	if (aggstate->agg_done)
		return NULL;

	/*
	 * We loop retrieving groups until we find one satisfying
	 * aggstate->ss.ps.qual
	 */
	while (!aggstate->agg_done)
	{
		HashAggEntry *entry = agg_hash_iter(aggstate);

		if (entry == NULL)
		{
			aggstate->agg_done = TRUE;

			return NULL;
		}

		ResetExprContext(econtext);

		/*
		 * Store the copied first input tuple in the tuple table slot reserved
		 * for it, so that it can be used in ExecProject.
		 */
		ExecStoreMemTuple((MemTuple)entry->tuple_and_aggs, firstSlot, false);
		pergroup = (AggStatePerGroup)((char *)entry->tuple_and_aggs +
			MAXALIGN(memtuple_get_size((MemTuple)entry->tuple_and_aggs,
					aggstate->hashslot->tts_mt_bind)));

		/*
		 * Finalize each aggregate calculation, and stash results in the
		 * per-output-tuple context.
		 */
		finalize_vaggregates(aggstate, pergroup);

		/*
		 * Use the representative input tuple for any references to
		 * non-aggregated input columns in the qual and tlist.
		 */
		econtext->ecxt_scantuple = firstSlot;

		if (is_final_rollup_agg && input_has_grouping)
		{
			econtext->group_id =
				get_grouping_groupid(econtext->ecxt_scantuple,
					node->grpColIdx[node->numCols-node->numNullCols-1]);
			econtext->grouping =
				get_grouping_groupid(econtext->ecxt_scantuple,
					node->grpColIdx[node->numCols-node->numNullCols-2]);
		}
		else
		{
			econtext->group_id = node->rollupGSTimes;
			econtext->grouping = node->grouping;
		}

		/*
		 * Check the qual (HAVING clause); if the group does not match, ignore
		 * it and loop back to try to process another group.
		 */
		if (ExecQual(aggstate->ss.ps.qual, econtext, false))
		{
			/*
			 * Form and return a projection tuple using the aggregate results
			 * and the representative input tuple.	Note we do not support
			 * aggregates returning sets ...
			 */
			Gpmon_M_Incr_Rows_Out(GpmonPktFromAggState(aggstate));
			CheckSendPlanStateGpmonPkt(&aggstate->ss.ps);
			return ExecProject(projInfo, NULL);
		}
	}

	/* No more groups */
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
	int aggno;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];

		if (peraggstate->numSortCols > 0)
		{
			// TODO
		}

		if (peraggstate->initValueIsNull)
		{
			pergroupstate->transValue = peraggstate->initValue;
		}
		else
		{
			pergroupstate->transValue = datumCopyWithMemManager(0,
				peraggstate->initValue,
				peraggstate->transtypeByVal,
				peraggstate->transtypeLen,
				mem_manager);
		}
		pergroupstate->transValueIsNull = peraggstate->initValueIsNull;
		pergroupstate->noTransValue = peraggstate->initValueIsNull;
	}
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
	int			aggno;
	ExprContext *tmpcontext = aggstate->tmpcontext;
	TupleTableSlot *scantuple = tmpcontext->ecxt_scantuple;
	if (scantuple == NULL || scantuple->PRIVATE_tts_data == NULL)
	{
		elog(ERROR, "no tuple batch during advance_vaggregates");
	}

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];
		int32 argumentCount = peraggstate->numArguments + 1;
		char *transitionFuncName = NULL;
		char vectorTransitionFuncName[NAMEDATALEN];
		List *qualVectorTransitionFuncName = NIL;
		FuncCandidateList vectorTransitionFuncList = NULL;
		FunctionCallInfoData fcinfo;

		/*
		 * If the user typed sum(), count(), or avg() instead of the vectorized
		 * aggregate names, manually map to the vectorized version here. This is
		 * merely syntactic sugar. Note that we rely on a naming convention here,
		 * where vectorized function names are regular function names with _vec
		 * appended to them.
		 */
		transitionFuncName = get_func_name(peraggstate->transfn_oid);
		snprintf(vectorTransitionFuncName, NAMEDATALEN, "%s_vec", transitionFuncName);

		qualVectorTransitionFuncName =
			stringToQualifiedNameList(vectorTransitionFuncName, "");
		vectorTransitionFuncList = FuncnameGetCandidates(qualVectorTransitionFuncName, argumentCount);

		Oid functionOid = 0;
		if (vectorTransitionFuncList != NULL)
		{
			functionOid = vectorTransitionFuncList->oid;
			fmgr_info(functionOid, &peraggstate->transfn);
		}

		advance_vtransition_function(aggstate, peraggstate, pergroupstate,
			&fcinfo, transitionFuncName);

	} /* aggno loop */

}

/*
 * finalize_aggregates
 *   Compute the final value for all aggregate functions.
 */
void
finalize_vaggregates(AggState *aggstate, AggStatePerGroup pergroup)
{
	AggStatePerAgg peragg = aggstate->peragg;
	ExprContext *econtext = aggstate->ss.ps.ps_ExprContext;
	Datum *aggvalues = econtext->ecxt_aggvalues;
	bool *aggnulls = econtext->ecxt_aggnulls;

	for (int aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		AggStatePerAgg peraggstate = &peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];
		if ( peraggstate->numSortCols > 0 )
		{
			// TODO
		}
		finalize_vaggregate(aggstate, peraggstate, pergroupstate,
			&aggvalues[aggno], &aggnulls[aggno]);
	}
}

/*
 * Compute the final value of one aggregate.
 *
 * The finalfunction will be run, and the result delivered, in the
 * output-tuple context; caller's CurrentMemoryContext does not matter.
 */
void
finalize_vaggregate(AggState *aggstate,
	AggStatePerAgg peraggstate,
	AggStatePerGroup pergroupstate,
	Datum *resultVal, bool *resultIsNull)
{
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(aggstate->ss.ps.ps_ExprContext->ecxt_per_tuple_memory);

	/*
	 * Apply the agg's finalfn if one is provided, else return transValue.
	 */
	if (OidIsValid(peraggstate->finalfn_oid))
	{
		FunctionCallInfoData fcinfo;

		InitFunctionCallInfoData(fcinfo, &(peraggstate->finalfn), 1,
			(void *) aggstate, NULL);
		fcinfo.arg[0] = pergroupstate->transValue;
		fcinfo.argnull[0] = pergroupstate->transValueIsNull;
		if (fcinfo.flinfo->fn_strict && pergroupstate->transValueIsNull)
		{
			/* don't call a strict function with NULL inputs */
			*resultVal = (Datum) 0;
			*resultIsNull = true;
		}
		else
		{
			*resultVal = FunctionCallInvoke(&fcinfo);
			*resultIsNull = fcinfo.isnull;
		}
	}
	else
	{
		*resultVal = pergroupstate->transValue;
		//*resultIsNull = pergroupstate->transValueIsNull;
		*resultIsNull = false;
	}

	/*
	 * If result is pass-by-ref, make sure it is in the right context.
	 */
	if (!peraggstate->resulttypeByVal && !*resultIsNull &&
		!MemoryContextContainsGenericAllocation(CurrentMemoryContext,
			DatumGetPointer(*resultVal)))
		*resultVal = datumCopy(*resultVal,
			peraggstate->resulttypeByVal,
			peraggstate->resulttypeLen);

	MemoryContextSwitchTo(oldContext);
}

/*
 * Similar to advance_transition_function, but in vectorized version we don't
 * check for nulls. A stripe should be never null. So handling null values is
 * the responsibility of the related trans function.
 */
void
advance_vtransition_function(AggState *aggstate, AggStatePerAgg peraggstate,
	AggStatePerGroup pergroupstate,
	FunctionCallInfoData *fcinfo,
	const char *funcName)
{
	ExprContext *tmpcontext = aggstate->tmpcontext;
	TupleTableSlot *scantuple = tmpcontext->ecxt_scantuple;
	if (scantuple == NULL || scantuple->PRIVATE_tts_data == NULL)
	{
		elog(ERROR, "no tuple batch during advance_vaggregates");
	}

	TupleBatch scan_tb = (TupleBatch)scantuple->PRIVATE_tts_data;

	MemoryContext oldContext;
	Datum newVal;

	int projIdx = 0;
	TupleBatch tb = NULL;
	int columnIndex = 0;
	if (peraggstate->evalproj->pi_isVarList) {
		projIdx = peraggstate->evalproj->pi_varNumbers[0]-1;  //core here before
		tb = (TupleBatch)scantuple->PRIVATE_tts_data;

		columnIndex = projIdx;
		if (tb->nvalid > 0)
			columnIndex = tb->vprojs[projIdx] - 1;
	}
	else {
		//use the eval slot to get tb
		projIdx = 0 ; //what index? suppose always 0 (peragg project 1 batch)
		tb = (TupleBatch)peraggstate->evalslot->PRIVATE_tts_data;
		columnIndex = projIdx;
	}

	TupleColumnData *columnData = tb->columnDataArray[columnIndex];

	/* we run the transition functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	if (strstr(funcName, "_sum") != NULL)
	{
		//sum
		if (scan_tb->agg_groupdata->group_cnt > 0)
			//groupby
			newVal = int4_sum_vec_group_internal(pergroupstate->transValue, columnData, scan_tb);
		else
			newVal = int4_sum_vec_internal(pergroupstate->transValue, columnData, scan_tb);
	}
	else
	{
		//count
		if (scan_tb->agg_groupdata->group_cnt > 0)
			//groupby
			newVal = int8inc_any_vec_group_internal(pergroupstate->transValue, columnData, scan_tb);
		else
			newVal = int8inc_any_vec_internal(pergroupstate->transValue, columnData, scan_tb);
	}

	if (!peraggstate->transtypeByVal &&
		DatumGetPointer(newVal) != DatumGetPointer(pergroupstate->transValue))
	{
		if (!fcinfo->isnull)
		{
			MemoryContextSwitchTo(aggstate->aggcontext);
			newVal = datumCopy(newVal, peraggstate->transtypeByVal,
				peraggstate->transtypeLen);
		}
		if (!pergroupstate->transValueIsNull)
		{
			pfree(DatumGetPointer(pergroupstate->transValue));
		}
	}


	pergroupstate->transValue = newVal;
	pergroupstate->transValueIsNull = fcinfo->isnull;

	MemoryContextSwitchTo(oldContext);
}

/* Function: agg_hash_initial_pass
 *
 * Performs ExecAgg initialization for the first pass of the hashed case:
 * - reads the input tuples,
 * - builds a hash table with an entry per group,
 * - spills all groups in the hash table to several overflow batches
 *   to be processed during later passes.
 *
 * Note that overflowed groups are distributed to batches in such
 * a way that groups with matching grouping keys will be in the same
 * batch.
 *
 * When called, CurrentMemoryContext should be the per-query context.
 */
bool
vagg_hash_initial_pass(AggState *aggstate)
{
	HashAggTable *hashtable = aggstate->hhashtable;
	ExprContext *tmpcontext = aggstate->tmpcontext;
	TupleTableSlot *outerslot = NULL;
	bool streaming = ((Agg *) aggstate->ss.ps.plan)->streaming;
	bool tuple_remaining = true;
	MemTupleBinding *mt_bind = aggstate->hashslot->tts_mt_bind;
	PlanState *outerPlan = outerPlanState(aggstate);
	bool print_nrows = true;

	Assert(hashtable);
	AssertImply(!streaming, hashtable->state == HASHAGG_BEFORE_FIRST_PASS);

	if (aggstate->hashslot->tts_tupleDescriptor != NULL &&
		hashtable->prev_slot != NULL)
	{
		outerslot = hashtable->prev_slot;
		hashtable->prev_slot = NULL;
	}

	else
	{
		outerslot = ExecParquetVScan((TableScanState *)outerPlan);
	}

	hashtable->pass = 0;

	//create agg_groupdata and set the pointer of tb to it
	BatchAggGroupData agg_groupdata;
	TupleBatch tb = (TupleBatch)outerslot->PRIVATE_tts_data;
	tb->agg_groupdata = &agg_groupdata;

	//the below items only need init once
	ExecStoreAllNullTuple(tb->rowSlot);
	int nvalid = tb->nvalid;
	if (tb->nvalid <= 0)
		nvalid = tb->ncol;
	TupSetVirtualTupleNValid(tb->rowSlot, nvalid);
	for (int i=0;i<nvalid;i++)
		tb->rowSlot->PRIVATE_tts_isnull[i] = false;


	while(true)
	{
		HashKey hashkey;
		bool isNew;
		HashAggEntry *entry;
		if (TupIsNull(outerslot))
		{
			tuple_remaining = false;
			break;
		}

		if (print_nrows) {
			elog(NOTICE, "batch row size:%d", tb->nrow);
			print_nrows = false;
		}

		//init agg_groupdata in each loop
		tb->agg_groupdata->group_cnt = 0;
		for (int i = 0; i < BATCH_SIZE; i++)
			tb->agg_groupdata->idx_list[i] = -1;

		for (int i=0;i<tb->nrow;i++)
		{
			TupleTableSlot *scanslot = getNextRowFromTupleBatch(tb, outerslot->tts_tupleDescriptor);

			if (aggstate->hashslot->tts_tupleDescriptor == NULL)
			{
				int size;

				ExecSetSlotDescriptor(aggstate->hashslot, outerslot->tts_tupleDescriptor);
				ExecStoreAllNullTuple(aggstate->hashslot);
				mt_bind = aggstate->hashslot->tts_mt_bind;

				size = ((Agg *)aggstate->ss.ps.plan)->numCols * sizeof(HashKey);

				hashtable->hashkey_buf = (HashKey *)palloc0(size);
				hashtable->mem_for_metadata += size;
			}

			tmpcontext->ecxt_scantuple = scanslot;

			hashkey = calc_hash_value(aggstate, scanslot);
			entry = lookup_agg_hash_entry(aggstate, (void *)scanslot, 0, 0, hashkey, 0, &isNew);

			if (entry == NULL)
			{
				elog(NOTICE, "hashtable is full");
				if (GET_TOTAL_USED_SIZE(hashtable) > hashtable->mem_used)
					hashtable->mem_used = GET_TOTAL_USED_SIZE(hashtable);

				if (hashtable->num_ht_groups <= 1)
					ereport(ERROR,
						(errcode(ERRCODE_GP_INTERNAL_ERROR),
						 ERRMSG_GP_INSUFFICIENT_STATEMENT_MEMORY));

				if (streaming)
				{
					Assert(tuple_remaining);
					hashtable->prev_slot = scanslot;
					break;
				}

				if (!hashtable->is_spilling && aggstate->ss.ps.instrument)
					agg_hash_table_stat_upd(hashtable);
			}


			if (isNew)
			{
				setGroupAggs(hashtable, mt_bind, entry);

				//elog(NOTICE, "new hash group for key:%u", hashkey);
				int tup_len = memtuple_get_size((MemTuple)entry->tuple_and_aggs, mt_bind);
				MemSet((char *)entry->tuple_and_aggs + MAXALIGN(tup_len), 0,
					aggstate->numaggs * sizeof(AggStatePerGroupData));
				initialize_aggregates(aggstate, aggstate->peragg, hashtable->groupaggs->aggs,
					&(aggstate->mem_manager));
			}

			//First, builds the group linklist.
			GroupData *cur_header = NULL;
			//find current group_header if exists, just O(n) find
			for (int j = 0; j < tb->agg_groupdata->group_cnt; j++)
				if (tb->agg_groupdata->group_header[j].entry == entry)
					cur_header = &(tb->agg_groupdata->group_header[j]);

			if (cur_header == NULL) {
				// add a new group header
				tb->agg_groupdata->group_header[tb->agg_groupdata->group_cnt].idx = i;
				tb->agg_groupdata->group_header[tb->agg_groupdata->group_cnt].entry = entry;
				tb->agg_groupdata->group_cnt++;
			}
			else {
				//group header already exists, just insert the current tuple to the "neck"
				tb->agg_groupdata->idx_list[i] = cur_header->idx;
				cur_header->idx = i;
			}

			//no need call advance_aggregates on each tuple
			//advance_aggregates(aggstate, hashtable->groupaggs->aggs, &(aggstate->mem_manager));

			hashtable->num_tuples++;

			ResetExprContext(tmpcontext);

			if (streaming && !HAVE_FREESPACE(hashtable))
			{
				Assert(tuple_remaining);
				ExecClearTuple(aggstate->hashslot);
				break;
			}

		}

		//Second, do exec batch project
		tmpcontext->ecxt_scantuple = outerslot;
		for (int aggno = 0; aggno < aggstate->numaggs; aggno++)
		{
			AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
			VExecProject(peraggstate->evalproj, NULL);
			//slot_getallattrs(slot);
		}

		//Third, call advance_aggregates in each agg group.
		tmpcontext->ecxt_scantuple = outerslot;
		for (int i = 0; i < tb->agg_groupdata->group_cnt; i++) {
			GroupData *cur_header = &(tb->agg_groupdata->group_header[i]);
			tb->agg_groupdata->group_idx = i;

			//set hashtable->groupaggs to the agg_hash_entry
			setGroupAggs(hashtable, aggstate->hashslot->tts_mt_bind, cur_header->entry);

			advance_vaggregates(aggstate, hashtable->groupaggs->aggs, &(aggstate->mem_manager));
		}

		ResetExprContext(tmpcontext);
		outerslot = ExecParquetVScan((TableScanState *)outerPlan);
	}

	if (GET_TOTAL_USED_SIZE(hashtable) > hashtable->mem_used)
		hashtable->mem_used = GET_TOTAL_USED_SIZE(hashtable);

	AssertImply(tuple_remaining, streaming);

	return tuple_remaining;
}

Datum int4_sum_vec_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb)
{
	int nrow = tb->nrow;
	int64 newValue = DatumGetInt64(origValue);

	for (int i=0;i<nrow;i++)
	{
		newValue = newValue + (int64) DatumGetInt32(columnData->values[i]);
	}

	return Int64GetDatum(newValue);
}

Datum int8inc_any_vec_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb)
{
	int nrow = tb->nrow;
	return Int64GetDatum(DatumGetInt64(origValue) + nrow);
}

//group sum
Datum int4_sum_vec_group_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb)
{
	int64 newValue = DatumGetInt64(origValue);
	GroupData *cur_header = &(tb->agg_groupdata->group_header[tb->agg_groupdata->group_idx]);

	int cur_idx = cur_header->idx;
	//go through the current linklist and calculate the item sum
	while (cur_idx != -1) {
		int64 cur_value = (int64) DatumGetInt32(columnData->values[cur_idx]);
		newValue += cur_value;
		cur_idx = tb->agg_groupdata->idx_list[cur_idx];
	}

	return Int64GetDatum(newValue);
}

//group count
Datum int8inc_any_vec_group_internal(Datum origValue, TupleColumnData *columnData, TupleBatch tb)
{
	int64 count = 0;
	GroupData *cur_header = &(tb->agg_groupdata->group_header[tb->agg_groupdata->group_idx]);

	int cur_idx = cur_header->idx;
	//go through the current linklist and calculate the item count
	while (cur_idx != -1) {
		count++;
		cur_idx = tb->agg_groupdata->idx_list[cur_idx];
	}

	return Int64GetDatum(DatumGetInt64(origValue) + count);
}
