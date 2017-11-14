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

PG_MODULE_MAGIC;

static exec_agg_hook_type PreviousExecAggHook = NULL;
static exec_scan_hook_type PreviousExecScanHook = NULL;

/*
 * hook function
 */
static TupleTableSlot *VExecAgg(AggState *node);
static TupleTableSlot *VExecScan(TableScanState *node);

/*
 * extern function
 */
extern TupleTableSlot *agg_retrieve_hash_table(AggState *aggstate);

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

static Datum int4_sum_vec_internal(Datum origValue, TupleColumnData *columnData, int nrow);
static Datum int8inc_any_vec_internal(Datum origValue, TupleColumnData *columnData, int nrow);

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
	
	//PreviousExecScanHook = exec_scan_hook;
	//exec_scan_hook = VExecScan;
}

/*
 * _PG_fini is called when the module is unloaded. This function uninstalls the
 * extension's hooks.
 */
void 
_PG_fini(void)
{
	exec_agg_hook = PreviousExecAggHook;
	//exec_scan_hook = PreviousExecScanHook;
}

TupleTableSlot *
VExecScan(TableScanState *node)
{
	return ExecParquetVScan(node);
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
				tuple = agg_retrieve_hash_table(node);
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
		//elog(NOTICE, "row count is :%d", rcounter);	
	
		//print_slot(outerslot);
		//dumpTupleBatch(outerslot);

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

	TupleBatch tb = (TupleBatch)scantuple->PRIVATE_tts_data;

	for (aggno = 0; aggno < aggstate->numaggs; aggno++)
	{
		Datum value;
		bool isnull;
		AggStatePerAgg peraggstate = &aggstate->peragg[aggno];
		AggStatePerGroup pergroupstate = &pergroup[aggno];
		int32 argumentCount = peraggstate->numArguments + 1;
		//Aggref	   *aggref = peraggstate->aggref;
		//PercentileExpr *perc = peraggstate->perc;
		//int			i;
		TupleTableSlot *slot;
		//int nargs;
        char *transitionFuncName = NULL;
        char vectorTransitionFuncName[NAMEDATALEN];
        List *qualVectorTransitionFuncName = NIL;
        FuncCandidateList vectorTransitionFuncList = NULL;
        FunctionCallInfoData fcinfo;

		//TupleColumnData *columnData = NULL; 
        
		/* simple check to handle count(*) */
/*
        int simpleColumnCount =  peraggstate->evalproj->pi_numSimpleVars;
        if (simpleColumnCount >= 1)
        {
            int columnIndex = peraggstate->evalproj->pi_varNumbers[0]-1;
            columnData = tb->columnDataArray[columnIndex];
        }
*/
        //columnData = tb->columnDataArray[0];
        

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
/*
		elog(LOG, "transitionFuncName:%s vectorTransitionFuncName:%s functionOid:%d",
						transitionFuncName,
						vectorTransitionFuncName,
						functionOid);
        fcinfo.arg[1] = PointerGetDatum(columnData);
        fcinfo.arg[2] = Int32GetDatum(tb->nrow);
*/

		advance_vtransition_function(aggstate, peraggstate, pergroupstate, &fcinfo, transitionFuncName);
										//&fcinfo, mem_manager);

		/*
		if (aggref)
			nargs = list_length(aggref->args);
		else
		{
			Assert (perc);
			nargs = list_length(perc->args);
		}

		slot = ExecProject(peraggstate->evalproj, NULL);
		slot_getallattrs(slot);	
		
		if (peraggstate->numSortCols > 0)
		{
		}
		else
		{
			FunctionCallInfoData fcinfo;
			
			Assert(slot->PRIVATE_tts_nvalid >= nargs);
			if (aggref)
			{
				for (i = 0; i < nargs; i++)
				{
					fcinfo.arg[i + 1] = slot_getattr(slot, i+1, &isnull);
					fcinfo.argnull[i + 1] = isnull;
				}

			}
			else
			{
				int		natts;

				Assert(perc);
				natts = slot->tts_tupleDescriptor->natts;
				for (i = 0; i < natts; i++)
				{
					fcinfo.arg[i + 1] = slot_getattr(slot, i + 1, &isnull);
					fcinfo.argnull[i + 1] = isnull;
				}
			}
		}
		*/
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

	TupleBatch tb = (TupleBatch)scantuple->PRIVATE_tts_data;

	int	numArguments = peraggstate->numArguments;
	MemoryContext oldContext;
	Datum newVal;

	int projIdx = peraggstate->evalproj->pi_varNumbers[0]-1;
	int columnIndex = tb->projs[projIdx] - 1;
   	TupleColumnData *columnData = tb->columnDataArray[columnIndex];

	/* we run the transition functions in per-input-tuple memory context */
	oldContext = MemoryContextSwitchTo(aggstate->tmpcontext->ecxt_per_tuple_memory);

	if (strstr(funcName, "_sum") != NULL)
	{
		newVal = int4_sum_vec_internal(pergroupstate->transValue, columnData, tb->nrow);
	}
	else
	{
		newVal = int8inc_any_vec_internal(pergroupstate->transValue, columnData, tb->nrow);
	}
	
	//elog(NOTICE, "colIdx:%d newValue after transition is %lld", columnIndex, DatumGetInt64(newVal));

	/* OK to call the transition function */
/*
	InitFunctionCallInfoData(*fcinfo, &(peraggstate->transfn), numArguments + 1, (void *) aggstate, NULL);
	fcinfo->arg[0] = pergroupstate->transValue;
	fcinfo->argnull[0] = pergroupstate->transValueIsNull;
	newVal = FunctionCallInvoke(fcinfo);
*/
	/*
	 * If pass-by-ref datatype, must copy the new value into aggcontext and
	 * pfree the prior transValue.	But if transfn returned a pointer to its
	 * first input, we don't need to do anything.
	 */
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

		TupleBatch tb = (TupleBatch)outerslot->PRIVATE_tts_data;

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

			setGroupAggs(hashtable, mt_bind, entry);
			
			if (isNew)
			{
				elog(NOTICE, "new hash group for key:%u", hashkey);
				int tup_len = memtuple_get_size((MemTuple)entry->tuple_and_aggs, mt_bind);
				MemSet((char *)entry->tuple_and_aggs + MAXALIGN(tup_len), 0,
					   aggstate->numaggs * sizeof(AggStatePerGroupData));
				initialize_aggregates(aggstate, aggstate->peragg, hashtable->groupaggs->aggs,
									  &(aggstate->mem_manager));
			}
				
			advance_aggregates(aggstate, hashtable->groupaggs->aggs, &(aggstate->mem_manager));
			
			hashtable->num_tuples++;

			ResetExprContext(tmpcontext);

			if (streaming && !HAVE_FREESPACE(hashtable))
			{
				Assert(tuple_remaining);
				ExecClearTuple(aggstate->hashslot);
				break;
			}

		}

		outerslot = ExecParquetVScan((TableScanState *)outerPlan);
	}

	if (GET_TOTAL_USED_SIZE(hashtable) > hashtable->mem_used)
		hashtable->mem_used = GET_TOTAL_USED_SIZE(hashtable);

   	AssertImply(tuple_remaining, streaming);

	return tuple_remaining;
}

Datum int4_sum_vec_internal(Datum origValue, TupleColumnData *columnData, int nrow)
{
	int64 newValue = DatumGetInt64(origValue);

	for (int i=0;i<nrow;i++)
	{
//		if (!columnData->nulls[i])
//		{
			newValue = newValue + (int64) DatumGetInt32(columnData->values[i]);
//		}
	}

	return Int64GetDatum(newValue);
}

Datum int8inc_any_vec_internal(Datum origValue, TupleColumnData *columnData, int nrow)
{
	return Int64GetDatum(DatumGetInt64(origValue) + nrow);
}
