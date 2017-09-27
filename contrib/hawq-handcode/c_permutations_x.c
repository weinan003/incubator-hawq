#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(c_permutations_x);

struct c_reverse_tuple_args {
       int32   argvals[3];
       bool    argnulls[3];
       bool    anyargnull;
};

Datum
c_permutations_x(PG_FUNCTION_ARGS)
{
    FuncCallContext     *funcctx;
    const char  *argnames[3] = {"a","b","c"};
    // 6 possible index permutations for 0,1,2
    const int   ips[6][3] = {{0,1,2},{0,2,1},
                             {1,0,2},{1,2,0},
                             {2,0,1},{2,1,0}};
    int i, call_nr;
    struct c_reverse_tuple_args* args;
    if (SRF_IS_FIRSTCALL()) {
        HeapTupleHeader th = PG_GETARG_HEAPTUPLEHEADER(0); 
        MemoryContext oldcontext;
        /* create a function context for cross-call persistence */ 
        funcctx = SRF_FIRSTCALL_INIT();
        /* switch to memory context appropriate for multiple function calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
        /* allocate and zero-fill struct for persisting extracted arguments*/
        args = palloc0(sizeof(struct c_reverse_tuple_args));
        args->anyargnull = false;
        funcctx->user_fctx = args;
        /* total number of tuples to be returned */
        funcctx->max_calls = 6; // there are 6 permutations of 3 elements
        // extract argument values and NULL-ness
        for(i=0;i<3;i++){
            args->argvals[i] = DatumGetInt32(GetAttributeByName(th, argnames[i], &(args->argnulls[i])));
            if (args->argnulls[i])
                args->anyargnull = true;
        }
        // set up tuple for result info
        if (get_call_result_type(fcinfo, NULL, &funcctx->tuple_desc)
            != TYPEFUNC_COMPOSITE)
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record")));
        BlessTupleDesc(funcctx->tuple_desc);
        // restore memory context
        MemoryContextSwitchTo(oldcontext);
    }
    funcctx = SRF_PERCALL_SETUP(); 
    args = funcctx->user_fctx; 
    call_nr = funcctx->call_cntr;
    
    if (call_nr < funcctx->max_calls) {
        HeapTuple rettuple;
        Datum retvals[4];
        bool retnulls[4];
        for(i=0;i<3;i++){
            retvals[i] = Int32GetDatum(args->argvals[ips[call_nr][i]]);
            retnulls[i] = args->argnulls[ips[call_nr][i]];
        }
        retvals[3] = Int32GetDatum(args->argvals[ips[call_nr][0]]
                                           * args->argvals[ips[call_nr][1]]
                                           + args->argvals[ips[call_nr][2]]);
        retnulls[3] = args->anyargnull;
        rettuple = heap_form_tuple(funcctx->tuple_desc, retvals, retnulls);
        SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum( rettuple ));
    }
    else    /* do when there is no more left */
    {
        SRF_RETURN_DONE(funcctx);
    }
}

