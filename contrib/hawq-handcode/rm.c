#include "postgres.h"
#include "c.h"
#include "funcapi.h"
#include "fmgr.h"

extern text *cstring_to_text(const char *s);

PG_FUNCTION_INFO_V1(pg_resqueue_status2);

struct tpch2_args {
    int32     id;
    char    text[128];
};


Datum pg_resqueue_status2(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx    = NULL;
    Datum            result;
    MemoryContext    oldcontext = NULL;
    HeapTuple        tuple      = NULL;
    
    struct tpch2_args*  funcdata;

    if (SRF_IS_FIRSTCALL())
    {

        funcctx = SRF_FIRSTCALL_INIT();

        /* Switch context when allocating stuff to be used in later calls */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        funcdata = palloc0(sizeof(struct tpch2_args));
        funcctx->user_fctx = (void *)funcdata; 
        
        TupleDesc tupledesc = CreateTemplateTupleDesc(
                                    10,
                                    false);
        TupleDescInitEntry(tupledesc, (AttrNumber) 1,  "rsqname",   TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 2,  "segmem",    TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 3,  "segcore",   TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 4,  "segsize",   TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 5,  "segsizemax",TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 6,  "inusemem",  TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 7,  "inusecore", TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 8,  "rsqholders",TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 9,  "rsqwaiters",TEXTOID, -1, 0);
        TupleDescInitEntry(tupledesc, (AttrNumber) 10, "paused",    TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupledesc);

        /* Return to original context when allocating transient memory */
        MemoryContextSwitchTo(oldcontext);

        //funcctx->max_calls = ((DQueue)(funcctx->user_fctx))->NodeCount;
        funcctx->max_calls = 6;
    }

    funcctx = SRF_PERCALL_SETUP();

    if (funcctx->call_cntr < funcctx->max_calls)
    {
        Datum       values[10];
        bool        nulls[10];
        char        buf[1024];

        for (int i=0;i<10;i++)
        {
            nulls[i] = false;
        }

        funcdata = funcctx->user_fctx;
        funcdata->id = funcctx->call_cntr;
        snprintf(funcdata->text, sizeof(funcdata->text), "%s", "aaa");
       
        snprintf(buf, sizeof(buf), "%d", 0);
        values[0] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%d", 1);
        values[1] = PointerGetDatum(cstring_to_text(buf));  
        snprintf(buf, sizeof(buf), "%f", 2);
        values[2] = PointerGetDatum(cstring_to_text(buf));  
        snprintf(buf, sizeof(buf), "%d", 3);
        values[3] = PointerGetDatum(cstring_to_text(buf));  
        snprintf(buf, sizeof(buf), "%d", 4);
        values[4] = PointerGetDatum(cstring_to_text(buf));  
        snprintf(buf, sizeof(buf), "%d", 5);
        values[5] = PointerGetDatum(cstring_to_text(buf));  
        snprintf(buf, sizeof(buf), "%f", 6);
        values[6] = PointerGetDatum(cstring_to_text(buf));  
        snprintf(buf, sizeof(buf), "%d", 7);
        values[7] = PointerGetDatum(cstring_to_text(buf));  
        snprintf(buf, sizeof(buf), "%d", 8);
        values[8] = PointerGetDatum(cstring_to_text(buf));
        snprintf(buf, sizeof(buf), "%c", 9);
        values[9] = PointerGetDatum(cstring_to_text(buf));


        elog(LOG, "pg_resqueue_status: values[0]=%p", values[0]);
        elog(LOG, "pg_resqueue_status: values[1]=%p", values[1]);
        elog(LOG, "pg_resqueue_status: values[2]=%p", values[2]);
    
        /* Build and return the tuple. */
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);

        SRF_RETURN_NEXT(funcctx, result);
    }
    else {
        SRF_RETURN_DONE(funcctx);
    }
}

