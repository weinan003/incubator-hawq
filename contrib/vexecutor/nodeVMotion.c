
#include "nodeVMotion.h"
#include "tuplebatch.h"
/*=========================================================================
 * FUNCTIONS PROTOTYPES
 */
static TupleTableSlot *execVMotionSender(MotionState * node);
static TupleTableSlot *execVMotionUnsortedReceiver(MotionState * node);

static void doSendTupleBatch(Motion * motion, MotionState * node, TupleTableSlot *outerTupleSlot);
static SendReturnCode
SendTupleBatch(MotionLayerState *mlStates, ChunkTransportState *transportStates,
               int16 motNodeID, TupleBatch tuplebatch, int16 targetRoute);
/*=========================================================================
 */

/* ----------------------------------------------------------------
 *		ExecVMotion
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecVMotion(MotionState * node)
{
    Motion	   *motion = (Motion *) node->ps.plan;

    /*
     * at the top here we basically decide: -- SENDER vs. RECEIVER and --
     * SORTED vs. UNSORTED
     */
    if (node->mstype == MOTIONSTATE_RECV)
    {
        TupleTableSlot *tuple;
#ifdef MEASURE_MOTION_TIME
        struct timeval startTime;
		struct timeval stopTime;

		gettimeofday(&startTime, NULL);
#endif

        if (node->ps.state->active_recv_id >= 0)
        {
            if (node->ps.state->active_recv_id != motion->motionID)
            {
                elog(LOG, "DEADLOCK HAZARD: Updating active_motion_id from %d to %d",
                     node->ps.state->active_recv_id, motion->motionID);
                node->ps.state->active_recv_id = motion->motionID;
            }
        } else
            node->ps.state->active_recv_id = motion->motionID;

        /* Running in diagnostic mode ? */
        if (Gp_interconnect_type == INTERCONNECT_TYPE_NIL)
        {
            node->ps.state->active_recv_id = -1;
            return NULL;
        }

        if (motion->sendSorted)
        {
            elog(WARNING,"VMOTION DONOT SUPPORT SORT YET");
            if (gp_enable_motion_mk_sort)
                (void) 0;//TODO:: tuple = execMotionSortedReceiver_mk(node);
            else
                (void) 0;//TODO:: tuple = execMotionSortedReceiver(node);
        }
        else
            tuple = execVMotionUnsortedReceiver(node);

        if (tuple == NULL)
            node->ps.state->active_recv_id = -1;
        else
        {
            Gpmon_M_Incr(GpmonPktFromMotionState(node), GPMON_QEXEC_M_ROWSIN);
            Gpmon_M_Incr_Rows_Out(GpmonPktFromMotionState(node));
            setMotionStatsForGpmon(node);
        }
#ifdef MEASURE_MOTION_TIME
        gettimeofday(&stopTime, NULL);

		node->motionTime.tv_sec += stopTime.tv_sec - startTime.tv_sec;
		node->motionTime.tv_usec += stopTime.tv_usec - startTime.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif
        CheckSendPlanStateGpmonPkt(&node->ps);
        return tuple;
    }
    else if(node->mstype == MOTIONSTATE_SEND)
    {
        return execVMotionSender(node);
    }

    Assert(!"Non-active motion is executed");
    return NULL;
}

static TupleTableSlot *
execVMotionSender(MotionState * node)
{
    /* SENDER LOGIC */
    TupleTableSlot *outerTupleSlot;
    PlanState  *outerNode;
    Motion	   *motion = (Motion *) node->ps.plan;
    bool		done = false;


#ifdef MEASURE_MOTION_TIME
    struct timeval time1;
	struct timeval time2;

	gettimeofday(&time1, NULL);
#endif

    AssertState(motion->motionType == MOTIONTYPE_HASH ||
                (motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0) ||
                (motion->motionType == MOTIONTYPE_FIXED && motion->numOutputSegs <= 1));
    Assert(node->ps.state->interconnect_context);

    while (!done)
    {
        /* grab TupleTableSlot from our child. */
        outerNode = outerPlanState(node);
        outerTupleSlot = ExecProcNode(outerNode);

#ifdef MEASURE_MOTION_TIME
        gettimeofday(&time2, NULL);

		node->otherTime.tv_sec += time2.tv_sec - time1.tv_sec;
		node->otherTime.tv_usec += time2.tv_usec - time1.tv_usec;

		while (node->otherTime.tv_usec < 0)
		{
			node->otherTime.tv_usec += 1000000;
			node->otherTime.tv_sec--;
		}

		while (node->otherTime.tv_usec >= 1000000)
		{
			node->otherTime.tv_usec -= 1000000;
			node->otherTime.tv_sec++;
		}
#endif
        /* Running in diagnostic mode, we just drop all tuples. */
        if (Gp_interconnect_type == INTERCONNECT_TYPE_NIL)
        {
            if (!TupIsNull(outerTupleSlot))
                continue;

            return NULL;
        }

        if (done || TupIsNull(outerTupleSlot))
        {
            doSendEndOfStream(motion, node);
            done = true;
        }
        else
        {
            doSendTupleBatch(motion, node, outerTupleSlot);

            Gpmon_M_Incr_Rows_Out(GpmonPktFromMotionState(node));
            setMotionStatsForGpmon(node);
            CheckSendPlanStateGpmonPkt(&node->ps);

            if (node->stopRequested)
            {
                elog(gp_workfile_caching_loglevel, "Motion initiating Squelch walker");
                /* propagate stop notification to our children */
                ExecSquelchNode(outerNode);
                done = true;
            }
        }
#ifdef MEASURE_MOTION_TIME
        gettimeofday(&time1, NULL);

		node->motionTime.tv_sec += time1.tv_sec - time2.tv_sec;
		node->motionTime.tv_usec += time1.tv_usec - time2.tv_usec;

		while (node->motionTime.tv_usec < 0)
		{
			node->motionTime.tv_usec += 1000000;
			node->motionTime.tv_sec--;
		}

		while (node->motionTime.tv_usec >= 1000000)
		{
			node->motionTime.tv_usec -= 1000000;
			node->motionTime.tv_sec++;
		}
#endif
    }

    Assert(node->stopRequested || node->numTuplesFromChild == node->numTuplesToAMS);

    /* nothing else to send out, so we return NULL up the tree. */
    return NULL;
}

void
doSendTupleBatch(Motion * motion, MotionState * node, TupleTableSlot *outerTupleSlot)
{
    int16		    targetRoute;
    HeapTuple       tuple;
    SendReturnCode  sendRC;
    ExprContext    *econtext = node->ps.ps_ExprContext;

    /* We got a tuple from the child-plan. */
    node->numTuplesFromChild++;

    if (motion->motionType == MOTIONTYPE_FIXED)
    {
        if (motion->numOutputSegs == 0) /* Broadcast */
        {
            targetRoute = BROADCAST_SEGIDX;
        }
        else /* Fixed Motion. */
        {
            Assert(motion->numOutputSegs == 1);
            /*
             * Actually, since we can only send to a single output segment
             * here, we are guaranteed that we only have a single
             * targetRoute setup that we could possibly send to.  So we
             * can cheat and just fix the targetRoute to 0 (the 1st
             * route).
             */
            targetRoute = 0;
        }
    }
    else if (motion->motionType == MOTIONTYPE_HASH) /* Redistribute */
    {
        uint32		hval = 0;

        Assert(motion->numOutputSegs > 0);
        Assert(motion->outputSegIdx != NULL);

        econtext->ecxt_outertuple = outerTupleSlot;

        Assert(node->cdbhash->numsegs == motion->numOutputSegs);

        hval = evalHashKey(econtext, node->hashExpr,
                           motion->hashDataTypes, node->cdbhash);

        Assert(hval < GetQEGangNum() && "redistribute destination outside segment array");

        /* hashSegIdx takes our uint32 and maps it to an int, and here
         * we assign it to an int16. See below. */
        targetRoute = motion->outputSegIdx[hval];

        /* see MPP-2099, let's not run into this one again! NOTE: the
         * definition of BROADCAST_SEGIDX is key here, it *cannot* be
         * a valid route which our map (above) will *ever* return.
         *
         * Note the "mapping" is generated at *planning* time in
         * makeDefaultSegIdxArray() in cdbmutate.c (it is the trivial
         * map, and is passed around our system a fair amount!). */
        Assert(targetRoute != BROADCAST_SEGIDX);
    }
    else /* ExplicitRedistribute */
    {
        Datum segidColIdxDatum;

        Assert(motion->segidColIdx > 0 && motion->segidColIdx <= list_length((motion->plan).targetlist));
        bool is_null = false;
        segidColIdxDatum = slot_getattr(outerTupleSlot, motion->segidColIdx, &is_null);
        targetRoute = Int32GetDatum(segidColIdxDatum);
        Assert(!is_null);
    }


    /* send the tuple out. */
    sendRC = SendTupleBatch(node->ps.state->motionlayer_context,
                            node->ps.state->interconnect_context,
                            motion->motionID,
                            outerTupleSlot->PRIVATE_tb,
                            targetRoute);

    Assert(sendRC == SEND_COMPLETE || sendRC == STOP_SENDING);
    if (sendRC == SEND_COMPLETE)
        node->numTuplesToAMS++;
    else
        node->stopRequested = true;


#ifdef CDB_MOTION_DEBUG
    if (sendRC == SEND_COMPLETE && node->numTuplesToAMS <= 20)
	{
		StringInfoData  buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "   motion%-3d snd->%-3d, %5d.",
				motion->motionID,
				targetRoute,
				node->numTuplesToAMS);
		formatTuple(&buf, tuple, ExecGetResultType(&node->ps),
				node->outputFunArray);
		elog(DEBUG3, buf.data);
		pfree(buf.data);
	}
#endif
}

TupleTableSlot *execVMotionUnsortedReceiver(MotionState * node)
{
    /* RECEIVER LOGIC */
    TupleTableSlot *slot;
    HeapTuple	tuple;
    Motion	   *motion = (Motion *) node->ps.plan;
    ReceiveReturnCode recvRC;

    AssertState(motion->motionType == MOTIONTYPE_HASH ||
                (motion->motionType == MOTIONTYPE_EXPLICIT && motion->segidColIdx > 0) ||
                (motion->motionType == MOTIONTYPE_FIXED && motion->numOutputSegs <= 1));

    Assert(node->ps.state->motionlayer_context);
    Assert(node->ps.state->interconnect_context);

    if (node->stopRequested)
    {
        SendStopMessage(node->ps.state->motionlayer_context,
                        node->ps.state->interconnect_context,
                        motion->motionID);
        return NULL;
    }

    recvRC = RecvTupleFrom(node->ps.state->motionlayer_context,
                           node->ps.state->interconnect_context,
                           motion->motionID, &tuple, ANY_ROUTE);

    if (recvRC == END_OF_STREAM)
    {
#ifdef CDB_MOTION_DEBUG
        if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		    elog(DEBUG4, "motionID=%d saw end of stream", motion->motionID);
#endif
        Assert(node->numTuplesFromAMS == node->numTuplesToParent);
        Assert(node->numTuplesFromChild == 0);
        Assert(node->numTuplesToAMS == 0);
        return NULL;
    }

    node->numTuplesFromAMS++;
    node->numTuplesToParent++;

    /* store it in our result slot and return this. */
    slot = node->ps.ps_ResultTupleSlot;
    slot->PRIVATE_tb = tuple;
    //slot = ExecStoreGenericTuple(tuple, slot, true /* shouldFree */);

#ifdef CDB_MOTION_DEBUG
    if (node->numTuplesToParent <= 20)
    {
        StringInfoData  buf;

        initStringInfo(&buf);
        appendStringInfo(&buf, "   motion%-3d rcv      %5d.",
                         motion->motionID,
                         node->numTuplesToParent);
        formatTuple(&buf, tuple, ExecGetResultType(&node->ps),
                    node->outputFunArray);
        elog(DEBUG3, buf.data);
        pfree(buf.data);
    }
#endif

    return slot;
}

/*
 * Function:  SendTupleBatch - Sends a batch of tuples to the AMS layer.
 */
SendReturnCode
SendTupleBatch(MotionLayerState *mlStates,
          ChunkTransportState *transportStates,
          int16 motNodeID,
          TupleBatch tuplebatch,
          int16 targetRoute)
{
    MotionNodeEntry *pMNEntry;
    TupleChunkListData tcList;
    MemoryContext oldCtxt;
    SendReturnCode rc;

    /*
     * Analyze tools.  Do not send any thing if this slice is in the bit mask
     */
    if (gp_motion_slice_noop != 0 && (gp_motion_slice_noop & (1 << currentSliceId)) != 0)
        return SEND_COMPLETE;

    /*
     * Pull up the motion node entry with the node's details.  This includes
     * details that affect sending, such as whether the motion node needs to
     * include backup segment-dbs.
     */
    pMNEntry = getMotionNodeEntry(mlStates, motNodeID, "SendTuple");

    /*
    if (targetRoute != BROADCAST_SEGIDX)
    {
        struct directTransportBuffer b;

        getTransportDirectBuffer(transportStates, motNodeID, targetRoute, &b);

        if (b.pri != NULL && b.prilen > TUPLE_CHUNK_HEADER_SIZE)
        {
            int sent = 0;

            sent = SerializeTupleDirect(tuple, &pMNEntry->ser_tup_info, &b);
            if (sent > 0)
            {
                putTransportDirectBuffer(transportStates, motNodeID, targetRoute, sent);

                tcList.num_chunks = 1;
                tcList.serialized_data_length = sent;

                statSendTuple(mlStates, pMNEntry, &tcList);

                return SEND_COMPLETE;
            }
        }
    }
    */

    /* Create and store the serialized form, and some stats about it. */
    oldCtxt = MemoryContextSwitchTo(mlStates->motion_layer_mctx);

    MemTuple tuple = palloc0(sizeof(MemTuple) + 1024);
    for(int i = 0;i < 1024;i ++)
        tuple->PRIVATE_mt_bits[i] = (char)(i % 255);
    memtuple_set_size(tuple,NULL,sizeof(MemTuple) + 1024);
    SerializeTupleIntoChunks(tuple, &pMNEntry->ser_tup_info, &tcList);
    pfree(tuple);

    MemoryContextSwitchTo(oldCtxt);

    /* do the send. */
    if (!SendTupleChunkToAMS(mlStates, transportStates, motNodeID, targetRoute, tcList.p_first))
    {
        pMNEntry->stopped = true;
        rc = STOP_SENDING;
    }
    else
    {
        /* update stats */
        statSendTuple(mlStates, pMNEntry, &tcList);

        rc = SEND_COMPLETE;
    }

    /* cleanup */
    clearTCList(&pMNEntry->ser_tup_info.chunkCache, &tcList);

    return rc;
}
