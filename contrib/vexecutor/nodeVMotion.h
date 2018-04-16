#ifndef NODEVMOTION_H
#define NODEVMOTION_H

#include "postgres.h"
#include "executor/nodeMotion.h"

#include "access/heapam.h"
#include "nodes/execnodes.h" /* Slice, SliceTable */
#include "cdb/cdbheap.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbhash.h"
#include "executor/executor.h"
#include "executor/execdebug.h"
#include "executor/nodeMotion.h"
#include "utils/tuplesort.h"
#include "utils/tuplesort_mk.h"
#include "utils/memutils.h"

extern TupleTableSlot *ExecVMotion(MotionState * node);

#endif
