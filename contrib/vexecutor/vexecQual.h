#ifndef __EXEC_QUAL_H__
#define __EXEC_QUAL_H__

#include "postgres.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/tuptoaster.h"
#include "catalog/pg_type.h"
#include "cdb/cdbvars.h"
#include "cdb/partitionselection.h"
#include "commands/typecmds.h"
#include "executor/execdebug.h"
#include "executor/nodeAgg.h"
#include "executor/nodeSubplan.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "parser/parse_expr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

TupleTableSlot *
VExecProject(ProjectionInfo *projInfo, ExprDoneCond *isDone);
ExprState *
VExecInitExpr(Expr *node, PlanState *parent);


#endif

