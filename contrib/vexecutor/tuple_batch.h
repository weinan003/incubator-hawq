#ifndef __TUPLE_BATCH_H__
#define __TUPLE_BATCH_H__

#include "executor/tuptable.h"
#include "executor/execHHashagg.h"

#define BATCH_SIZE 1024

typedef struct BatchAggGroupData BatchAggGroupData;

typedef struct TupleColumnData {
	Datum	*values;
	bool	*nulls;
} TupleColumnData;

typedef struct TupleBatchData {
	// for original scan tuples
	int				nrow;
	int				ncol;
	bool			*projs;
	
	// for projection
	int				nvalid;
	int				*vprojs;

	// original scan column data
	TupleColumnData	**columnDataArray;

	// row tuple 
	TupleDesc		tupDesc;
	TupleTableSlot 	*rowSlot;
	int				rowIdx;

	// only for batch hash agg
	BatchAggGroupData *agg_groupdata;

	// for qualify
	Datum			*skip;
} TupleBatchData, *TupleBatch;

TupleBatch createTupleBatch(int nrow, int ncol, TupleDesc tupdesc, bool *projs);
void destroyTupleBatch(TupleBatch tb);
TupleColumnData *getTupleBatchColumn(TupleBatch tb, int colIdx);
void setTupleBatchNValid(TupleBatch tb, int ncol);
void setTupleBatchProjColumn(TupleBatch tb, int colIdx, int value);
TupleBatch createMaxTupleBatch(int ncol, TupleDesc tupdesc, bool *projs);

TupleTableSlot *getNextRowFromTupleBatch(TupleBatch tb, TupleDesc tupdesc);
void assignNextRowFromTupleBatch(TupleBatch tb, TupleTableSlot *slot);
void resetTupleBatch(TupleBatch tb);


#endif
