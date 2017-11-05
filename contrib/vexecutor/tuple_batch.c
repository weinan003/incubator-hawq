#include "postgres.h"
#include "tuple_batch.h"

#define TUPLE_BATCH_ROW_MAX_SIZE	100000

TupleBatch createMaxTupleBatch(int ncol)
{
	return createTupleBatch(TUPLE_BATCH_ROW_MAX_SIZE, ncol);
}

TupleBatch createTupleBatch(int nrow, int ncol)
{
	TupleBatch tb = (TupleBatch)palloc0(sizeof(TupleBatchData));
	tb->nrow = nrow;
	tb->ncol = ncol;

	tb->columnDataArray = (TupleColumnData **)palloc0(tb->ncol * sizeof(TupleColumnData *));
	for (int i=0;i<tb->ncol;i++)
	{
		tb->columnDataArray[i] = (TupleColumnData *)palloc0(sizeof(TupleColumnData));
		tb->columnDataArray[i]->values = (Datum *)palloc0(tb->nrow * sizeof(Datum));
		tb->columnDataArray[i]->nulls = (bool *)palloc0(tb->nrow * sizeof(bool));
	}
	
	tb->projs = (int *)palloc0(ncol * sizeof(int));
	tb->nvalid = 0;

	return tb;
}

void destroyTupleBatch(TupleBatch tb)
{
	for (int i=0;i<tb->ncol;i++)
	{
		if (tb->columnDataArray[i])
		{
			if (tb->columnDataArray[i]->values)
			{
				pfree(tb->columnDataArray[i]->values);
			}
			if (tb->columnDataArray[i]->nulls)
			{
				pfree(tb->columnDataArray[i]->nulls);
			}
			pfree(tb->columnDataArray[i]);
		}

		if (tb->projs)
		{
			pfree(tb->projs);
		}
	}
	pfree(tb);
}

TupleColumnData *getTupleBatchColumn(TupleBatch tb, int colIdx)
{
	return tb->columnDataArray[colIdx];
}

void setTupleBatchNValid(TupleBatch tb, int ncol)
{
	tb->nvalid = ncol;
/*
	if (tb->projs)
	{
		pfree(tb->projs);
	}
	tb->projs = (int *)palloc0(ncol * sizeof(int));
*/
}

void setTupleBatchProjColumn(TupleBatch tb, int colIdx, int value)
{
	tb->projs[colIdx] = value;
}
