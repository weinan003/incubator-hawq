#include "postgres.h"
#include "tuple_batch.h"

TupleBatch createTupleBatch(int nrow, int ncol)
{
	TupleBatch tc = (TupleBatch)palloc0(sizeof(TupleBatchData));
	tc->nrow = nrow;
	tc->ncol = ncol;

	tc->columnDataArray = (TupleColumnData **)palloc0(tc->ncol * sizeof(TupleColumnData *));
	for (int i=0;i<tc->ncol;i++)
	{
		tc->columnDataArray[i] = (TupleColumnData *)palloc0(sizeof(TupleColumnData));
		tc->columnDataArray[i]->values = (Datum *)palloc0(tc->nrow * sizeof(Datum));
		tc->columnDataArray[i]->nulls = (bool *)palloc0(tc->nrow * sizeof(bool));
	}

	return tc;
}

void destroyTupleBatch(TupleBatch tc)
{
	for (int i=0;i<tc->ncol;i++)
	{
		if (tc->columnDataArray[i])
		{
			if (tc->columnDataArray[i]->values)
			{
				pfree(tc->columnDataArray[i]->values);
			}
			if (tc->columnDataArray[i]->nulls)
			{
				pfree(tc->columnDataArray[i]->nulls);
			}
			pfree(tc->columnDataArray[i]);
		}
	}
	pfree(tc);
}

TupleColumnData *getTupleBatchColumn(TupleBatch tc, int colIdx)
{
	return tc->columnDataArray[colIdx];
}
