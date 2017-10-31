#ifndef __TUPLE_BATCH_H__
#define __TUPLE_BATCH_H__

typedef struct TupleColumnData {
	Datum	*values;
	bool	*nulls;
} TupleColumnData;

typedef struct TupleBatchData {
	int				nrow;
	int				ncol;
	TupleColumnData	**columnDataArray;
} TupleBatchData, *TupleBatch;

TupleBatch createTupleBatch(int nrow, int ncol);
void destroyTupleBatch(TupleBatch tc);
TupleColumnData *getTupleBatchColumn(TupleBatch tc, int colIdx);



#endif
