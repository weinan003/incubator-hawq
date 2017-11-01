#ifndef __TUPLE_BATCH_H__
#define __TUPLE_BATCH_H__

typedef struct TupleColumnData {
	Datum	*values;
	bool	*nulls;
} TupleColumnData;

typedef struct TupleBatchData {
	// for original scan tuples
	int				nrow;
	int				ncol;
	
	// for projection
	int				nvalid;
	int				*projs;

	// original scan column data
	TupleColumnData	**columnDataArray;
} TupleBatchData, *TupleBatch;

TupleBatch createTupleBatch(int nrow, int ncol);
void destroyTupleBatch(TupleBatch tb);
TupleColumnData *getTupleBatchColumn(TupleBatch tb, int colIdx);
void setTupleBatchNValid(TupleBatch tb, int ncol);
void setTupleBatchProjColumn(TupleBatch tb, int colIdx, int value);
TupleBatch createMaxTupleBatch(void);

#endif
