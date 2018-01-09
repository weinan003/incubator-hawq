#ifndef __VINT_H__
#define __VINT_H__

#include "postgres.h"

#include <ctype.h>
#include <limits.h>

#include "catalog/pg_type.h"
#include "funcapi.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/builtins.h"

int
batch_int4pl(Datum *arg1, Datum *arg2, int *batch_size, Datum *result);

int
batch_int4mi(Datum *arg1, Datum *arg2, int *batch_size, Datum *result);

int
batch_int4mul(Datum *arg1, Datum *arg2, int *batch_size, Datum *result);
#endif
