/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "vtype.h"

#define MAX_NUM_LEN 32
extern int BATCHSIZE;
static char canary = 0xe7;
#define CANARYSIZE  sizeof(canary)
#define VTYPEHEADERSZ (sizeof(vtype))
#define VDATUMSZ(dim) (sizeof(Datum) * dim)
#define ISNULLSZ(dim) (sizeof(bool) * dim)

#define VTYPESIZE(dim) (VTYPEHEADERSZ + VDATUMSZ(dim) + CANARYSIZE + ISNULLSZ(dim))
#define CANARYOFFSET(vtype) ((char*)(vtype + VTYPEHEADERSZ + VDATUMSZ(dim)))
#define ISNULLOFFSET(vtype) ((bool*)(vtype + VTYPEHEADERSZ + VDATUMSZ(dim) + CANARYSIZE))

vtype* buildvtype(Oid elemtype,int dim,bool *skip)
{
    vtype *res;
    res = palloc0(VTYPESIZE(dim));
    res->dim = dim;
    res->elemtype = elemtype;

    *CANARYOFFSET(res) = canary;
    res->isnull = ISNULLOFFSET(res);
    res->skipref = skip;

    return res;
}

void destroyvtype(vtype** vt)
{
    pfree((*vt));
    *vt = NULL;
}

/*
 * IN function for the abstract data types
 * e.g. Datum vint2in(PG_FUNCTION_ARGS)
 */
#define FUNCTION_IN(type, typeoid, MACRO_DATUM) \
PG_FUNCTION_INFO_V1(v##type##in); \
Datum \
v##type##in(PG_FUNCTION_ARGS) \
{ \
    char *intString = PG_GETARG_CSTRING(0); \
    vtype *res = NULL; \
    char *tempstr[MAX_NUM_LEN] = {0}; \
    int n = 0; \
    res = buildvtype(typeoid,BATCHSIZE,NULL);\
    for (n = 0; *intString && n < BATCHSIZE; n++) \
    { \
    	    char *start = NULL;\
        while (*intString && isspace((unsigned char) *intString)) \
            intString++; \
        if (*intString == '\0') \
            break; \
        start = intString; \
        while ((*intString && !isspace((unsigned char) *intString)) && *intString != '\0') \
            intString++; \
        Assert(intString - start < MAX_NUM_LEN); \
        strncpy(tempstr, start, intString - start); \
        tempstr[intString - start] = 0; \
        res->values[n] =  MACRO_DATUM(DirectFunctionCall1(type##in, CStringGetDatum(tempstr))); \
        while (*intString && !isspace((unsigned char) *intString)) \
            intString++; \
    } \
    while (*intString && isspace((unsigned char) *intString)) \
        intString++; \
    if (*intString) \
        ereport(ERROR, \
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
                errmsg("int2vector has too many elements"))); \
    res->elemtype = typeoid; \
    res->dim = n; \
    PG_RETURN_POINTER(res); \
}

/*
 * OUT function for the abstract data types
 * e.g. Datum vint2out(PG_FUNCTION_ARGS)
 */
#define FUNCTION_OUT(type, typeoid, MACRO_DATUM) \
PG_FUNCTION_INFO_V1(v##type##out); \
Datum \
v##type##out(PG_FUNCTION_ARGS) \
{ \
	vtype * arg1 = (v##type *) PG_GETARG_POINTER(0); \
	int len = arg1->dim; \
    int i = 0; \
	char *rp; \
	char *result; \
	rp = result = (char *) palloc0(len * MAX_NUM_LEN + 1); \
	for (i = 0; i < len; i++) \
	{ \
		if (i != 0) \
			*rp++ = ' '; \
		strcat(rp, DatumGetCString(DirectFunctionCall1(type##out, MACRO_DATUM(arg1->values[i]))));\
		while (*++rp != '\0'); \
	} \
	*rp = '\0'; \
	PG_RETURN_CSTRING(result); \
}

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP(type1, type2, opsym, opstr) \
PG_FUNCTION_INFO_V1(v##type1##v##type2##opstr); \
Datum \
v##type1##v##type2##opstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type1 *arg1 = PG_GETARG_POINTER(0); \
    v##type2 *arg2 = PG_GETARG_POINTER(1); \
    bool *res = arg1->skipref; \
    size = arg1->dim < arg2->dim ? arg1->dim : arg2->dim; \
    while(i < size) \
    { \
        res[i] = arg1->values[i] opsym arg2->values[i]; \
        i++; \
    } \
    PG_RETURN_POINTER(res); \
}

/*
 * Operator function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2pl(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, opsym, opstr) \
PG_FUNCTION_INFO_V1(v##type##const_type##opstr); \
Datum \
v##type##const_type##opstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type *arg1 = PG_GETARG_POINTER(0); \
    const_type arg2 = CONST_ARG_MACRO(1); \
    bool *res = arg1->skipref; \
    size = arg1->dim;\
    while(i < size) \
    { \
        res[i] = ((type)(arg1->values[i])) opsym arg2; \
        i ++ ;\
    } \
    PG_RETURN_POINTER(res); \
}

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP V-types.
 * e.g. extern Datum vint2vint2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP(type1, type2, cmpsym, cmpstr) \
PG_FUNCTION_INFO_V1(v##type1##v##type2##cmpstr); \
Datum \
v##type1##v##type2##cmpstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type1 *arg1 = PG_GETARG_POINTER(0); \
    v##type2 *arg2 = PG_GETARG_POINTER(1); \
    bool *res = arg1->skipref; \
    size = arg1->dim; \
    while(i < size) \
    { \
        res[i] = ((type1)(arg1->values[i])) cmpsym ((type2)(arg2->values[i])); \
        i++; \
    } \
    PG_RETURN_POINTER(res); \
}

/*
 * Comparision function for the abstract data types, this MACRO is used for the 
 * V-types OP Consts.
 * e.g. extern Datum vint2int2eq(PG_FUNCTION_ARGS);
 */
#define __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, cmpsym, cmpstr) \
PG_FUNCTION_INFO_V1(v##type##const_type##cmpstr); \
Datum \
v##type##const_type##cmpstr(PG_FUNCTION_ARGS) \
{ \
    int size = 0; \
    int i = 0; \
    v##type *arg1 = PG_GETARG_POINTER(0); \
    const_type arg2 = CONST_ARG_MACRO(1); \
    bool *res = arg1->skipref; \
    size = arg1->dim; \
    while(i < size) \
    { \
        res[i] = ((type)(arg1->values[i])) cmpsym arg2; \
        i++; \
    } \
    PG_RETURN_POINTER(res); \
}

//Macro Level 3
/* These MACRO will be expanded when the code is compiled. */
#define _FUNCTION_OP(type1, type2) \
    __FUNCTION_OP(type1, type2, +, pl)  \
    __FUNCTION_OP(type1, type2, -, mi)  \
    __FUNCTION_OP(type1, type2, *, mul) \
    __FUNCTION_OP(type1, type2, /, div)

#define _FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, +, pl)  \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, -, mi)  \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, *, mul) \
    __FUNCTION_OP_RCONST(type, const_type, CONST_ARG_MACRO, /, div)

#define _FUNCTION_CMP(type1, type2) \
    __FUNCTION_CMP(type1, type2, ==, eq) \
    __FUNCTION_CMP(type1, type2, !=, ne) \
    __FUNCTION_CMP(type1, type2, >, gt) \
    __FUNCTION_CMP(type1, type2, >=, ge) \
    __FUNCTION_CMP(type1, type2, <, lt) \
    __FUNCTION_CMP(type1, type2, <=, le)

#define _FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, ==, eq)  \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, !=, ne)  \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO,  >, gt) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, >=, ge) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO,  <, lt) \
    __FUNCTION_CMP_RCONST(type, const_type, CONST_ARG_MACRO, <=, le) \

//Macro Level 2
#define FUNCTION_OP(type) \
    _FUNCTION_OP(type, int2) \
    _FUNCTION_OP(type, int4) \
    _FUNCTION_OP(type, int8) \
    _FUNCTION_OP(type, float4) \
    _FUNCTION_OP(type, float8)

#define FUNCTION_OP_RCONST(type) \
    _FUNCTION_OP_RCONST(type, int2, PG_GETARG_INT16) \
    _FUNCTION_OP_RCONST(type, int4, PG_GETARG_INT32) \
    _FUNCTION_OP_RCONST(type, int8, PG_GETARG_INT64) \
    _FUNCTION_OP_RCONST(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_OP_RCONST(type, float8, PG_GETARG_FLOAT8)

#define FUNCTION_CMP(type1) \
    _FUNCTION_CMP(type1, int2) \
    _FUNCTION_CMP(type1, int4) \
    _FUNCTION_CMP(type1, int8) \
    _FUNCTION_CMP(type1, float4) \
    _FUNCTION_CMP(type1, float8)

#define FUNCTION_CMP_RCONST(type) \
    _FUNCTION_CMP_RCONST(type, int2, PG_GETARG_INT16) \
    _FUNCTION_CMP_RCONST(type, int4, PG_GETARG_INT32) \
    _FUNCTION_CMP_RCONST(type, int8, PG_GETARG_INT64) \
    _FUNCTION_CMP_RCONST(type, float4, PG_GETARG_FLOAT4) \
    _FUNCTION_CMP_RCONST(type, float8, PG_GETARG_FLOAT8)

//Macro Level 1
#define FUNCTION_OP_ALL(type) \
    FUNCTION_OP(type) \
    FUNCTION_OP_RCONST(type) \
    FUNCTION_CMP(type) \
    FUNCTION_CMP_RCONST(type) 

//Macro Level 0
FUNCTION_OP_ALL(int2)
FUNCTION_OP_ALL(int4)
FUNCTION_OP_ALL(int8)
FUNCTION_OP_ALL(float4)
FUNCTION_OP_ALL(float8)
FUNCTION_OP_ALL(bool)

FUNCTION_IN(int2, INT2OID, DatumGetInt16)
FUNCTION_IN(int4, INT4OID, DatumGetInt32)
FUNCTION_IN(int8, INT8OID, DatumGetInt64)
FUNCTION_IN(float4, FLOAT4OID, DatumGetFloat4)
FUNCTION_IN(float8, FLOAT8OID, DatumGetFloat8)
FUNCTION_IN(bool, BOOLOID, DatumGetBool)

FUNCTION_OUT(int2, INT2OID, Int16GetDatum)
FUNCTION_OUT(int4, INT4OID, Int32GetDatum)
FUNCTION_OUT(int8, INT8OID, Int64GetDatum)
FUNCTION_OUT(float4, FLOAT4OID, Float4GetDatum)
FUNCTION_OUT(float8, FLOAT8OID, Float8GetDatum)
FUNCTION_OUT(bool, BOOLOID, BoolGetDatum)