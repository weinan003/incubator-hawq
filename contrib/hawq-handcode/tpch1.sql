CREATE OR REPLACE FUNCTION runtpch1()
    RETURNS SETOF RECORD
    AS '$libdir/tpch1', 'runtpch1'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION runtpch2()
    RETURNS SETOF RECORD
    AS '$libdir/tpch1', 'pg_resqueue_status2'
    LANGUAGE C STRICT;
