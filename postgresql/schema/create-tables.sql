DROP MATERIALIZED VIEW IF EXISTS mv2;
DROP TABLE IF EXISTS types;
DROP TYPE IF EXISTS t_record;
DROP TYPE IF EXISTS t_enum;
DROP MATERIALIZED VIEW IF EXISTS mv1;
DROP VIEW IF EXISTS v1;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;

/* Single auto-increment PK */
CREATE TABLE t1 (
    c11 serial PRIMARY KEY,
    c12 integer NOT NULL,
    c13 text NOT NULL
);

COMMENT ON TABLE t1 IS 'comment of t1';
COMMENT ON COLUMN t1.c11 IS 'comment of c11';
COMMENT ON COLUMN t1.c12 IS 'comment of c12';
COMMENT ON COLUMN t1.c13 IS 'comment of c13';

/* Multiple PK */
CREATE TABLE t2 (
    c21 integer NOT NULL,
    c22 integer NOT NULL,
    c23 text NOT NULL,
    PRIMARY KEY (c21, c22)
);

/* Foreign PK */
CREATE TABLE t3 (
    c31 integer PRIMARY KEY REFERENCES t1 (c11) ON DELETE CASCADE,
    c32 serial NOT NULL,
    c33 text
);

/* Multiple foreign PK */
CREATE TABLE t4 (
    c41 integer NOT NULL REFERENCES t1 (c11) ON DELETE CASCADE,
    c42 integer NOT NULL,
    c43 integer NOT NULL,
    FOREIGN KEY (c42, c43) REFERENCES t2 (c21, c22) ON DELETE CASCADE,
    PRIMARY KEY (c41, c42, c43)
);

CREATE VIEW v1 AS
    SELECT
        t1.c11, t1.c12, t3.c31, t3.c32
    FROM 
        t1
        INNER JOIN t3 ON t1.c11 = t3.c31;

COMMENT ON VIEW v1 IS 'comment of v1';
COMMENT ON COLUMN v1.c11 IS 'comment of c11 in v1';

CREATE MATERIALIZED VIEW mv1 AS
    SELECT
        t1.c11, t1.c12, t3.c31, t3.c32
    FROM 
        t1
        INNER JOIN t3 ON t1.c11 = t3.c31;

COMMENT ON MATERIALIZED VIEW mv1 IS 'comment of mv1';
COMMENT ON COLUMN mv1.c11 IS 'comment of c11 in mv1';

CREATE TYPE t_enum AS ENUM ('a', 'b');
CREATE TYPE t_record AS (
    r1 integer,
    r2 integer
);

CREATE TABLE types (
    bool_ boolean,
    double_ real,
    int_ integer,
    string_ text,
    bytes_ bytea,
    date_ date,
    datetime_ timestamp with time zone,
    time_ time,
    delta_ interval,
    uuid_ uuid,
    enum_ t_enum,
    record_ t_record,
    array_ integer[],
    deeparray_ integer[][],
    json_ json,
    jsonb_ jsonb
);

CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM types;