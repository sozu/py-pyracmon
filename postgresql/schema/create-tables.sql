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
)