DROP VIEW IF EXISTS v1;
DROP TABLE IF EXISTS t4;
DROP TABLE IF EXISTS t3;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t1;

/* Single auto-increment PK */
CREATE TABLE t1 (
    c11 integer NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'comment of c11',
    c12 integer NOT NULL COMMENT 'comment of c12',
    c13 text NOT NULL COMMENT 'comment of c13'
) COMMENT = 'comment of t1';

/* Multiple PK */
CREATE TABLE t2 (
    c21 integer NOT NULL,
    c22 integer NOT NULL,
    c23 text NOT NULL,
    PRIMARY KEY (c21, c22)
);

/* Foreign PK */
CREATE TABLE t3 (
    c31 integer PRIMARY KEY,
    c32 integer NOT NULL,
    c33 text,
    FOREIGN KEY (c31) REFERENCES t1 (c11) ON DELETE CASCADE
);

/* Multiple foreign PK */
CREATE TABLE t4 (
    c41 integer NOT NULL,
    c42 integer NOT NULL,
    c43 integer NOT NULL,
    FOREIGN KEY (c41) REFERENCES t1 (c11) ON DELETE CASCADE,
    FOREIGN KEY (c42, c43) REFERENCES t2 (c21, c22) ON DELETE CASCADE,
    PRIMARY KEY (c41, c42, c43)
);

CREATE VIEW v1 AS
    SELECT
        t1.c11, t1.c12, t3.c31, t3.c32
    FROM 
        t1
        INNER JOIN t3 ON t1.c11 = t3.c31;