# Python O/R mapping extension for DB-API 2.0

## Overview

This library provides following functionalities designed to simplify python codes handling database operations.

- Implicit declaration of model class which represents a table and whose instance represents a record.
- Functions executing simple INSERT/UPDATE/DELETE statements.
- Utilities to construct SELECT queries with less boilerplate codes.
- Data structure which reflects relationships between records.

On the other hand, this library does NOT support following features which are common in libraries called O/R mapper.

- Query generation which resolves relations (foreign keys) automatically.
- APIs to build complete query without having to write SQL strings.
- Object based query operation such as lazy loading or dirty state handling.
- Database migration based on entity declarations.

## Prerequisite

- Python >= 3.6
- Supporting DBMS
    - MySQL >= 8.0
    - PostgreSQL >= 10

## Getting started

### Prepare tables

Suppose, you have a database including some tables defined as following queries (written in PostgreSQL syntax).

```
CREATE TABLE blog (
    id serial PRIMARY KEY,
    title text NOT NULL
);

CREATE TABLE post (
    id serial PRIMARY KEY,
    blog_id integer NOT NULL REFERENCES blog (id) ON DELETE CASCADE,
    title text NOT NULL,
    content text NOT NULL
);

CREATE TABLE image (
    post_id integer NOT NULL REFERENCES post (id) ON DELETE CASCADE,
    url text NOT NULL
);
```

### Declare models

First, you should connect the database via some module supporting the DBMS and conforming to DB-API 2.0. Call `pyracmon.connection.connect` with the DB-API 2.0 module and arguments specific for `connect` function it exports. The example here shows the case using `psycopg2`.

```
import psycopg2
from pyracmon.connection import connect

db = connect(psycopg2, dbname="example", user="postgres", password="postgres")
```

The function returns a wrapper of connection which also conforms to the specification of `Connection` in DB-API 2.0. Therefore, you can use this object in the same way of connection obtained by `psycopg2.connect`. You can declare models of all tables in the database via this object.

```
import pyracmon
from pyracmon.dialect import postgresql

pyracmon.declare_models(postgresql, db)
# import my.models
# declare_models(postgresql, db, my.models)

assert pyracmon.blog.name == "blog"
```

`declare_models` adds the definition of model classes into `pyracmon` module if target module is not specified. Commented lines show the way to specify the module. After the function call, model classes which have the same name as the name of the originated table are exported from the module.

### Insert, Update and Delete

### Select and create a graph
