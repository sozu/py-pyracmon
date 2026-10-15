# Pyracmon

Pyracmon is a Python O/R mapping library that builds models from an existing database schema. It combines model-based CRUD operations, helpers for writing SQL, and graphs for assembling query results into application-specific structures.

Use it when your database schema and SQL are central to your application, and you want to work with records as Python objects and organize related records for further processing or API responses.

## Contents

- [Features and design](#features-and-design)
- [Requirements and installation](#requirements-and-installation)
- [Quick start](#quick-start)
- [Working with SQL](#working-with-sql)
- [Organizing results with graphs](#organizing-results-with-graphs)
- [Type information and model generation](#type-information-and-model-generation)
- [Asynchronous operations](#asynchronous-operations)
- [Additional features and documentation](#additional-features-and-documentation)
- [License](#license)

## Features and design

The database schema is the source of model metadata. Pyracmon reads tables and views through a database dialect and declares model classes in a Python module. Column names, Python types, primary keys, and other metadata come from the database rather than a separate set of handwritten model declarations.

Model classes provide methods for common operations such as inserting records, fetching by primary key, updating matching records, and deleting records. For joins, aggregation, and other custom queries, you write SQL and use helpers to select columns, bind values, and convert result rows into model objects.

Graphs organize records and computed values independently of how they were fetched. You declare the relationships required by your application, then append data from one or more queries. Typed graph views expose that structure through annotated Python classes, and serialization settings control the resulting dictionaries.

Pyracmon does not manage schema migrations or automatically issue queries when you access related records. You control the SQL, the queries you execute, and transaction boundaries. Graph relationships are explicitly declared; foreign keys do not automatically define the complete output structure.

## Requirements and installation

Pyracmon 1.1.0 requires Python 3.12 or later. Database drivers are installed separately from Pyracmon.

| Database | Synchronous driver | Asynchronous driver |
|---|---|---|
| PostgreSQL | `psycopg2` | `psycopg` with `AsyncConnection.connect` |
| MySQL | `PyMySQL` | Not currently supported |

Pyracmon is designed around DB-API 2.0: in principle, a driver that conforms to DB-API 2.0 is all that is required for driver compatibility. The table above lists the driver combinations illustrated here. Some APIs, such as asynchronous operations, have additional interface requirements beyond DB-API 2.0.

To use the version described here, clone the repository and install it from the repository root into your Python environment. Choose the driver for your database; the examples below use PostgreSQL.

```sh
python -m pip install . psycopg2-binary

# Also install this driver to run the asynchronous examples.
python -m pip install 'psycopg[binary]'
```

## Quick start

This example assumes an existing PostgreSQL database named `pyracmon_example`. Create it using your database administration tools and ensure that your user can read schema metadata and perform CRUD operations on the example tables. Apply the following SQL once if the `blog` table does not already exist.

The connection examples use `localhost:35432` with user and password `postgres`. Adjust the host, port, database name, and credentials directly in the code to match your database. Update the connection URLs in the CLI examples to use the same settings.

```sql
CREATE TABLE blog (
    id serial PRIMARY KEY,
    title text NOT NULL
);
```

Create an empty `models.py` next to the following `quickstart.py`. `declare_models()` reads the schema and adds model classes to that module at runtime. The helper restricts discovery to the tables used in this guide.

```python
# quickstart.py

import psycopg2
import models
from pyracmon import connect, declare_models
from pyracmon.dialect import postgresql


def connect_db():
    db = connect(
        psycopg2,
        host="localhost",
        port=35432,
        dbname="pyracmon_example",
        user="postgres",
        password="postgres",
    )
    try:
        declare_models(postgresql, db, models, includes=["blog", "post"])
    except Exception:
        db.close()
        raise
    return db


def main():
    db = connect_db()
    try:
        created = models.blog.insert(db, {"title": "First blog"})
        found = models.blog.fetch(db, created.id)
        print(found.title)  # First blog

        models.blog.update(db, created.id, {"title": "Updated blog"})
        models.blog.delete(db, created.id)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
```

Run `python quickstart.py` in the environment where Pyracmon and the driver are installed. The script inserts a row, reads it, updates it, and deletes it before committing, so the demonstration leaves no additional blog row behind.

The connection wraps the underlying driver connection and forwards connection arguments to that driver. Manage transactions and connection lifetime explicitly: commit successful changes, roll back on failure, and close the connection when it is no longer needed. Later synchronous examples reuse `connect_db()` and should be run from the same directory.

## Working with SQL

For joins and custom queries, select model columns with aliases and combine them into an expression. `read_row()` uses the same expression to split each result row into its corresponding model objects. This section requires the following `post` table in addition to `blog`; both are included in the repository's example schema.

```sql
CREATE TABLE post (
    id serial PRIMARY KEY,
    blog_id integer NOT NULL REFERENCES blog (id) ON DELETE CASCADE,
    title text NOT NULL,
    content text NOT NULL
);
```

The following query fetches posts for a set of blog IDs. It prints one blog title and post title per matching row; if the database has no matching records, it prints nothing.

```python
import models
from quickstart import connect_db
from pyracmon import Q, cursor, read_row, where


db = connect_db()
try:
    blog_ids = [1, 2]
    exp = models.blog.select("b") + models.post.select("p")
    condition, params = where(Q.in_("b", id=blog_ids))
    sql = f"""
        SELECT {exp}
        FROM blog AS b
        INNER JOIN post AS p ON p.blog_id = b.id
        {condition}
        ORDER BY b.id, p.id
        LIMIT $_
    """
    with cursor(db.stmt().execute(sql, *params, 20)) as c:
        for row in c.fetchall():
            record = read_row(row, *exp)
            print(record.b.title, record.p.title)
finally:
    db.rollback()
    db.close()
```

`exp` and `condition` can be interpolated directly into the SQL: `exp` expands to the selected columns with their aliases, and `condition` expands to the `WHERE` clause. This lets you write the query in its SQL form without a separate sequence of query-building method calls.

`$_` marks a bound value. During `db.stmt().execute()`, markers are converted to the driver's parameter style. Pass values as execution arguments rather than interpolating them into SQL. In this example, the blog IDs and limit remain bound parameters, separate from the column expression and condition embedded in the SQL.

For simpler queries, model methods such as `fetch_where()`, `count()`, `update_where()`, and `delete_where()` accept conditions built with `Q`. See the [API reference](https://sozu.github.io/py-pyracmon/) for condition operators, ordering, pagination, bulk operations, and database-specific behavior.

## Organizing results with graphs

A join returns one row for each combination of related records. A graph can organize those rows into blogs with lists of posts, combining repeated occurrences of the same model according to its identity. It can also accept data from several queries and values computed in Python.

### Declare and populate a typed graph

`TNode[ModelType]` describes a node containing a model and any declared children, while `TGraph` describes the roots. Fields can hold a single value or a collection, as shown below. `TEdge` provides a list-like view of graph values rather than a built-in Python list.

| Field type | Example | Value accessed through the typed view |
|---|---|---|
| A query-result value type | `total: int` | A single value, such as a `COUNT(*)` result |
| A `TNode` subclass | `latest_post: PostNode` | A single node with access to its entity and children |
| `TEdge` of a query-result value type | `title_lengths: TEdge[int]` | A collection of values, such as integer query results |
| `TEdge` of a `TNode` subclass | `blogs: TEdge[BlogNode]` | A collection of nodes with their entities and children |

Use a type such as `int` when the driver already returns the corresponding Python value; the annotation does not request a SQL cast or convert arbitrary values. A field without `TEdge` exposes the first value or node. Add `| None` when a single value or node may be absent, as with `latest_post` below.

The following example reads all graph data from SQL results using `append_rows()`: a blog count, the most recent post by ID, and blogs with their posts and post-title lengths. Each assignment maps a result selection or a function of the result row to a declared graph field. The queries use the `blog` and `post` tables introduced above.

```python
import models
from quickstart import connect_db
from pyracmon import TEdge, TGraph, TNode, append_rows, cursor, new_typed_graph


db = connect_db()
try:
    class PostNode(TNode[models.post]):
        pass

    class BlogNode(TNode[models.blog]):
        posts: TEdge[models.post]

    class BlogGraph(TGraph):
        total: int
        latest_post: PostNode | None
        title_lengths: TEdge[int]
        blogs: TEdge[BlogNode]

    graph = new_typed_graph(BlogGraph)

    with cursor(db.stmt().execute("SELECT COUNT(*) FROM blog")) as c:
        append_rows(c, ["total"], graph, total=lambda row: row.total)

    latest = models.post.select("p")
    with cursor(db.stmt().execute(
        f"SELECT {latest} FROM post AS p ORDER BY p.id DESC LIMIT 1"
    )) as c:
        append_rows(c, latest, graph, latest_post=latest.p)

    exp = models.blog.select("b") + models.post.select("p") + "title_length"
    sql = f"""
        SELECT {exp(title_length='LENGTH(p.title)')}
        FROM blog AS b
        INNER JOIN post AS p ON p.blog_id = b.id
        ORDER BY b.id, p.id
    """
    with cursor(db.stmt().execute(sql)) as c:
        append_rows(
            c, exp, graph,
            blogs=exp.b,
            posts=exp.p,
            title_lengths=lambda row: row.title_length,
        )

    print(graph.view.total)
    if graph.view.latest_post is not None:
        print(graph.view.latest_post().title)
    print(list(graph.view.title_lengths))
    for node in graph.view.blogs:
        print(node().title)
        for post in node.posts:
            print(post.title)
finally:
    db.rollback()
    db.close()
```

A string added to `exp`, such as `"title_length"`, names an additional query element. Calling `exp(title_length='LENGTH(p.title)')` supplies that element and renders the complete selection, including the model columns, so it can be embedded in SQL as a single expression. The same name identifies the resulting value as `row.title_length` when reading rows.

The graph contains the count even when no blogs exist; `latest_post` is `None` when there are no posts, and collections are empty when their queries return no rows. The joined query groups repeated blog models by primary key, so each blog with posts appears once with its posts beneath it. `title_lengths` contains one integer per joined row, including repeated lengths. Appending query results changes only the graph and does not write records to the database.

### Serialize a graph

Use `graph_schema()` with `S` settings to convert the populated graph into nested dictionaries and lists. Continuing with the preceding example, `S.head()` selects a single value for the count and latest post, while `S.of()` retains collections. The resulting dictionary can be passed to your application's response or serialization layer.

```python
from pyracmon import S, graph_schema

schema = graph_schema(
    BlogGraph.template(),
    total=S.head(),
    latest_post=S.head(),
    title_lengths=S.of(),
    blogs=S.of(),
    posts=S.of(),
)
result = schema.serialize(graph.view)
print(result)
```

`S` supports output names, single-value selection, value transformations, and documentation metadata. Dictionary serialization does not itself encode values such as dates or decimals as JSON; choose the final encoding in your application. `dump_typed_graph()` serves a different purpose: it exposes declared fields as Python values, retaining model objects and representing a `TNode` as an entity-and-children tuple rather than flattening it into a response dictionary.

You can also declare graphs with `graph_template()` and create them with `new_graph()`. Typed graph declarations provide an annotated view of the underlying graph mechanism. See the [API reference](https://sozu.github.io/py-pyracmon/) for identity policies, graph templates, and serialization options, and [the complete graph example](examples/graph_example.py) for a result assembled from multiple queries.

## Type information and model generation

Pyracmon has several distinct mechanisms for working with types: model stubs for editors and type checkers, generated Python model code, typed graph views, and runtime descriptions of serialized output. Generating a runtime schema does not by itself make every dynamically created model or output dictionary statically known to a type checker.

### Model stubs

The stub command reads the database schema and writes a `.pyi` file next to an existing importable model module. Run it from the directory containing the empty `models.py` introduced above. The stub describes the model API to development tools; runtime model declaration is still needed.

```sh
python -m pyracmon.stub \
  psycopg2 \
  postgresql://postgres:postgres@localhost:35432/pyracmon_example \
  postgresql \
  models
```

For asynchronous model stubs, use `psycopg` and add `--async AsyncConnection.connect`. Alternatively, `declare_models(..., write_stub=True)` or its asynchronous counterpart can write stubs during model declaration. Regenerate stubs when the database schema changes.

### Generated model code

The model generator writes Python model definitions to standard output. Redirect that output to a module if you want to keep schema-derived definitions in source files and import them without declaring the same models from the database at application startup. Regenerate and review the file after schema changes; the live database is still required for queries.

```sh
python -m pyracmon.generate \
  psycopg2 \
  postgresql://postgres:postgres@localhost:35432/pyracmon_example \
  postgresql > generated_models.py
```

### Output schemas

The `GraphSchema` created in the graph example exposes a runtime output type through `schema.schema`. `walk_schema()` turns that type into a dictionary describing its structure. Pass `True` as the second argument to include available documentation metadata, such as column comments supplied by the database dialect.

```python
from pyracmon import walk_schema

print(walk_schema(schema.schema))
print(walk_schema(schema.schema, True))
```

Use this information to build documentation or integrate with your own schema tooling. Conversion to a particular API specification format is outside Pyracmon's built-in output.

## Asynchronous operations

For PostgreSQL, the asynchronous API uses psycopg's asynchronous connection. Await model declaration, CRUD operations, SQL execution, and transaction methods. The following standalone script assumes the `blog` table from the quick start and an empty `models_async.py` next to it.

```python
import asyncio

import psycopg
import models_async
from pyracmon import aconnect, acursor, declare_models_async
from pyracmon.dialect import postgresql


async def main():
    db = await aconnect(
        psycopg.AsyncConnection.connect,
        api=psycopg,
        host="localhost",
        port=35432,
        dbname="pyracmon_example",
        user="postgres",
        password="postgres",
    )
    try:
        await declare_models_async(
            postgresql, db, models_async, includes=["blog"]
        )
        created = await models_async.blog.insert(db, {"title": "Async blog"})
        found = await models_async.blog.fetch(db, created.id)
        print(found.title)  # Async blog

        async with acursor(db.stmt().execute("SELECT $_", 42)) as c:
            row = await c.fetchone()
            print(row[0])  # 42

        await models_async.blog.delete(db, created.id)
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
```

Graph declarations and in-memory graph operations remain synchronous. Use `await append_rows_async(...)` when reading an asynchronous cursor into a graph, then serialize the graph in the same way as a synchronously populated graph. The [asynchronous example](examples/graph_example_async.py) combines asynchronous queries with a typed graph.

## Additional features and documentation

### Testing support

Declaring models with `testing=True` adds testing methods through `TestingMixin` or `AsyncTestingMixin`. Fixture generation fills supported column values while allowing explicit overrides, so a test can focus on the values relevant to its scenario. Utilities also provide table truncation and value matching; consult the API reference for foreign-key handling, configuration, and method-specific limitations.

### Configuration and extension

`default_config()` provides configuration for query logging, parameter logging, type mapping, graph behavior, and fixture generation. Connections can also use custom execution contexts. Pass additional mixin classes to model declaration to add application-specific methods to the generated models.

### Further reading

The [API reference](https://sozu.github.io/py-pyracmon/) covers complete signatures, options, and behavior. The repository examples show larger workflows; start with the synchronous graph example or its asynchronous counterpart after preparing the example database.

| Resource | Purpose |
|---|---|
| [API reference](https://sozu.github.io/py-pyracmon/) | Detailed API documentation |
| [Synchronous graph example](examples/graph_example.py) | Multiple queries, typed graphs, and serialization |
| [Asynchronous graph example](examples/graph_example_async.py) | Asynchronous queries and typed graphs |

## License

Pyracmon is distributed under the MIT License. See [LICENSE](LICENSE) for the full license text.
