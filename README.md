# Python O/R mapping extension for DB-API 2.0

## Overview

This library provides functionalities to refine codes executing DB operations. The main peculiar concepts are *model declaration* and *relational graph*.

*Model* is the class representing a table and its instance corresponds to a record. This library provides a way to declare model classes by reverse-engineering from actual tables. Once you connect DB, model classes are importable from a module you specified. Those classes have various methods executing simple operations on a table.

*Graph* is composed of *node*s and *edge*s, where each node contains a model object or any kind of value. An edge between nodes represents their relation, such as foreign key constraint between records. *Graph*s are designed to store results of queries with straight-forward codes, which makes you free from suffering from the reconstruction of data structure. Additionally, *graph view* provides intuitive interfaces to traverse it and *graph serialization* feature enables conversion from a *graph* into a `dict` with keeping its structure and applying flexible representation to each node.

On the other hand, this library does NOT support following features which are common in libraries called O/R mapper.

- Query generation resolving relations (foreign keys) automatically.
- Object based query operation such as lazy loading or dirty state handling.
- Code first migrations, that is, database migrations based on entity declarations.

## Features

**DB-API 2.0**

This library works as a wrapper of any kind of DB driver compliant with DB-API 2.0 such as *psycopg2* or *PyMySQL*. DB operations are passed to the driver so that any functionality it provides is also available. 

**Automatic declaration of model types**

Similarly to many other O/R mappers, this library represents a table with a class called *model type*. The characteristic is that the declarations of *model types* are done by a function crawling tables in DB at runtime, not by manual coding.

**Query helpers**

While this library mainly focuses on the use of SQL, not DSL, it exports functions to generate a part of query and each *model type* has methods to execute some routine DB operations on its representing table. They will decrease boilerplate codes to construct query string and parameters. Meanwhile, complicated queries which contain table join, sub-query and so on are not suppoted intentionaly, because it is revealed that trials to manage to them by DSL will end up to bring larger difficulties.

**Record graph**

`Graph` is a type representing tree structure where each node has at most one parent node, where relations between *model*s are represented with edges. By using the graph to store structured records, every *model type* does not require extra attributes which often spoil the type structure. In addition, *graph serialization* feature provides flexible representations of the graph for later use. Therefore, the *graph* contributes decoupling DB operations and data representations in your codes.

**Static typing support**

This library provides the functionality to get the schema of serialized graph statically. For example in RESTful applications, the schema can be used for documentation of HTTP response. Because the schema reflects flexible change on data structure (ex. adding computed values, removing keys from a dictionary) in serialization phase, it no longer necessary to declare a type only to represent response structure. Note that the state of this functionality is still unstable becasue static typing support of python is changing frequently.

**Testing support**

Experimental.

Testing DB operations is an important but difficult task. `pyracmon.testing` provides interfaces to reduce the difficulty. One of them is the generation of fixture by minimal declaration of column values. Feeding values only on columns in interest generates rows by complementing other columns with automatically generated values. Others are in experimental stage and will be changed or removed in future version.

## Prerequisite

Pyracmon requires python 3.6 or higher.

Static typing functionalities are highly affected by python version.
Because of frequent update of python `typing` package, syntax assumed in this library might already have got deprecated.
Those functionalities will be left not completely conforming to specifications of the package while they are unstable.

Currently supported DBMS are PostgreSQL (>= 10.0) and MySQL (>= 8.0).

Although pyracmon does not require any libraries for use by itself, it needs DB driver which conforms to DB-API 2.0.
[psycopg2](https://pypi.org/project/psycopg2/) (for PostgreSQL) and [PyMySQL](https://pypi.org/project/PyMySQL/)_ are used in development.
Use them or some other library and tell it to pyracmon via `pyracmon.declare_models`.

## Installation

```
$ pip install pyracmon==1.0.dev9
```

## Grance of functionalities

Full API documentation is available on <https://sozu.github.io/pyracmon-doc/>.

### Assumption

Suppose, you have a database having tables as follows (written in PostgreSQL syntax).

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

CREATE TABLE blog_category (
    id serial PRIMARY KEY,
    blog_id integer NOT NULL REFERENCES blog (id) ON DELETE CASCADE,
    name text NOT NULL
);

CREATE TABLE post_comment (
    id serial PRIMARY KEY,
    post_id integer NOT NULL REFERENCES post (id) ON DELETE CASCADE,
    likes integer NOT NULL,
    content text NOT NULL
);
```

### Create connection

All operations start with creating connection object. This library works as a wrapper of other DB-API 2.0 compliant DB drivers, thereby arguments given to `connect()` are passed through to `connect()` API of specified DB driver. Next code is an example using `psycopg2`.

```
import psycopg2
from pyracmon import connect

db = connect(psycopg2, dbname="example", user="postgres", password="postgres")
```

Returned object `db` is a wrapped `Connection` object which also conforms to DB-API 2.0's `Connection`.

### Declarations of model types

*Model types* get importable just by giving a `Connection` object to `declare_models()`. All tables and views are collected and declared as *Model types* named by their table or view names. The function takes at least 3 arguments listed below.

- A module imported from `pyracmon.dialect` package specifying the type of DBMS. `postgresql` and `mysql` are available currently.
- `Connection` object.
- An arbitrary module where *model types* are declared.

```
import models
from pyracmon import declare_models
from pyracmon.dialect import postgresql

declare_models(postgresql, db, models)
```

After that, *model types* can be imported from the specified module.

```
from models import blog, post, image, blog_category, post_comment
```

### Model methods

Each *model type* provides class methods executing typical CRUD operations on its corresponding table.

```
from pyracmon import Q

# Insert a record (you can omit the column where the value is assigned in database side).
# sql: INSERT INTO blog (title) VALUES (%s)
# parameters: "Blog title"
blog.insert(db, dict(title = "Blog title"))

# Update a record by primary key.
# sql: UPDATE blog SET title = %s WHERE id = %s
# parameters: "New title", 1
blog.update(db, 1, dict(title = "New title"))

# Delete a record by primary key.
# sql: DELETE FROM blog WHERE id = %s
# parameters: title", 1
blog.delete(db, 1)

# Update records by condition.
# sql: UPDATE blog SET title = %s, content = %s WHERE blog_id = %s
# parameters: "New title", "New content", 2
post.update_where(db, dict(title = "New title", content = "New content"), Q.eq(blog_id = 2))

# Delete records by condition
# sql: DELETE FROM blog WHERE blog_id = %s
# parameters: 2
post.delete_where(db, Q.eq(blog_id = 2))
```

Data fetching operations return *model object(s)* which expose column values via their attributes whose name indicates the column.

```
# Fetch a record by primary key.
# sql: SELECT id, blog_id, title, content FROM post WHERE id = %s
# parameters: 1
p = post.fetch(db, 1)
print(f"id = {p.id}, blog_id = {p.blog_id}, title = {p.title}, content = {p.content}")

# Fetch records by condition.
# sql: SELECT id, blog_id, title, content FROM post WHERE blog_id = %s ORDER BY title ASC limit %s OFFSET %s
# parameters: 2, 10, 20
m = db.helper.marker()
for p in post.fetch_where(db, Q.eq(blog_id = 2), orders = dict(title = True), limit = 10, offset = 20):
    print(f"id = {p.id}, blog_id = {p.blog_id}, title = {p.title}, content = {p.content}")

# Counting records. Returned value n is a number of records.
# sql: SELECT COUNT(*) FROM post WHERE blog_id = %s
# parameters: 2
n = post.count(db, Q.eq(blog_id = 2))
```

Other than usages in above examples, arguments for those methods have some variations, for example in `insert()`, *model object* is also available instead of dictionary. See API documentation for complete information.

### SQL operations

Instead of `Cursor` defined in DB-API 2.0, this library provides `Statement` for query execution. A functionality of `Statement` is the abstraction layer for placeholder markers. While DB-API 2.0 allows various styles, `Statement` accepts SQL where placeholders are marked by `$` prepended variables. By courtesy of `string.Template` module in python, those variables are converted to correct markers which DB driver can recognize.

Although there are several rules for the conversion, using `$_` for every placeholder and passing parameters in order makes sense in most cases.

```
from pyracmon import Q, where, read_row

# Fetch blogs and their posts by blog IDs.
blog_ids = ...
limit, offset = ...

# 1. Columns with aliases to select from each table.
# b.id, b.title, p.title, p.content
exp = blog.select("b"), post.select("p", ["title", "content"])

# 2. WHERE clause and parameters.
# WHERE b.id IN (?, ?, ...)
conds = Q.in_("b", id = blog_ids)
w, params = where(conds)

# 3. SQL execution
c = db.stmt().execute(f"""
    SELECT
        {exp}
    FROM
        blog AS b
        INNER JOIN post AS p ON b.id = p.blog_id
    {w}
    LIMIT $_ OFFSET $_
    """, *params, *[limit, offset])

# 4. Model objects obtained from each row.
for row in c.fetchall():
    r = read_row(row, *exp)
    blog = r.b
    post = r.p
```

Above code shows the basic flow of execution of SELECT query.

1. Creates `Expressions` object which contains columns and aliases of their tables to select.
    - As well as *models*, raw expressions like `COUNT(*)` are also available.
2. Creates `Conditional` object, and then obtatins conditional clause starting with `WHERE` and parameters used in it.
    - There are many functions to create `Conditional` object like `Q.in_()`.
3. Executs SQL on `Statement` object. Range condition and its parameters are added in both SQL and parameter list.
4. Obtains *model objects* from each row. `read_row()` parses a row and returns an object which exposes *model objects* via its attributes named by the alias given to `select()`.

See API documentation for further information.

### Graph declaration and construction

Previous section shows the way to get *models* by joining query. `Graph` is convenient object to return them with their relational structure.

Suppose you want a structured list of blogs like below.

```
{
    "blogs": [
        {
            "id": 1,
            "title": "Blog title",
            "posts": [
                {
                    "id": 1,
                    "title": "Post title",
                    "images": [
                        {
                            "url": "https://github.com/example/image/1",
                            "thumbnail": "https://github.com/example/image/1/thumbnail",
                        },
                        ...
                    ],
                    "comments": [
                        {
                            "id": 1,
                            "content": "The content of post comment",
                        },
                        ...
                    ],
                    "most_liked_comment": {
                        "id": 3,
                        "content": "The content of post"
                    },
                    "total_comments": 100
                }
            ],
            "total_posts": 100,
            "categories": [
                {
                    "id": 1,
                    "name": "Category name"
                },
                ...
            ]
        }
    ],
    "total": 100
}
```

Each blog entry contains various kinds of values which possibly should be obtained by multiple queries. First of all, you should declare `GraphTemplate` representing graph structure covering required values.

```
from pyracmon import graph_template

t = graph_template(
    blogs = blog,
    recent_posts = post,
    total_posts = int,
    categories = blog_category,
    images = image,
    recent_comments = post_comment,
    most_liked_comment = post_comment,
    total_comments = int,
    total = int,
)
t.blogs << [t.categories, t.total_posts, t.recent_posts]
t.recent_posts << [t.images, t.recent_comments, t.most_liked_comment, t.total_comments]
```

In each keyword argument, key denotes the kind of nodes and value denotes the type of node value; `blogs` specifies the container of nodes each of which contains `blog` *model object*. Relationships between nodes are declared by shift operators; category, total number of posts and recent post are children of each blog.

Next example shows the query execution and `Graph` creation which contains the result of the query (actual queries are not written to save spaces).

```
def fetch_blogs():
    # Create graph object.
    graph = new_graph(t)

    # Execute query to fetch blogs with their categories and total number of posts
    # In this query, blog and category are joined and the total number of posts are counted for each blog.
    exp = blog.select("b") + blog_category.select("c")

    c.execute("...")
    for row in c.fetchall():
        r = read_row(row, *exp, "posts")
        graph.append(
            blogs = r.b,
            categories = r.c,
            total_posts = r.posts,
        )

    # Execute query to fetch recent posts and their images from selected blogs above.
    # In this query, blog, post and image are joined and total number of comments are counted for each post.
    blog_ids = [b().id for b in graph.view.blogs]
    exp = post.select("p") + image.select("i")

    c.execute("...", blog_ids)
    for row in c.fetchall():
        r = read_row(row, *exp, "comments")
        graph.append(
            blogs = blog(id = r.p.blog_id),
            recent_posts = r.p,
            images = r.i,
            total_comments = r.comments,
        )

    # Execute query to fetch most liked comment for recent posts respectively.
    # In this query, post and post_comments are joined.
    post_ids = [p().id for p in graph.view.recent_posts]
    pc_ = post_comment.select("pc")
    c.execute("...")
    for row in c.fetchall():
        r = read_row(row, pc_, "liked", "recent")
        graph.append(
            recent_posts = post(id = r.pc.post_id),
            most_liked_comment = r.pc if r.liked else None,
            recent_comments = r.pc if r.recent else None,
        )

    # Count total number of blogs.
    graph.append(
        total = blog.count(db),
    )

    # Return view of the graph.
    return graph.view
```

Note that the flattened invocations of `append()` creates hierarchical relationships in a `Graph`. `append()` works as follows:

1. Values are sorted from parents to children.
2. For each value, if *identical* node is found, it is added to the node path of this invocation. Otherwise, new node is created and added to the node path.
3. Add edges between every adjacent nodes in the path.

A value is determined to be *identical* to a node when the node contains the *identical* value and its parent is the same as previous node in the node path. By default, the *identification* scheme is defined only on *model types*, that is, any pair of values of other types is never considered to be *identical*. Only the pair of *model objects* which have the same type and the same primary key value is *identical*.

As a result of these mechanisms, foreign key relationships in DB are recovered in the graph.

This function returns `view` attribute of the graph, which is unmodifiable view exposing intuitive interfaces to access nodes and their values.

### Graph serialization

The another feature of `Graph` is the serialization mechanism which converts `Graph` object into hierarchical `dict`. By default, serialization works as follows:

1. Starting from specified root node container, descending containers are handled from parents to children.
2. Each container is converted into a list of its nodes.
3. For each node, the preset function is applied to its value and the result is the actual value stored in the list.
    - *model object* is converted into a `dict` where the column name is mapped to the column value.
    - Values of other types are used as they are unless the function is set explicitly.
4. When converted value is a `dict`, serializes child nodes similarly and put them into it.

Some behaviors can be changed at serialization stage via class methods of `S` as show in next example.

```
from pyracmon import S, graph_dict

def add_thumbnail(s, v):
    r = s(v)
    r['thumbnail'] = f"{r['url']}/thumbnail"
    return r

result = graph_dict(
    fetch_blogs(),
    blogs = S.of(),
    recent_posts = S.name("posts"),
    total_posts = S.head(),
    categories = S.of(),
    images = S.each(add_thumbnail),
    recent_comments = S.name("comments"),
    most_liked_comment = S.head(),
    total_comments = S.head(),
    total = S.head(),
)
```

- `of()` does not affect default behavior.
- `name()` changes the key which by default is the name of node container.
- `head()` uses a value of the first node in the container instead of a list of values.
- When the function is given by `each()`, it is applied to each node to generate the value in resulting `dict`.

`S` provides some more class methods to control the serialization mechanism. Additionally, their arguments have variations. See API documentation for further information.

### Static typing

The schema of graph serialization can be obtained via `graph_schema()`. Its signature is similar to `graph_dict()` except for that the first argument is `GraphTemplate` instead of `GraphView`, which means the schema is generated statically.

```
from pyracmon import S, graph_schema

schema = graph_schema(
    t,
    blogs = S.of(),
    recent_posts = S.name("posts"),
    total_posts = S.head(),
    categories = S.of(),
    images = S.each(add_thumbnail),
    recent_comments = S.name("comments"),
    most_liked_comment = S.head(),
    total_comments = S.head(),
    total = S.head(),
)
```

Returned value is an instance of `GraphSchema` which exposes `schema` attribute. It is an instance of `TypedDict` which is similar implementation of `typing.TypedDict` introduced in python3.8. `TypedDict` represents a key-value data with types of values specified by type annotations. `walk_schema` returns the structure in the form of `dict`.

```
>>> from pyracmon.graph.schema import walk_schema
>>> 
>>> walk_schema(schema, True)
```

The second argument denotes whether the result contains a documentation of each item which is given by `S.doc()`. Note that each column of *model types* uses its comment as the document. DBMS such as PostgreSQL provides a way to set a comment on each column and it will be used in the documentation automatically. This feature shows the *DB first* principle.

You can use the schema as you like, for example, `swagger` is a good solution to describe it. The conversion into a commonly defined format is not the role of this library.

### Testing

TBD.