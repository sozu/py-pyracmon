# Python O/R mapping extension for DB-API 2.0

## Overview

This library provides functionalities which refine codes executing DB operations. The main peculiar concepts are *model declaration* and *relational graph*.

*Model* is the class representing a table and its instance corresponds to a record. This library provides a way to declare model classes by reverse-engineering from actual tables. Once you connect DB, model classes are importable from a module you specified. Those classes have various methods executing simple operations on a table.

*Graph* is composed of *node*s and *edge*s, where each node contains a model object or any kind of value. An edge between nodes represents the relation between their values, such as relation between records. *Graph*s are designed to accept rows obtained from DB with straight-forward codes, which makes you free from suffering from the reconstruction of data structure for query result. Additionally, *graph view* provides intuitive interfaces to traverse it and *graph serialization* feature enables conversion from a *graph* into a `dict` with keeping its structure and applying flexible representation to each node. As a result, you no longer need to care how the result of DB operation is used in other modules.

On the other hand, this library does NOT support following features which are common in libraries called O/R mapper.

- Query generation which resolves relations (foreign keys) automatically.
- Object based query operation such as lazy loading or dirty state handling.
- Code first migrations, that is, database migrations based on entity declarations.

## Features

**DB-API 2.0**

This library works as a wrapper of any kind of DB driver compliant with DB-API 2.0 such as *psycopg2* or *PyMySQL*. DB operations are passed to the driver so that any functionality it provides is also available. The role of this library is providing flexible, intuitive and sophisticated way to write codes executing the operations.

**Automatic declaration of model types**

Similarly to many other O/R mappers, this library represents a table with a class called *model type*. Though, it exports a function `declare_models()` which collects tables and views from connected databasse and declares model types of them automatically in specified module. Because the declaration is done at runtime, changes by DB schema migrations are reflected without any manual operation.

**Query helpers**

While this library mainly focuses on the use of SQL, not DSL, it exports many functions to generate a part of query and each *model type* has methods to execute some routine DB operations on its representing table. Both will decrease boilerplate codes to construct query string and its parameters. Meanwhile, complicated queries which contain table join, sub-query and so on are not suppoted intentionaly. Trials done by many projects to represent those queries by object oriented interfaces or DSLs have ended up to bring larger difficulties, that is, SQL is preferable than the interfaces or DSLs.

**Record graph**

`Graph` is a type representing tree structure where each node has at most a parent node. It is designed to store relations between objects, especially *model objects*, without any extension on *model types*. This feature helps us avoiding to declare special classes or properties for each query result. Another functionality of `Graph` is a declarative serialization into a dictionary. Which and how value should be serialied is determined at serialization stage and thereby decoupling database operations from the representastions of the results becomes much easier.

**Static typing support**

Experimental, TBD.

**Testing support**

Experimental, TBD.

## Prerequisite

- Python >= 3.6
- Supporting DBMS
    - MySQL >= 8.0
    - PostgreSQL >= 10

## Installation

```
$ pip install pyracmon==1.0.dev2
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
- An arbitrary module, where *model types* are declared.

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

Data fetching operations return *model object(s)* which expose column values via their attributes. Each column name in database is used for attribute name as it is.

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

Other than usages in above example, arguments for those methods have some variations, for example in `insert()`, *model object* is also available instead of dictionary. See API documentation for complete information.

### SQL operations

Instead of `Cursor` defined in DB-API 2.0, this library provides `Statement` on which SQL in executed. A functionality of `Statement` is the abstraction layer for placeholder markers. While DB-API 2.0 allows various styles, `Statement` accepts SQL where placeholders are marked by `$` prepended variables. By courtesy of `string.Template` module in python, those variables are converted to correct markers which DB driver can recognize.

Although there are several rules for the conversion, using `$_` as every variable and passing parameters in order makes sense in most cases.

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

Above code shows the basic flow of selecting operation by SQL.

1. Creates `Expressions` object contains columns and aliases of their tables to select.
    - As well as *models*, raw expressions are also available.
2. Creates `Conditional` object, and then obtatins conditional clause starting with `WHERE` and parameters used in it.
    - There are many functions to create `Conditional` object like `Q.in_()`.
3. Executs SQL on `Statement` object. Range condition and its parameters are added in both SQL and parameter list.
4. Obtains *model objects* from each row. `read_row()` parses a row and returns an object which exposes *model objects* via its attributes named by each alias.

See API documentation for further information.

### Graph declaration and construction

Former sections show the way to obtain record values as *model objects*, on the other hand, the way to deal with them is not considered, that is, how to return the to the caller and how to represent them to the application user. `Graph` is introduced to take that role. This section shows the way to gather obtained *model objects* and any other values into a `Graph` with keeping their relationships.

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

Each keyword argument corresponds to a node container which stored values of specified type as nodes respectively. Relationships between nodes are declared by shift operators; category, total number of posts and recent post are children of each blog.

The function in the next code executes queries (actual queries are not written to save spaces) and stores their results in a `Graph` object bound to the `GraphTemplate`.

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

Values are stored correctly just by repeating invocation of `append()` due to predefined relationships. In short, `append()` works as follows:

1. Sorts given values in descending order according to their relationships.
2. For the top value, searches nodes whose values are *identical* to it. if nodes are found, keeps them as parent nodes for succeeding values, otherwise, adds new node and keep it.
3. For succeeding values, searches *identical* nodes under parent nodes. Adds new node only under parents having no *identical* node. Found *identical* nodes and added nodes are kept as parent nodes for succeeding values.

While the *identification* of nodes is configurable, only *model objects* having the same primary key are considered to be *identical* by default. This is the reason why a *model object* to which only primary key is assigned is used in the invocation of `append()`.

This function returns `view` attribute of the `Graph`, which is unmodifiable view exposing intuitive interfaces to access nodes and their values.

### Graph serialization

The another feature of `Graph` is the serialization mechanism which converts `Graph` object into hierarchical dictionary. By default, serialization works as follows:

1. Starting from root node containers, puts them to the dictionary by setting their names as keys.
2. Converts Each container to a list containing its nodes.
3. Converts the value of each node by preset function bound to its type.
    - *model object* is converted to a dictionary holding column names and their values.
    - Values of other types are used as they are.
4. When converted value is a dictionary, serializes child nodes similarly and put them into it.

These are default behaviors which can changed at serialization stage. For example:

- Keys in dictionary can be changed.
- Each node container can be aggregated into a single value (not list).
- Node conversion function can be supplied to override or inhert preset one.
- Parent node can merge key value pairs in child dictionary.

Next is an example to serialize a `Graph` into a dicionary in the form shown above.

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

`S` is an utility class providing various class methods which controls serialization. See API documentation for further information.

### Static typing

TBD.

### Testing

TBD.