# Python O/R mapping extension for DB-API 2.0

## Overview

This library provides functionalities to refine programs handling DB operations. The main peculiar concepts are *model declaration* and *relational graph*.

*Model* is a class representing a table and its instance corresponds to a record. This library provides a way to declare the classes by reverse-engineering from actual tables. Once you connect DB, model classes get importable from a module you specified. Also, the class has various methods executing simple operations on a table.

*Graph* is composed of *node*s and *edge*s where each node contains a record or any kind of value and each edge between nodes represents the relation between their records. Because they are designed to accept rows obtained from DB with straight-forward codes and provide flexible way to represent them, you will be free from suffering from the data structure of query result.

In addition, various utilties to construct SQL are available.

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

## Grance of functionalities

### Assumption

Suppose, you have a database including tables as follows (written in PostgreSQL syntax).

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

### Model declaration

Model classes gets importable by just giving a connection to `declare_models()`. You need database adapter module conforming to DB-API 2.0 because `Connection` this library provides is just a proxy of actual `Connection` opened by the adapter. In this case, `psycopg2` is used.

```
import psycopg2
from pyracmon import connect, declare_models
from pyracmon.dialect import postgresql

# The first argument is a DB-API 2.0 module.
# Following arguments and keyword arguments are passed though to psycopg2.connect()
db = connect(psycopg2, dbname="example", user="postgres", password="postgres")

import models
declare_models(postgresql, db, models)
```

You can choose any package (`models` in above example) where model classes are declared. After `declare_models()`, model classes are available by importing from the package.

> Currently, the name of model class is the same as the table name. This causes the restriction that the table name must be a valid identifier of python.

```
from models import *

assert blog.name == "blog"
assert [c.name for c in post.columns] == ["id", "blog_id", "title", "content"]
```

### Model based operations

Model class provides static methods executing typical data operations for its corresponding table.

```
from pyracmon.query import Q, order_by

# Insert a record (you can omit the column where the value is assigned in database side).
# sql: INSERT INTO blog (title) VALUES (?)
# parameters: "Blog title"
blog.insert(db, dict(title = "Blog title"))

# Update a record by primary key.
# sql: UPDATE blog SET title = ? WHERE id = ?
# parameters: "New title", 1
blog.update(db, 1, dict(title = "New title"))

# Delete a record by primary key.
# sql: DELETE FROM blog WHERE id = ?
# parameters: title", 1
blog.delete(db, 1)

# Update records by condition.
# sql: UPDATE blog SET title = ?, content = ? WHERE blog_id = ?
# parameters: "New title", "New content", 2
post.update_where(
    db,
    dict(title = "New title", content = "New content"),
    lambda m: Q.condition(f"blog_id = {m()}", 2),
)

# Delete records by condition
# sql: DELETE FROM blog WHERE blog_id = ?
# parameters: 2
post.delete_where(
    db,
    lambda m: Q.condition(f"blog_id = {m()}", 2),
)
```

Data fetching operations return model instance where each attribute holds a value of the column of the same name.

```
# Fetch a record by primary key.
# sql: SELECT id, blog_id, title, content FROM post WHERE id = ?
# parameters: 1
p = post.fetch(db, 1)
print(f"id = {p.id}, blog_id = {p.blog_id}, title = {p.title}, content = {p.content}")

# Fetch records by condition.
# sql: SELECT id, blog_id, title, content FROM post WHERE blog_id = ? ORDER BY title ASC limit ? OFFSET ?
# parameters: 2, 10, 20
m = db.helper.marker()
for p in post.fetch_where(
    db,
    lambda m: Q.condition(f"blog_id = {m()}", 2),
    order_by(dict(title: True)), 10, 20,
):
    print(f"id = {p.id}, blog_id = {p.blog_id}, title = {p.title}, content = {p.content}")

# Counting records. Returned value n is a number of records.
# sql: SELECT COUNT(*) FROM post WHERE blog_id = ?
# parameters: 2
n = post.count(db, lambda m: Q.condition(f"blog_id = {m()}", 2))
```

Those methods accept other optional arguments to modify executing queries (ex. applying additional expression around place holder marker) and allow variations of arguments types (ex. `dict` for multiple primary keys).

### SQL operations

The `Connection` object of this library is just a proxy to the `Connection` which conforms to DB-API 2.0, therefore, it also conforms to DB-API 2.0. You can post any SQL via `Cursor` obtained by `cursor()`. `helper` attribute of the connection has some helper functions to construct SQL as follows.

```
# Fetch blogs and their posts by blog IDs.
blog_ids = ...
limit, offset = ...

# Marker of place holder.
m = db.helper.marker()

# Columns with aliases to select from each table.
# -> b.id, b.title, p.title, p.content
b_, p_ = blog.select("b"), post.select("p", ["title", "content"])

c = db.cursor()
c.execute(f"""
    SELECT
        {b_}, {p_}
    FROM
        blog AS b
        INNER JOIN post AS p ON b.id = p.blog_id
    WHERE
        b.id IN ({db.helper.holders(len(blog_ids))})
    LIMIT {m()} OFFSET {m()}
    """, blogs_ids + [limit, offset])

# The pairs of blog and post models: [[blog, post]]
blog_post_pairs = [read_row(row, b_, p_) for row in c.fetchall()]
```

Each invocation of marker object `m` returns a string correponding to parameter, for psycopg2, `%s`. `select()` method of the model determines columns to select and their order. Using objects returned by `select()`s in SQL and arguments of `read_row()` in the same order enables correct constructions of model instances. `holders()` is one of helper method defined in `QueryHelper` which generates given numbers of markers concatenated with comma. This kind of methods are available via `QueryHelper`.

`read_row()` returns a list of models in the same order as the optional arguments, in this case, `b_` and `p_`.

### Crete relational graph

In many cases, actual application requires not only records but also additional informations such as grouping count, and we often have to merge results of multiple queries according to their relationships. `Graph` is a data structure which stores them by predefined hierarchical (tree-like) structure as `GraphTemplate` and exports a view to scan its nodes in natural syntax in python.

Suppose you need complicated structured list of blogs like below.

```
{
    "blogs": [
        {
            "id": 1,
            "title": "Blog title",
            "recent_posts": [
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
                    "recent_comments": [
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

This kind of complexity often falls into undesirable code such as fat model or a lot of lazy loadings. Although you can't avoid executing multiple queries and constructing complex SQL to obtain records correctly and efficiently, `Graph` helps your code being straight-forward and free from boilerplates. Next code shows the usage of `Graph` for this data structure.

```
from pyracmon import graph_template, new_graph

# Declare template representing graph structure.
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

def fetch_blogs():
    # Create graph instance.
    graph = new_graph(t)

    # Execute query to fetch blogs with their categories and total number of posts
    # In this query, blog and category are joined and the total number of posts are counted for each blog.
    rs = ...
    for row in rs:
        b, c, ps = read_row(...)
        graph.append(
            blogs = b,
            categories = c,
            total_posts = ps,
        )

    # Execute query to fetch recent posts and their images from selected blogs above.
    # In this query, blog, post and image are joined and total number of comments are counted for each post.
    rs = ...
    for row in rs:
        b, p, i, cs = read_row(...)
        graph.append(
            blogs = b,
            recent_posts = p,
            images = i,
            total_comments = cs,
        )

    # Execute query to fetch most liked comment for recent posts respectively.
    # In this query, post and post_comments are joined.
    rs = ...
    for row in rs:
        p, pc = read_row(...)
        graph.append(
            recent_posts = p,
            most_liked_comment = pc,
        )

    # Count total number of blogs.
    graph.append(
        total = blog.count(db),
    )

    # Return view of the graph.
    return graph.view
```

You might be able to guess what this code does without description and it would be true. `graph_template()` declares a sturcture of a `Graph`. Each key of argument dictionary corresponds to a node of graph. The value of the dictionary can be given in various styles. This exmaple shows most simple style, just a type of node entity. Shift operators in following lines declare parent-child relationships, in other words, edges of the graph. Then, `Graph` instance with the structure is generated by `new_graph()`.

`Graph.append()` is a method to append data into the `Graph`. This method takes dictionary as an argument and stores each value as a node distinguished by its key. Edges between nodes are also created simultaneously. Actually in this phase, if the identical model has been stored already, node is not newly appended and the edge is connected to the existing one. The identification is based on the value of primary key(s) of the model type by default, which is the reason you should specify the type of node entity.

`view` attribute is the view of the graph which provides natural interfaces accessing values stored values and keep the graph immutable. This object is also designed for serialization described below, therefore, it should be a good practice to always return the view after constructing graph completely.