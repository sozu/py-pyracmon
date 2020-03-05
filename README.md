# Python O/R mapping extension for DB-API 2.0

## Overview

This library provides functionalities which refine codes executing DB operations. The main peculiar concepts are *model declaration* and *relational graph*.

*Model* is the class representing a table and its instance corresponds to a record. This library provides a way to declare model classes by reverse-engineering from actual tables. Once you connect DB, model classes are importable from a module you specified. Those classes have various methods executing simple operations on a table.

*Graph* is composed of *node*s and *edge*s, where each node contains a model object or any kind of value. An edge between nodes represents the relation between their values, such as relation between records. *Graph*s are designed to accept rows obtained from DB with straight-forward codes, which makes you free from suffering from the reconstruction of data structure for query result. Additionally, *graph view* provides intuitive interfaces to traverse it and *graph serialization* feature enables conversion from a *graph* into a `dict` with keeping its structure and applying flexible representation to each node. As a result, you no longer need to care how the result of DB operation is used in other modules.

On the other hand, this library does NOT support following features which are common in libraries called O/R mapper.

- Query generation which resolves relations (foreign keys) automatically.
- Object based query operation such as lazy loading or dirty state handling.
- Code first migrations, that is, database migrations based on entity declarations.

## Prerequisite

- Python >= 3.6
- Supporting DBMS
    - MySQL >= 8.0
    - PostgreSQL >= 10

## Installation

```
$ pip install pyracmon
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

### Model declaration

Model classes get importable just by giving a `Connection` object to `declare_models()`. Any kind of DB adapter module conforming to DB-API 2.0 specifications is available to obtain the `Connection` object. What you have to do is invoking `connect()` imported from `pyracmon` package. This function takes the module in the first argument, and other arguments are passed to `connect()` function defined in the module. Next code shows the case where `psycopg2` is used.

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

Model classes are declared into the module specified in third argument of `declare_models()` (`models` in above example). After that, they get available by importing from the module.

> Currently, the name of model class is the same as the table name. This causes the restriction that the table name must be a valid identifier of python.

```
from models import *

assert blog.name == "blog"
assert [c.name for c in post.columns] == ["id", "blog_id", "title", "content"]
```

### Model based operations

Model class provides static methods executing typical CRUD operations on its corresponding table.

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
post.update_where(
    db,
    dict(title = "New title", content = "New content"),
    lambda m: Q.of(f"blog_id = {m()}", 2),
)

# Delete records by condition
# sql: DELETE FROM blog WHERE blog_id = %s
# parameters: 2
post.delete_where(
    db,
    lambda m: Q.of(f"blog_id = {m()}", 2),
)
```

Data fetching operations return model instance(s). Model instance has attributes each of which holds a value of the column of the same name.

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
for p in post.fetch_where(
    db,
    lambda m: Q.condition(f"blog_id = {m()}", 2),
    orders = dict(title = True),
    limit = 10,
    offset = 20,
):
    print(f"id = {p.id}, blog_id = {p.blog_id}, title = {p.title}, content = {p.content}")

# Counting records. Returned value n is a number of records.
# sql: SELECT COUNT(*) FROM post WHERE blog_id = %s
# parameters: 2
n = post.count(db, lambda m: Q.condition(f"blog_id = {m()}", 2))
```

Those methods accept some other optional arguments to control executing queries. See API documentation for further information.

### SQL operations

The `Connection` object of this library is just a proxy to the connection object which conforms to DB-API 2.0, therefore, it also conforms to DB-API 2.0. You can post any query via `Cursor` obtained by `cursor()`. `Connection` has an additional attribute named `helper` which provides some helper functions to construct SQL as follows.

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

Invoking marker object `m` returns a place holder string determined by `paramstyle` attribute of DB-API 2.0 module, in this case, `%s`. `select()` returns `Selection` object which determines columns to select and their orders. `Selection` objects are also used in `read_row()` to parse obtained rows into model objects.

This is the basic flow of selecting operation. See API documentation for further information.

### Create relational graph

This is often the case that the actual application requires not only records but also additional informations such as:

- Count of grouping records.
- Total number of existing records.
- Maximum/Minimum value of a column in each group.
- Calculation or transformation to column value.
- etc...

Those values are sometimes obtained by query and sometimes by programs. Despite how they are obtained, this kind of additional informations tend to be a cause of problems like fat model. Also, we often have to merge results of multiple queries according to their relationships, which can make codes complicated.

Suppose you need a structured list of blogs like below.

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

`Graph` is a data structure where nodes are connected by edges according to predefined hierarchical relationships in `GraphTemplate`. Therefore, you can separate the definition of data structure (`GraphTemplate`) from data handling operations.

In addition, `Graph` is designed to accept querying results in straight-forward manner in order to keep codes simple.

Next code is the example to construct similar to above structured list by using `Graph`.

> Some keys intentionally are set to different keys from above example. They are modified in next chapter.

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

You might be able to guess what this code does without description and it would be correct. `graph_template()` declares a sturcture of a `Graph`. Each item of keyword arguments correspond to a name of node list and a type of entity stored in the node (Actually, you can supply additional arguments to control the graph behavior). Bit-shift operators in following lines declare parent-child relationships between nodes, that is, edges of the graph.

`new_graph()` creates an empty `Graph` instance which works according to the template structure. `append()` is a method which inserts values into the nodes specified by each key of keyword arguments respectively, and then, creates edges between related nodes. In order to construct correct relationships, the method first searches a node whose entity is *identical* to each inserting value and, if exists, it does not insert the value and creates an edge toward found node.

The key of this behavior is *identicalness* of node entity. By default, only model objects fulfilling following conditions are *identical*:

- Corresponding table has primary key(s).
- Every primary key is set.
- Every primary key value is equal. Equality is obtained by `==` operator.

You can add or override identifying methods in some ways. Start from documentation of `GraphSpec` to know the detail.

This example returns `view` attribute of the graph. This is the unmodifiable expressios of the graph which provides intuitive interfaces to access nodes according to the graph structure.

### Serialize graph

The another feature of `Graph` is the serialization mechanism. For example in typical HTTP applications, obtained values should be serialized in json strings. For that purpose, this library provides the mechanism to convert `Graph` into a hierarchical `dict`. It enables you to separate codes of data handling and response rendering.

`graph_dict()` is the function to do the conversion. It takes a view of graph and optional keyword arguments where each key denotes a property name and the value is `NodeSerializer` object or its equivalent `tuple`.

```
from pyracmon import S

def add_thumbnail(s, v):
    r = s(v)
    r['thumbnail'] = f"{r['url']}/thumbnail"
    return r

result = graph_dict(
    fetch_blogs(),
    blogs = (),
    recent_posts = ("posts",),
    total_posts = S.head(),
    categories = (),
    images = S.each(add_thumbnail),
    recent_comments = ("comments",),
    most_liked_comment = S.head(),
    total_comments = S.head(),
    total = S.head(),
)
```

`S` is an utility class to create `NodeSerializer` instance via builder methods. The behavior of `NodeSerializer` consists of 3 phases below.

- Naming phase which determines a key of the property.
    - By default, property name is used as the key.
    - Given strings (`"posts"` or `"comments"` in the example) are used instead of original property names (`recent_posts` or `recent_comments`).
- Aggregation phase which aggregates nodes of the property into a node.
    - By default, every property is represented with a `list` of serialized nodes.
    - According to the aggregation function, the property is represented with a value obtained from a selected node.
    - In the exmaple, `head()` set the aggregation function which select the first node (or `None` if no nodes exist).
- Serialization phase which converts a node entity into a serializable value. 
    - By default, model object is converted into a `dict` representing column name and value. Any other types are passed without conversion.
    - Serialization function can be set by `each()` method.

Although there remains various options and rules, this is the fundamental mechanism of graph serialization. 
