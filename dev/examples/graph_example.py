import psycopg2
from pyracmon.dialect import postgresql
from pyracmon import *

"""
Before running this example, create stub files for the models via the following command:

```
uv run python -m pyracmon.stub \
  psycopg2 \
  postgresql://postgres:postgres@postgres:5432/pyracmon_example \
  postgresql \
  examples.models
```
"""

#----------------------------------------------------------------
# Connect to the database.
#----------------------------------------------------------------
db = connect(
    psycopg2,
    host="postgres",
    port=5432,
    dbname="pyracmon_example",
    user="postgres",
    password="postgres"
)

#----------------------------------------------------------------
# Declare model types at runtime.
#----------------------------------------------------------------
from examples import models
ms = declare_models(postgresql, db, models)

from examples.models import *

#----------------------------------------------------------------
# Insert sample data.
#----------------------------------------------------------------
blog.truncate(db)

blog.inserts(db, [dict(title = f"title_{i}") for i in range(0, 20)])
for i in range(0, 20):
    post.inserts(db, [dict(
        blog_id = i + 1,
        title = f"post_title_{i}_{j}",
        content = f"post_content_{i}_{j}",
    ) for j in range(0, 10)])

    for j in range(0, 10):
        image.inserts(db, [dict(
            post_id = i * 10 + j + 1,
            url = f"https://github.com/py-pyracmon/image_{i}_{j}_{k}",
        ) for k in range(0, 3)])

        post_comment.inserts(db, [dict(
            post_id = i * 10 + j + 1,
            likes = 43 % (k + 5),
            content = f"https://github.com/py-pyracmon/image_{i}_{j}_{k}",
        ) for k in range(0, 5)])

for i in range(0, 20):
    blog_category.inserts(db, [dict(
        blog_id = i + 1,
        name = f"category_{i}_{j}",
    ) for j in range(0, 3)])

#----------------------------------------------------------------
# Declare graph.
#----------------------------------------------------------------
class Post(TNode[post]):
    images: TEdge[image]
    recent_comments: TEdge[post_comment]
    most_liked_comment: post_comment | None
    total_comments: int

class Blog(TNode[blog]):
    categories: TEdge[blog_category]
    total_posts: int
    recent_posts: TEdge[Post]

class BlogGraph(TGraph):
    blogs: TEdge[Blog]
    total: int

#----------------------------------------------------------------
# Create graph by executing queries.
#----------------------------------------------------------------
graph = new_typed_graph(BlogGraph)

# Execute query to fetch blogs with their categories and total number of posts
# In this query, blog and category are joined and the total number of posts are counted for each blog.
exp = blog.select("b") + blog_category.select("c") + "posts"

with cursor(db.stmt().execute(f"""\
    SELECT
        {exp.b}, {exp.c}, bp.posts
    FROM
        (
            SELECT
                b.id, COUNT(p.id) AS posts
            FROM
                blog AS b
                INNER JOIN post AS p ON b.id = p.blog_id
            GROUP BY
                b.id
            LIMIT $_ OFFSET $_
        ) AS bp
        INNER JOIN blog AS b ON bp.id = b.id
        INNER JOIN blog_category AS c ON b.id = c.blog_id
    """, 10, 0)) as c:
    append_rows(
        c, exp, graph,
        blogs=exp.b,
        categories=exp.c,
        total_posts=exp.posts,
    )

# Execute query to fetch recent posts and their images from selected blogs above.
# In this query, blog, post and image are joined and total number of comments are counted for each post.
blog_ids = [b().id for b in graph.view.blogs]
exp = post.select("p") + image.select("i") + "comments"

with cursor(db.stmt().execute(f"""\
    SELECT
        {exp.p}, {exp.i}, q.comments
    FROM
        (
            SELECT
                p.id, COUNT(pc.id) as comments
            FROM
                (
                    SELECT
                        id, blog_id, RANK() OVER (PARTITION BY blog_id ORDER BY id DESC) AS rank
                    FROM
                        post
                ) AS p
                INNER JOIN post_comment AS pc ON p.id = pc.post_id
            WHERE
                p.rank <= 3
                AND p.blog_id IN ({holders(len(blog_ids))})
            GROUP BY
                p.id
        ) AS q
        INNER JOIN post AS p ON q.id = p.id
        INNER JOIN image AS i ON q.id = i.post_id
    """, *blog_ids)) as c:
    append_rows(
        c, exp, graph,
        blogs=lambda r: blog(id=r.p.blog_id),
        recent_posts=exp.p,
        images=exp.i,
        total_comments=exp.comments,
    )

# Execute query to fetch recent comments and most liked comment for recent posts respectively.
# In this query, post and post_comments are joined.
post_ids = [p().id for b in graph.view.blogs for p in b.recent_posts]
exp = post_comment.select("pc") + "liked" + "recent"

with cursor(db.stmt().execute(f"""\
    SELECT
        {exp.pc}, q.liked = 1, q.recent <= 3
    FROM
        (
            SELECT
                id,
                RANK() OVER (PARTITION BY post_id ORDER BY likes DESC) AS liked,
                RANK() OVER (PARTITION BY post_id ORDER BY id DESC) AS recent
            FROM
                post_comment
            WHERE
                post_id IN ({holders(len(post_ids))})
        ) AS q
        INNER JOIN post_comment AS pc ON q.id = pc.id
    WHERE
        q.liked = 1 OR q.recent <= 3
    """, *post_ids)) as c:
    append_rows(
        c, exp, graph,
        recent_posts=lambda r: post(id=r.pc.post_id),
        most_liked_comment=lambda r: r.pc if r.liked else None,
        recent_comments=lambda r: r.pc if r.recent else None,
    )

# Count total number of blogs.
graph.append(total = blog.count(db))

#----------------------------------------------------------------
# Serialize graph into a dictionary.
#----------------------------------------------------------------
def add_thumbnail(cxt: NodeContext):
    r = cxt.serialize()
    r['thumbnail'] = f"{r['url']}/thumbnail"
    return r

result = graph_schema(
    BlogGraph.template(),
    blogs=S.of(),
    recent_posts=S.name("posts"),
    total_posts=S.head(),
    categories=S.of(),
    images=S.each(add_thumbnail),
    recent_comments=S.name("comments"),
    most_liked_comment=S.head(),
    total_comments=S.head(),
    total=S.head(),
).serialize(graph.view)

import pprint
pprint.pprint(result)