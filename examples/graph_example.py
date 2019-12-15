import psycopg2
from pyracmon import connect, declare_models, graph_template, new_graph, read_row, graph_dict
from pyracmon.graph import head
from pyracmon.dialect import postgresql

db = connect(
    psycopg2,
    host="postgres",
    port=5432,
    dbname="pyracmon_example",
    user="postgres",
    password="postgres"
)

import models
declare_models(postgresql, db, models)

from models import *

db.cursor().execute("TRUNCATE blog RESTART IDENTITY CASCADE")

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

    c = db.cursor()
    m = db.helper.marker()

    # Execute query to fetch blogs with their categories and total number of posts
    # In this query, blog and category are joined and the total number of posts are counted for each blog.
    b_, bc_ = blog.select("b"), blog_category.select("c")
    c.execute(f"""\
        SELECT
            {b_}, {bc_}, bp.posts
        FROM
            (
                SELECT
                    b.id, COUNT(p.id) AS posts
                FROM
                    blog AS b
                    INNER JOIN post AS p ON b.id = p.blog_id
                GROUP BY
                    b.id
                LIMIT {m()} OFFSET {m()}
            ) AS bp
            INNER JOIN blog AS b ON bp.id = b.id
            INNER JOIN blog_category AS c ON b.id = c.blog_id
        """, [10, 0])
    for row in c.fetchall():
        b, bc, ps = read_row(row, b_, bc_, ())
        graph.append(
            blogs = b,
            categories = bc,
            total_posts = ps,
        )

    # Execute query to fetch recent posts and their images from selected blogs above.
    # In this query, blog, post and image are joined and total number of comments are counted for each post.
    blog_ids = [b().id for b in graph.view.blogs]
    p_, i_ = post.select("p"), image.select("i")
    c.execute(f"""\
        SELECT
            {p_}, {i_}, q.comments
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
                    AND p.blog_id IN ({db.helper.holders(len(blog_ids))})
                GROUP BY
                    p.id
            ) AS q
            INNER JOIN post AS p ON q.id = p.id
            INNER JOIN image AS i ON q.id = i.post_id
        """, blog_ids)
    for row in c.fetchall():
        p, i, cs = read_row(row, p_, i_, ())
        graph.append(
            blogs = blog(id = p.blog_id),
            recent_posts = p,
            images = i,
            total_comments = cs,
        )

    # Execute query to fetch most liked comment for recent posts respectively.
    # In this query, post and post_comments are joined.
    post_ids = [p().id for p in graph.view.recent_posts]
    pc_ = post_comment.select("pc")
    c.execute(f"""\
        SELECT
            {pc_}
        FROM
            (
                SELECT
                    id, RANK() OVER (PARTITION BY post_id ORDER BY likes DESC) AS rank
                FROM
                    post_comment
                WHERE
                    post_id IN ({db.helper.holders(len(post_ids))})
            ) AS q
            INNER JOIN post_comment AS pc ON q.id = pc.id
        WHERE
            q.rank = 1
        """, post_ids)
    for row in c.fetchall():
        pc = read_row(row, pc_)[0]
        graph.append(
            recent_posts = post(id = pc.post_id),
            most_liked_comment = pc,
        )

    # Count total number of blogs.
    graph.append(
        total = blog.count(db),
    )

    # Return view of the graph.
    return graph.view

result = graph_dict(
    fetch_blogs(),
    blogs = (),
    recent_posts = (),
    total_posts = (None, head),
    categories = (),
    images = (),
    recent_comments = (),
    most_liked_comment = (None, head),
    total_comments = (None, head),
    total = (None, head),
)

import pprint
pprint.pprint(result)