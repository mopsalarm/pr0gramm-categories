import json
import os
import random
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import bottle
import datadog
import pcc
from psycopg2.extras import DictCursor

from cache import lru_cache_pool

CONFIG_POSTGRES_HOST = os.environ["POSTGRES_HOST"]

datadog.initialize()
stats = datadog.ThreadStats()
if datadog.api._api_key:
    stats.start()

print("open database at", CONFIG_POSTGRES_HOST)
dbpool = pcc.RefreshingConnectionCache(
        lifetime=60,
        host=CONFIG_POSTGRES_HOST,
        user="postgres", password="password", dbname="postgres",
        cursor_factory=DictCursor)


def metric_name(name):
    return "pr0gramm.categories." + name


def explode_flags(flags):
    if not 1 <= flags <= 7:
        raise ValueError("flags out of range")

    return tuple(flag for flag in (1, 2, 4) if flags & flag)


def fix_username_column(value, copy=True):
    if "username" in value and "user" not in value:
        value = value.copy()
        value["user"] = value.pop("username")

    return value


QUERY_RANDOM_NSFL = """
    SELECT *
    FROM items
    WHERE id IN (
      (
        SELECT id
        FROM random_items_nsfl TABLESAMPLE BERNOULLI(2)
        WHERE promoted!=0
        ORDER BY random() LIMIT 90)
      UNION (
        SELECT id
        FROM random_items_nsfl TABLESAMPLE BERNOULLI(1)
        WHERE promoted=0
        ORDER BY random() LIMIT 30));
"""

QUERY_RANDOM_REST = """
    (
      SELECT *
      FROM items TABLESAMPLE SYSTEM (0.5)
      WHERE flags IN %(flags)s AND promoted!=0
      ORDER BY random() LIMIT 90)
    UNION (
      SELECT *
      FROM items TABLESAMPLE SYSTEM (0.1)
      WHERE flags IN %(flags)s AND promoted=0
      ORDER BY random() LIMIT 30)
"""


def generate_item_feed_random(flags):
    query = QUERY_RANDOM_NSFL if flags == 4 else QUERY_RANDOM_REST
    with dbpool.tx() as database, database.cursor() as cursor:
        cursor.execute(query, {"flags": explode_flags(flags)})
        items = [fix_username_column(dict(row)) for row in cursor.fetchall()]

    random.shuffle(items)
    return items


def generate_item_feed_controversial(flags, older):
    clauses = ["items.flags IN (%s)" % ",".join(str(flag) for flag in explode_flags(flags))]

    if older and older > 0:
        clauses += ["items.id<%d" % older]

    query = """
        SELECT items.* FROM items
          JOIN controversial ON items.id=controversial.item_id
        WHERE %s AND items.id NOT IN (
          SELECT tags.item_id FROM tags WHERE tags.item_id=items.id AND tags.confidence>0.3 AND lower(tag)='repost')
        ORDER BY controversial.id DESC LIMIT 120""" % " AND ".join(clauses)

    with dbpool.tx() as database, database.cursor() as cursor:
        cursor.execute(query)
        items = [fix_username_column(item) for item in cursor]

    return len(items) < 120, items


def generate_item_feed_bestof(flags, older_than, min_score, tag, user):
    """
    SELECT items.* FROM items_bestof
    JOIN items ON items_bestof.id=items.id JOIN tags ON items_bestof.id=tags.item_id
    WHERE to_tsvector('simple', tags.tag) @@ to_tsquery('simple', 'hassen & diesen') AND items_bestof.score > 2000 AND items.flags IN (1, 2)
    LIMIT 120;
    """

    def join_as_bytes(*xs, sep=b" "):
        return sep.join((x if isinstance(x, bytes) else x.encode("utf8")) for x in xs)

    with dbpool.tx() as database, database.cursor() as cursor:
        # parts of the base query.
        joins = ["JOIN items ON items_bestof.id=items.id"]
        where_clauses = ["items_bestof.score > %d" % min_score]

        if tag:
            # add tags specific query parts
            joins += ["JOIN tags ON items_bestof.id=tags.item_id"]
            query = pg_tsquery(tag)
            where_clauses += [cursor.mogrify("to_tsvector('simple', tags.tag) @@ to_tsquery('simple', %s)", [query])]

        if flags != 7:
            # filter flags
            where_clauses += ["items.flags IN (%s)" % ",".join(map(str, explode_flags(flags)))]

        if user:
            where_clauses += [cursor.mogrify("lower(items.username)=%s", [user.lower()])]

        if older_than and older_than > 0:
            where_clauses += ["items_bestof.id<%d" % older_than]

        query = join_as_bytes("SELECT DISTINCT ON (items_bestof.id) items.* FROM items_bestof",
                              join_as_bytes(*joins),
                              "WHERE", join_as_bytes(*where_clauses, sep=b" AND "),
                              "ORDER BY items_bestof.id DESC LIMIT 120")

        cursor.execute(query)
        items = [fix_username_column(item) for item in cursor]

    return len(items) < 120, items


def pg_tsquery(tag):
    return " & ".join(re.split(r"\s+", tag))


thread_pool = ThreadPoolExecutor(4)


@lru_cache_pool(thread_pool, 8)
@stats.timed(metric_name("random.update"))
def process_request_random(flags):
    return json.dumps({
        "items": generate_item_feed_random(flags),
        "ts": time.time(),
        "atEnd": False, "atStart": True, "error": None, "cache": None, "rt": 1, "qc": 1
    })


@lru_cache_pool(thread_pool, 32)
@stats.timed(metric_name("controversial.update"))
def process_request_controversial(flags, older=None):
    at_end, items = generate_item_feed_controversial(flags, older)
    return json.dumps({
        "items": items,
        "ts": time.time(),
        "atEnd": at_end, "atStart": True, "error": None, "cache": None, "rt": 1, "qc": 1
    })


@stats.timed(metric_name("bestof.query"))
def process_request_bestof(flags, older_than, min_score, tag, user):
    at_end, items = generate_item_feed_bestof(flags, older_than, min_score, tag, user)
    return json.dumps({
        "items": items,
        "ts": time.time(),
        "atEnd": at_end, "atStart": True, "error": None, "cache": None, "rt": 1, "qc": 1
    })


@bottle.get("/random")
@stats.timed(metric_name("random.request"))
def feed_random_cached():
    params = bottle.request.params.decode("utf8")
    flags = int(params.get("flags", "1"))

    bottle.response.content_type = "application/json"
    return process_request_random(flags)


@bottle.get("/controversial")
@stats.timed(metric_name("controversial.request"))
def feed_controversial_cached():
    flags = int(bottle.request.params.get("flags", "1"))
    older_than = int(bottle.request.params.get("older", "0"))

    bottle.response.content_type = "application/json"
    return process_request_controversial(flags, older_than)


@bottle.get("/bestof")
@stats.timed(metric_name("bestof.request"))
def feed_bestof_cached():
    params = bottle.request.params.decode("utf8")

    flags = int(params.get("flags", "1"))
    older_than = int(params.get("older", "0"))
    min_score = int(params.get("score", "1000"))
    tag = params.get("tags")

    # user = params.get("user")
    user = None

    bottle.response.content_type = "application/json"
    return process_request_bestof(flags, older_than, min_score, tag, user)


@bottle.route("/ping")
def ping():
    return


def update_random_nsfl_view():
    def update_once():
        print("Updating random_items_nsfl view now.")
        with dbpool.tx() as database, database.cursor() as cursor:
            cursor.execute("""
                -- create view with only nsfl posts
                CREATE MATERIALIZED VIEW IF NOT EXISTS random_items_nsfl (id, promoted) AS (
                  SELECT id, promoted FROM items WHERE flags=4);

                -- to use "refresh view concurrently", we need a unique index on the id column
                CREATE UNIQUE INDEX IF NOT EXISTS postgres_random_nsfl__id ON random_items_nsfl(id);

                -- refresh the nsfl items.
                REFRESH MATERIALIZED VIEW CONCURRENTLY random_items_nsfl;
            """)

            cursor.execute("SELECT COUNT(id) FROM random_items_nsfl")
            nsfl_count = cursor.fetchone()[0]

        print("We have {} nsfl items in the database".format(nsfl_count))

    while True:
        try:
            update_once()
        except KeyboardInterrupt:
            raise
        except:
            traceback.print_exc()

        time.sleep(6 * 3600)

import threading
threading.Thread(target=update_random_nsfl_view, daemon=True).start()
