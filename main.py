from concurrent.futures import ThreadPoolExecutor
import itertools
import json
from operator import itemgetter
import os
import random
import threading
import time
import traceback

import bottle
import datadog
from first import first
import psycopg2
from psycopg2.extras import DictCursor

from cache import lru_cache_pool
from controversial import update_controversial

CONFIG_POSTGRES_HOST = os.environ["POSTGRES_HOST"]

datadog.initialize()
stats = datadog.ThreadStats()
stats.start()

print("open database at", CONFIG_POSTGRES_HOST)
database = psycopg2.connect(host=CONFIG_POSTGRES_HOST,
                            user="postgres", password="password", dbname="postgres",
                            cursor_factory=DictCursor)


def metric_name(name):
    return "pr0gramm.categories." + name


def explode_flags(flags):
    if not 1 <= flags <= 7:
        raise ValueError("flags out of range")

    return [flag for flag in (1, 2, 4) if flags & flag]


def fix_username_column(value):
    if "username" in value and "user" not in value:
        value = value.copy()
        value["user"] = value.pop("username")

    return value


def query_random_items(cursor, flags, max_id, tags, promoted, count):
    q_flags = ",".join(str(flag) for flag in explode_flags(flags))
    q_promoted = "!=" if promoted else "="

    def query_with_tag(item_id):
        cursor.execute("""SELECT items.*
            FROM items INNER JOIN tags ON items.id=tags.item_id
            WHERE items.id<=%%s AND tags.tag=%%s AND flags IN (%s) AND promoted %s 0
            ORDER BY items.id DESC LIMIT 1""" % (q_flags, q_promoted), (item_id, tags))

    def query_simple(item_id):
        cursor.execute("""SELECT * FROM items
            WHERE id<=%%s AND flags IN (%s) AND promoted %s 0
            ORDER BY id DESC LIMIT 1""" % (q_flags, q_promoted), (item_id,))

    random_item_ids = sorted({random.randint(0, max_id) for _ in range(count)}, reverse=True)

    min_possible_result = max(random_item_ids)
    for rid in random_item_ids:
        if rid > min_possible_result:
            continue

        # now perform the query
        (query_with_tag if tags else query_simple)(rid)
        items = [dict(item) for item in cursor]

        # stop if we haven't found anything with a smaller id than this
        if not items:
            break

        # yield the results!
        for item in items:
            min_possible_result = item["id"]
            yield fix_username_column(item)


def unique(items, key_function=id):
    keys = set()
    for item in items:
        key = key_function(item)
        if key not in keys:
            yield item
            keys.add(key)


def generate_item_feed_random(flags, tags):
    with database, database.cursor() as cursor:
        cursor.execute("SELECT MAX(id) FROM items")
        max_id, = cursor.fetchone()
        items = itertools.chain(
            query_random_items(cursor, flags, max_id, tags, promoted=True, count=90),
            query_random_items(cursor, flags, max_id, tags, promoted=False, count=30))

        items = list(unique(items, itemgetter("id")))

    random.shuffle(items)
    return items


def start_update_controversial_thread():
    update_item_count = None

    def update():
        nonlocal update_item_count
        print("Updating controversial category now")
        with database, database.cursor() as cursor:
            with stats.timer(metric_name("controversial.rate")):
                update_controversial(database, update_item_count, vote_count=60, similarity=0.7)

            update_item_count = 1000

            cursor.execute("SELECT COUNT(*) FROM controversial")
            count = first(cursor.fetchone())
            print("Got {} items in controversial table".format(count))

    def loop():
        while True:
            # noinspection PyBroadException
            try:
                update()
            except:
                traceback.print_exc()

            time.sleep(120)

    print("Starting controversial-category update thread now")
    threading.Thread(target=loop, daemon=True).start()


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

    with database, database.cursor() as cursor:
        cursor.execute(query)
        items = [fix_username_column(item) for item in cursor]

    return len(items) < 120, items


def generate_item_feed_bestof(flags, older_than, min_score):
    clauses = ["items.flags IN (%s)" % ",".join(str(flag) for flag in explode_flags(flags))]

    if older_than and older_than > 0:
        clauses += ["items.id<%d" % older_than]

    query = "SELECT * FROM items WHERE %s AND up-down>=%s ORDER BY id DESC LIMIT 120" % \
            (" AND ".join(clauses), int(min_score))

    with database, database.cursor() as cursor:
        cursor.execute(query)
        items = [fix_username_column(item) for item in cursor]

    return len(items) < 120, items


thread_pool = ThreadPoolExecutor(4)


@lru_cache_pool(thread_pool, 256)
@stats.timed(metric_name("random.update"))
def process_request_random(flags, tags=None):
    return json.dumps({
        "items": generate_item_feed_random(flags, tags),
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


def process_request_bestof(flags, older_than, min_score):
    at_end, items = generate_item_feed_bestof(flags, older_than, min_score)
    return json.dumps({
        "items": items,
        "ts": time.time(),
        "atEnd": at_end, "atStart": True, "error": None, "cache": None, "rt": 1, "qc": 1
    })


@bottle.get("/random")
@stats.timed(metric_name("random.request"))
def feed_random_cached():
    flags = int(bottle.request.params.get("flags", "1"))
    tag_filter = bottle.request.params.getunicode("tags")

    if tag_filter:
        tag_filter = tag_filter.lower().strip()
        stats.increment(metric_name("random.request_tag"))

    bottle.response.content_type = "application/json"
    return process_request_random(flags, tag_filter)


@bottle.get("/controversial")
@stats.timed(metric_name("controversial.request"))
def feed_controversial_cached():
    flags = int(bottle.request.params.get("flags", "1"))
    older_than = int(bottle.request.params.get("older", "0"))

    bottle.response.content_type = "application/json"
    return process_request_controversial(flags, older_than)


@bottle.get("/bestof")
@stats.timed(metric_name("bestof.request"))
def feed_controversial_cached():
    flags = int(bottle.request.params.get("flags", "1"))
    older_than = int(bottle.request.params.get("older", "0"))
    min_score = int(bottle.request.params.get("score", "1000"))

    bottle.response.content_type = "application/json"
    return process_request_bestof(flags, older_than, min_score)


@bottle.route("/ping")
def ping():
    return


# preload cache
[process_request_random(flags) for flags in range(1, 8)]

# update the controversial category in the background
start_update_controversial_thread()
