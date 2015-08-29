from concurrent.futures import ThreadPoolExecutor
import json
import random
import threading
import time
import itertools
import sqlite3
from contextlib import closing
from operator import itemgetter
import traceback

import datadog
import bottle

from first import first

from controversial import update_controversial
from cache import lru_cache_pool


datadog.initialize()
stats = datadog.ThreadStats()
stats.start()


def metric_name(name):
    return "pr0gramm.categories." + name


def explode_flags(flags):
    if not 1 <= flags <= 7:
        raise ValueError("flags out of range")

    return [flag for flag in (1, 2, 4) if flags & flag]


def query_random_items(db, flags, max_id, tags, promoted, count):
    q_flags = ",".join(str(flag) for flag in explode_flags(flags))
    q_promoted = "!=" if promoted else "="

    def query_with_tag(item_id):
        return db.execute("""SELECT items.*
            FROM items INNER JOIN tags ON items.id=tags.item_id
            WHERE items.id<=? AND tags.tag=? AND flags IN (%s) AND promoted %s 0
            ORDER BY items.id DESC LIMIT 1""" % (q_flags, q_promoted), (item_id, tags))

    def query_simple(item_id):
        return db.execute("""SELECT * FROM items
            WHERE id<=? AND flags IN (%s) AND promoted %s 0
            ORDER BY id DESC LIMIT 1""" % (q_flags, q_promoted), (item_id,))

    random_item_ids = sorted({random.randint(0, max_id) for _ in range(count)}, reverse=True)

    min_possible_result = max(random_item_ids)
    for rid in random_item_ids:
        if rid > min_possible_result:
            continue

        # now perform the query
        query = query_with_tag if tags else query_simple
        with closing(query(rid)) as cursor:
            items = [dict(item) for item in cursor.fetchall()]

        # stop if we haven't found anything with a smaller id than this
        if not items:
            break

        # yield the results!
        for item in items:
            min_possible_result = item["id"]
            yield item


def unique(items, key_function=id):
    keys = set()
    for item in items:
        key = key_function(item)
        if key not in keys:
            yield item
            keys.add(key)


def generate_item_feed_random(flags, tags):
    with closing(sqlite3.connect("pr0gramm-meta.sqlite3")) as db:
        db.row_factory = sqlite3.Row
        with db:
            max_id, = db.execute("SELECT MAX(id) FROM items").fetchone()
            items = itertools.chain(
                query_random_items(db, flags, max_id, tags, promoted=True, count=90),
                query_random_items(db, flags, max_id, tags, promoted=False, count=30))

        items = list(unique(items, itemgetter("id")))
        random.shuffle(items)
        return items


def start_update_controversial_thread():
    update_item_count = None

    def update():
        nonlocal update_item_count
        print("Updating controversial category now")
        with closing(sqlite3.connect("pr0gramm-meta.sqlite3")) as db:
            update_controversial(db, update_item_count, vote_count=60, similarity=0.7)
            update_item_count = 1000

            count = first(db.execute("SELECT COUNT(*) FROM controversial").fetchone())
            print("Got {} items in controversial table".format(count))

    def loop():
        while True:
            # noinspection PyBroadException
            try:
                update()
            except:
                traceback.print_exc()

            time.sleep(30)

    print("Starting controversial-category update thread now")
    threading.Thread(target=loop).start()


def generate_item_feed_controversial(flags, older):
    clauses = []

    clauses += ["items.flags IN (%s)" % ",".join(str(flag) for flag in explode_flags(flags))]

    if older and older > 0:
        clauses += ["items.id<%d" % older]

    query = """
        SELECT items.* FROM items
          JOIN controversial ON items.id=controversial.item_id
        WHERE %s AND items.id NOT IN (
          SELECT tags.item_id FROM tags WHERE tags.item_id=items.id AND tags.confidence>0.3 AND tag='repost' COLLATE nocase)
        ORDER BY controversial.id DESC LIMIT 120""" % " AND ".join(clauses)

    with closing(sqlite3.connect("pr0gramm-meta.sqlite3")) as db:
        db.row_factory = sqlite3.Row
        items = [dict(item) for item in db.execute(query)]

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


@bottle.route("/ping")
def ping():
    return


# preload cache
[process_request_random(flags) for flags in range(1, 8)]

# update the controversial category in the background
start_update_controversial_thread()
