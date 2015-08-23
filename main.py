from concurrent.futures import ThreadPoolExecutor
import json
import random
import time
from contextlib import closing
from operator import itemgetter
import itertools
import sqlite3

import datadog
import bottle

import bottle.ext.sqlite
from cache import cached


datadog.initialize()
stats = datadog.ThreadStats()
stats.start()


def metric_name(name):
    return "pr0gramm.categories." + name


def explode_flags(flags):
    if not 1 <= flags <= 7:
        raise ValueError("flags out of range")

    return [flag for flag in (1, 2, 4) if flags & flag]


def query_random_items(db, flags, max_id, promoted, count):
    query_args = (",".join(str(flag) for flag in explode_flags(flags)),
                  "!=" if promoted else "=")

    query = """
        SELECT * FROM items
        WHERE id<? AND flags IN (%s) AND promoted %s 0
        ORDER BY id DESC LIMIT 1""" % query_args

    for idx in range(count):
        rid = random.randint(0, max_id)
        with closing(db.execute(query, (rid,))) as cursor:
            yield from (dict(item) for item in cursor.fetchall())


def unique(items, key_function=id):
    keys = set()
    for item in items:
        key = key_function(item)
        if key not in keys:
            yield item
            keys.add(key)


def generate_item_feed_random(flags):
    with closing(sqlite3.connect("pr0gramm-meta.sqlite3")) as db:
        db.row_factory = sqlite3.Row
        with db:
            max_id, = db.execute("SELECT MAX(id) FROM items").fetchone()
            items = itertools.chain(
                query_random_items(db, flags, max_id, promoted=True, count=90),
                query_random_items(db, flags, max_id, promoted=False, count=30))

        items = list(unique(items, itemgetter("id")))
        random.shuffle(items)
        return items


@stats.timed(metric_name("random.update"))
def handle_request_in_background(flags):
    return json.dumps({
        "atEnd": False,
        "atStart": True,
        "error": None,
        "ts": time.time(),
        "cache": None,
        "rt": 1,
        "qc": 1,
        "items": generate_item_feed_random(flags)
    })

# Use a pool to limit number of concurrent threads
pool = ThreadPoolExecutor(2)

# Create one cache object for each possible flag value
cached_random = [cached(pool, handle_request_in_background, flags) for flags in range(1, 8)]


@bottle.get("/random")
def feed_random_cached():
    flags = int(bottle.request.params.get("flags", "1"))

    bottle.response.content_type = "application/json"
    return cached_random[flags - 1]()
