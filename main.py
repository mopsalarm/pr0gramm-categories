from concurrent.futures import ThreadPoolExecutor
import json
import random
import time
import itertools
import sqlite3
import re
from contextlib import closing
from operator import itemgetter

import datadog

import bottle

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


@lru_cache_pool(ThreadPoolExecutor(4), 256)
@stats.timed(metric_name("random.update"))
def process_request(flags, tags=None):
    return json.dumps({
        "atEnd": False,
        "atStart": True,
        "error": None,
        "ts": time.time(),
        "cache": None,
        "rt": 1,
        "qc": 1,
        "items": generate_item_feed_random(flags, tags)
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
    return process_request(flags, tag_filter)


# preload cache
[process_request(flags) for flags in range(1, 8)]
