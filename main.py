import random
import time
from contextlib import closing
from operator import itemgetter

import datadog

import bottle

import bottle.ext.sqlite
import itertools


bottle.install(bottle.ext.sqlite.Plugin(dbfile="pr0gramm-meta.sql"))

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


@bottle.get("/random")
def feed_random(db):
    flags = int(bottle.request.params.get("flags", "1"))

    with stats.timer(metric_name("random.request")):
        max_id, = db.execute("SELECT MAX(id) FROM items").fetchone()
        items = itertools.chain(
            query_random_items(db, flags, max_id, promoted=True, count=90),
            query_random_items(db, flags, max_id, promoted=False, count=30))

        items = list(unique(items, itemgetter("id")))
        random.shuffle(items)

    return {
        "atEnd": False,
        "atStart": True,
        "error": None,
        "ts": time.time(),
        "cache": None,
        "rt": 1,
        "qc": 1,
        "items": items
    }

