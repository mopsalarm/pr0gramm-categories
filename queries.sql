-- get random posts if flags == 4
EXPLAIN ANALYSE
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

-- get random posts if flags != 4.
EXPLAIN ANALYSE
(
  SELECT *
  FROM items TABLESAMPLE SYSTEM (0.5)
  WHERE flags IN (1, 2) AND promoted!=0
  ORDER BY random() LIMIT 90)
UNION (
  SELECT *
  FROM items TABLESAMPLE SYSTEM (0.1)
  WHERE flags IN (1, 2) AND promoted=0
  ORDER BY random() LIMIT 30);

-- setup
-- DROP MATERIALIZED VIEW IF EXISTS random_items_nsfl;
CREATE MATERIALIZED VIEW IF NOT EXISTS random_items_nsfl (id, promoted) AS (
  SELECT id, promoted
  FROM items
  WHERE flags=4);

-- to use "refresh view concurrently", we need a unique index on the id column
CREATE UNIQUE INDEX IF NOT EXISTS postgres_random_nsfl__id ON random_items_nsfl(id);

-- refresh the nsfl items.
REFRESH MATERIALIZED VIEW CONCURRENTLY random_items_nsfl;


