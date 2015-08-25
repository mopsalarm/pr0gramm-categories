from first import first


def prepare(db):
    db.execute("""
        CREATE TABLE IF NOT EXISTS controversial (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          item_id INTEGER UNIQUE NOT NULL
        );
    """)


def update_controversial(database, count=1000, vote_count=40, similarity=0.75):
    with database:
        # create table if necessary
        prepare(database)

        # get the last few items
        if count:
            max_id = first(database.execute("SELECT MAX(id) FROM items").fetchone())
            first_item_id = max_id - count
        else:
            first_item_id = 0

        query = """
          INSERT OR IGNORE INTO controversial (item_id)
              SELECT id FROM items
                WHERE id>? AND up>? AND down>? AND 1.0*up/down>=? AND 1.0*up/down<=?
                ORDER BY id ASC"""

        database.execute(query, [first_item_id, vote_count, vote_count, similarity, 1 / similarity])

