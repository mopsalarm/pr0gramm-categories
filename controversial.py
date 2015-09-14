from first import first


def update_controversial(database, count=1000, vote_count=40, similarity=0.75):
    with database, database.cursor() as cursor:
        # get the last few items
        if count:
            cursor.execute("SELECT MAX(id) FROM items")
            max_id = first(cursor.fetchone())
            first_item_id = max_id - count
        else:
            first_item_id = 0

        query = """
          INSERT INTO controversial (item_id)
            SELECT id FROM items
              WHERE id>%s AND up>%s AND down>%s AND 1.0*up/down>=%s AND 1.0*up/down<=%s
              ORDER BY id ASC
            ON CONFLICT(item_id) DO NOTHING
        """

        cursor.execute(query, [first_item_id, vote_count, vote_count, similarity, 1 / similarity])

