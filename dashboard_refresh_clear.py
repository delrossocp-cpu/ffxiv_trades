# USED FOR CLEARING OUT MARKETBOARD_HISTORY, CURRENT_LISTINGS, CURRENT_LISTINGS_STATS
# AND MOST_RECENT_ITEMS IN ORDER TO POPULATE W/ FRESH DATA THROUGH INGEST

def dashboard_refresh(cur, conn, answer):

    if answer == "Y":
        try:
            cur.execute("SELECT COALESCE(MAX(batch), 0) FROM most_recent_items_history")
            current_batch = cur.fetchone()[0]
            new_batch = current_batch + 1

            cur.execute("""
                    INSERT INTO most_recent_items_history (item_id, upload_time, world_name, world_id, batch)
                    SELECT item_id, upload_time, world_name, world_id, %s
                    FROM most_recent_items WHERE world_name = 'Lamia'
                """, (new_batch,))
            conn.commit()
            print("SUCCESS: Saved recent items into history.")

        except Exception as e:
            conn.rollback()
            print(f"Error: Something went wrong while trying to save into most_recent_items. See{e}")
            raise

    try:
        cur.execute("TRUNCATE TABLE most_recent_items")
        print("SUCCESS: Most recent items erased.")
    except Exception as e:
        conn.rollback()
        print(f"Error: Something went wrong while trying to truncate most_recent_items table. See{e}")
        raise

    try:
        cur.execute("TRUNCATE TABLE current_listings")
        print("SUCCESS: Current listings erased.")
    except Exception as e:
        conn.rollback()
        print(f"Error: Something went wrong while trying to truncate current_listings table. See{e}")
        raise

    try:
        cur.execute("TRUNCATE TABLE current_listings_stats")
        print("SUCCESS: Stats for current listings erased.")
    except Exception as e:
        conn.rollback()
        print(f"Error: Something went wrong while trying to truncate current_listings_stats table. See{e}")
        raise

    try:
        cur.execute("TRUNCATE TABLE marketboard_history")
        print("SUCCESS: Marketboard history cleared.")
    except Exception as e:
        conn.rollback()
        print(f"Error: Something went wrong while trying to truncate marketboard_history table. See{e}")
        raise


