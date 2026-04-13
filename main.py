import psycopg2
from top_recent_items_ingest import recently_uploaded_items_ingest
from listings_ingest import current_listings_ingest
from dashboard_refresh_clear import dashboard_refresh
from marketboard_history_ingestion import marketboard_history_ingest

headers = {"User-Agent": "ffxiv-market-dashboard/1.0 (personal project)"}

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="FFXIV Market",
    user="postgres",
    password="55bfo"
)
cur = conn.cursor()

#Begins ingestion process by calling function to clear existing tables of stale data
answer = input("Add the current selection of 'recently updated items' to history table? Y/N: ")
dashboard_refresh(cur, conn, answer)

#Populates most_recent_items table w/ 200 items per world
recently_uploaded_items_ingest(cur, conn, headers)

##Assembles list of item ids from either Lamia or where an item appears on most recently updated for more than one world
cur.execute(
        """select distinct(item_id)
        from most_recent_items
        where item_id in (
        select item_id from most_recent_items
        group by item_id having count(distinct world_id) > 1
        ) or world_name = 'Lamia'"""
    )
item_ids = [row[0] for row in cur.fetchall()]

cur.execute("""
    SELECT id FROM items i
    JOIN categories c
    ON i.category_id = c.id
    WHERE i.id NOT IN %s 
    AND c.category IN ('Ingredient', 'Dye', 'Reagent', 'Materia')
""", (tuple(item_ids),))
item_ids.extend([row[0] for row in cur.fetchall()])

##Updates table w/ all listings for most recently updated items of interest
current_listings_ingest(cur, conn, headers, item_ids)

##Populates marketboard_history table with up to 15000 sale transactions per item over past 3 months
marketboard_history_ingest(cur, conn, headers, item_ids)

cur.close()
conn.close()
print("Done!")

