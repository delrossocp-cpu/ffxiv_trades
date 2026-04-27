import psycopg2
from listings_ingest import current_listings_ingest
from dashboard_refresh_clear import dashboard_refresh
from marketboard_history_ingestion import marketboard_history_ingest
from item_scope import item_ids

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

items = item_ids(cur,conn,headers)
current_listings_ingest(cur, conn, headers, items)

##Populates marketboard_history table with up to 15000 sale transactions per item over past 3 months
marketboard_history_ingest(cur, conn, headers, items)

cur.close()
conn.close()
print("Done!")

