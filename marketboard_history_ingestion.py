import requests
from datetime import datetime, timezone

BATCH_SIZE = 100
ENTRIES = "20000"
ENTRY_TIMEFRAME = "7776000" #3 months in seconds

# Retrieves ENTRIES number of sales over ENTRY_TIMEFRAME for items of interest. Sales history is exclusive to Lamia.
def marketboard_history_ingest(cur, conn, headers, item_ids):

    items_list = []
    ##Iterates through BATCH_SIZE length of items (API offers call for max 100 items)
    for i in range(0, len(item_ids), BATCH_SIZE):
        batch = item_ids[i:i + BATCH_SIZE]

        # prepares comma separated list for API url insertion
        item_ids_str = ",".join(str(item) for item in batch)

        api_listings = ("https://universalis.app/api/v2/history/Lamia/"
                        f"{item_ids_str}?entriesToReturn={ENTRIES}"
                        f"&entriesWithin={ENTRY_TIMEFRAME}&minSalePrice=0"
                        "&maxSalePrice=1000000000")

        response = requests.get(api_listings, headers=headers)
        items_list.extend(response.json()["items"].values())

    rows = []
    for item in items_list:
        for entry in item['entries']:
            entry['timestamp'] = datetime.fromtimestamp(entry['timestamp'], tz=timezone.utc)
            rows.append((item['itemID'],entry['hq'], entry['pricePerUnit'], entry['quantity'], entry['timestamp']))

    cur.execute("DROP INDEX IF EXISTS idx_item_id")
    cur.executemany("INSERT INTO marketboard_history (item_id, hq, price_per_unit, quantity, transaction_time)"
                    "VALUES (%s, %s, %s, %s, %s)", rows)
    cur.execute("CREATE INDEX idx_item_id ON marketboard_history(item_id)")
    conn.commit()
    print("SUCCESS: Marketboard history successfully updated!")



