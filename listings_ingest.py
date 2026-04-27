import requests
from datetime import datetime, timezone
import time


STATS_WITHIN = 259200000 #3 days in milliseconds
BATCH_SIZE = 100

WORLDS = [
    {"name": "Famfrit", "id": 35},
    {"name": "Hyperion", "id": 53},
    {"name": "Lamia", "id": 55},
    {"name": "Leviathan", "id": 64},
    {"name": "Ultros", "id": 77},
    {"name": "Behemoth", "id": 78},
    {"name": "Excalibur", "id": 93},
    {"name": "Exodus", "id": 95}
]

#RETRIEVES ALL CURRENT MARKET LISTINGS FOR MOST RECENTLY UPDATED ITEMS ON LAMIA OR
#ITEMS THAT ARE MOST RECENTLY UPDATED ACROSS TWO OR MORE WORLDS. RELATED STATES ARE ALSO RETRIEVED.
def current_listings_ingest(cur, conn, headers, item_ids):

    params = {
        "statsWithin": STATS_WITHIN,
        "fields": "items.itemID,items.lastUploadTime,items.listings.pricePerUnit,items.listings.quantity,items.listings.hq,items.listings.total,items.currentAveragePrice,items.currentAveragePriceNQ,items.currentAveragePriceHQ,items.regularSaleVelocity,items.nqSaleVelocity,items.hqSaleVelocity,items.averagePrice,items.averagePriceNQ,items.averagePriceHQ,items.minPrice,items.minPriceNQ,items.minPriceHQ,items.maxPrice,items.maxPriceNQ,items.maxPriceHQ"
    }

    rows_listings=[]
    rows_listing_stats=[]
    item_ids = sorted(item_ids)

    for world in WORLDS:
        for i in range(0, len(item_ids), BATCH_SIZE):
            batch = item_ids[i:i+BATCH_SIZE]
            item_ids_str = ",".join(str(item) for item in batch) #prepares comma separated list for API url insertion
            # print(f"World: {world['name']}, Batch {i}, Items: {len(batch)}, IDs: {item_ids_str[:50]}...")
            response = requests.get(f"https://universalis.app/api/v2/{world['name']}/{item_ids_str}", params=params, headers=headers)

            items_list = response.json()["items"].values()

            for item in items_list:
                item["lastUploadTime"] = datetime.fromtimestamp(item["lastUploadTime"] / 1000, tz=timezone.utc)
                rows_listings.append((item["itemID"], world["id"], world["name"], item["lastUploadTime"], item["currentAveragePrice"], item["currentAveragePriceNQ"],
                                item["currentAveragePriceHQ"], item["regularSaleVelocity"], item["nqSaleVelocity"],
                                item["hqSaleVelocity"], item["averagePrice"], item["averagePriceNQ"], item["averagePriceHQ"],
                                item["minPrice"], item["minPriceNQ"], item["minPriceHQ"], item["maxPrice"],
                                item["maxPriceNQ"], item["maxPriceHQ"]))

                for listing in item["listings"]:
                    rows_listing_stats.append((item["itemID"], world["id"], world["name"],
                    listing["pricePerUnit"], listing["quantity"],
                    listing["hq"], listing["total"]))

    cur.executemany("""INSERT INTO current_listings_stats (
                    item_id, world_id, world_name, last_upload_time,
                    current_average_price, current_average_price_nq, current_average_price_hq,
                    regular_sale_velocity, nq_sale_velocity, hq_sale_velocity,
                    average_price, average_price_nq, average_price_hq,
                    min_price, min_price_nq, min_price_hq,
                    max_price, max_price_nq, max_price_hq
                    ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                    )""", rows_listings)

    cur.executemany("""INSERT INTO current_listings (
                    item_id, world_id, world_name, price_per_unit, quantity, hq, total
                    ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                    )""", rows_listing_stats)

    conn.commit()
    print("SUCCESS: Listings uploaded.")