import requests
from datetime import datetime, timezone

STATS_WITHIN = 259200000 #3 days in milliseconds
BATCH_SIZE = 100

WORLDS = [
    {"name": "Famfrit", "id": 35},
    {"name": "Exodus", "id": 53},
    {"name": "Lamia", "id": 55},
    {"name": "Leviathan", "id": 64},
    {"name": "Ultros", "id": 77},
    {"name": "Behemoth", "id": 78},
    {"name": "Excalibur", "id": 93},
    {"name": "Hyperion", "id": 95}
]

#RETRIEVES ALL CURRENT MARKET LISTINGS FOR MOST RECENTLY UPDATED ITEMS ON LAMIA OR
#ITEMS THAT ARE MOST RECENTLY UPDATED ACROSS TWO OR MORE WORLDS. RELATED STATES ARE ALSO RETRIEVED.
def current_listings_ingest(cur, conn, headers, item_ids):

    rows_listings=[]
    rows_listing_stats=[]

    for world in WORLDS:
        for i in range(0, len(item_ids), BATCH_SIZE):
            batch = item_ids[i:i+BATCH_SIZE]
            item_ids_str = ",".join(str(item) for item in batch) #prepares comma separated list for API url insertion

            api_listings = ("https://universalis.app/api/v2/"
                    f"{world['name']}/"
                    f"{item_ids_str}"
                    f"statsWithin={STATS_WITHIN}"
                   "&fields=items.itemID%2C"
                   "items.lastUploadTime%2Citems.listings.pricePerUnit%2Citems.listings.quantity%2Citems.listings.hq%2C"
                   "items.listings.total%2Citems.currentAveragePrice%2C+items.currentAveragePriceNQ%2C"
                   "+items.currentAveragePriceHQ%2C+items.regularSaleVelocity%2C+items.nqSaleVelocity%2C"
                   "+items.hqSaleVelocity%2C+items.averagePrice%2C+items.averagePriceNQ%2C+items.averagePriceHQ%2C"
                   "+items.minPrice%2C+items.minPriceNQ%2C+items.minPriceHQ%2C"
                   "+items.maxPrice%2C+items.maxPriceNQ%2C+items.maxPriceHQ%2C+items")

            response = requests.get(api_listings,headers=headers)
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