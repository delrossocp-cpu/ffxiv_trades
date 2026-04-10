import requests
from datetime import datetime, timezone

CALL_LIST = ["world=Behemoth", "world=Excalibur", "world=Exodus", "world=Famfrit", "world=Hyperion",
             "world=Lamia", "world=Leviathan", "world=Ultros"]

# Retrieves 200 most recently updated items per world
def recently_uploaded_items_ingest(cur, conn, headers):

    rows=[]
    for world in CALL_LIST:
        response = requests.get(f"https://universalis.app/api/v2/extra/stats/most-recently-updated?{world}&entries=200", headers=headers)
        items = response.json()["items"]

        for item in items:
            item["lastUploadTime"] = datetime.fromtimestamp(item["lastUploadTime"] / 1000, tz=timezone.utc)
            rows.append((item["itemID"], item["lastUploadTime"], item["worldName"], item["worldID"]))


    cur.executemany("""INSERT INTO most_recent_items (item_id, upload_time, world_name, world_id) VALUES (%s, %s, %s, %s)""",
                rows)

    conn.commit()
    print("SUCCESS: Most recent uploads updated!")






