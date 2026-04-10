import psycopg2
import requests

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="FFXIV Market",
    user="postgres",
    password="55bfo"
)

cur = conn.cursor()

dc_request = requests.get("https://universalis.app/api/v2/data-centers")
datacentres = dc_request.json()

world_request = requests.get("https://universalis.app/api/v2/worlds")
worlds = world_request.json()

datacentres = [dc for dc in datacentres if dc["region"] in ("Europe", "North-America")]

for dc in datacentres:
    cur.execute("INSERT INTO datacenters (name, region) VALUES (%s, %s)",
                (dc["name"], dc["region"]))

    for world_id in dc["worlds"]:
        world_name = next((w["name"] for w in worlds if w["id"] == world_id), None)
        if world_name:
            cur.execute("INSERT INTO worlds (id, name, datacenter) VALUES (%s, %s, %s)",
                        (world_id, world_name, dc["name"]))

conn.commit()
cur.close()
conn.close()
print("Yeehaw")