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
row_start = 0
all_items = []

while True:
    response = requests.get(f"https://v2.xivapi.com/api/sheet/ItemUICategory?&after={row_start}")
    items = response.json()["rows"]

    if not items:
        break

    all_items.extend(items)
    row_start = items[-1]["row_id"]

for item in all_items:
    cur.execute("INSERT INTO categories (id, category) VALUES (%s, %s)", (item["row_id"], item["fields"]["Name"]))

conn.commit()
cur.close()
conn.close()
print("Done!")
