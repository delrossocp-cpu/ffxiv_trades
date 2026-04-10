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
    response = requests.get(f"https://v2.xivapi.com/api/sheet/Item?fields=Name,ItemUICategory&after={row_start}")
    items = response.json()["rows"]

    if not items:
        break

    all_items.extend(items)
    row_start = items[-1]["row_id"]

for item in all_items:
    cur.execute("INSERT INTO items (id, name, category_id, category_value) VALUES (%s, %s, %s, %s)", (item["row_id"], item["fields"]["Name"], item["fields"]["ItemUICategory"]["row_id"], item["fields"]["ItemUICategory"]["value"]))

conn.commit()
cur.close()
conn.close()
print("Done!")
