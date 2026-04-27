from collections import defaultdict

import requests

CALL_LIST = ["world=Behemoth", "world=Excalibur", "world=Exodus", "world=Famfrit", "world=Hyperion",
             "world=Lamia", "world=Leviathan", "world=Ultros"]

#Assembles list of item ids from either Lamia or where an item appears on most recently updated for more than one world
def item_ids(cur, conn, headers):

    rows = []

    for world in CALL_LIST:
        response = requests.get(f"https://universalis.app/api/v2/extra/stats/most-recently-updated?{world}&entries=200",
                                headers=headers)
        items = response.json()["items"]

        for item in items:
            rows.append((item["itemID"],  item["worldName"]))

    multiple_ids = defaultdict(set)
    lamia_ids = set()

    for item_id, world_name in rows:
        multiple_ids[item_id].add(world_name)
        if world_name == "Lamia":
            lamia_ids.add(item_id)

    ids = {item_id for item_id, worlds in multiple_ids.items()
              if len(worlds) > 1} | lamia_ids

    cur.execute("""
        SELECT i.id FROM items i
        JOIN categories c
        ON i.category_id = c.id
        WHERE i.id NOT IN %s 
        AND c.category IN ('Ingredient', 'Dye', 'Reagent', 'Materia')
    """, (tuple(ids),))

    category = [row[0] for row in cur.fetchall()]
    scope = [(item, 'True') for item in ids]
    scope.extend([(item, 'False') for item in category])

    cur.executemany("""
        INSERT INTO item_scope (id, is_recent) 
        VALUES (%s, %s)
    """, scope)

    conn.commit()

    ids = list(ids)
    ids.extend(category)
    return ids

