from flask import Flask, jsonify, request, render_template
import psycopg2
import psycopg2.extras

app = Flask(__name__)

def get_db():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST"),
        database=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        port=5432
    )

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/items", methods=["GET"])
def get_items():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT id, name FROM items ORDER BY name")
    items = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(items)

@app.route("/trades", methods=["GET"])
def get_trades():
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM trades_logger ORDER BY created_at DESC")
    trades = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(trades)


@app.route("/trades/<int:trade_id>", methods=["PATCH"])
def update_trade(trade_id):
    data = request.json
    sell_price = data["sell_price_per_unit"]

    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("SELECT quantity, total_purchase_price FROM trades_logger WHERE id = %s", (trade_id,))
    trade = cur.fetchone()

    total_sale = trade["quantity"] * sell_price
    profit = total_sale - trade["total_purchase_price"]

    cur.execute("""
        UPDATE trades_logger
        SET sell_price_per_unit = %s,
            total_sale_price = %s,
            profit = %s,
            sale_date = %s
        WHERE id = %s
    """, (sell_price, total_sale, profit, data["sale_date"], trade_id))

    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"success": True})

@app.route("/trades", methods=["POST"])
def add_trade():
    data = request.json
    total_purchase = data["quantity"] * data["purchase_price_per_unit"]
    sell_price = data.get("sell_price_per_unit")
    total_sale = data["quantity"] * sell_price if sell_price else None
    profit = total_sale - total_purchase if total_sale else None

    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trades_logger (item_id, item_name, quantity, purchase_price_per_unit,
        total_purchase_price, sell_price_per_unit, total_sale_price, profit,
        purchase_date, sale_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (data["item_id"], data["item_name"], data["quantity"],
          data["purchase_price_per_unit"], total_purchase,
          data["sell_price_per_unit"], total_sale, profit,
          data["purchase_date"], data["sale_date"]))
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({"success": True})

if __name__ == "__main__":
    app.run(debug=True)