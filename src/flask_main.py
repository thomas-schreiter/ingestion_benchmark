from flask import Flask
import dbwrapper

app = Flask(__name__)

@app.route("/")
@app.route("/index")
def hello():
    return "Hello World!"


@app.route("/prod")
def prod():
    # TODO query db for all results
    query = "SELECT * FROM ProducedMsg;"
    con = dbwrapper.get_connection()
    cur = dbwrapper.get_cursor(con)
    cur.execute(query)
    rows = cur.fetchall()
    con.close()
    return str(rows)

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)
    
