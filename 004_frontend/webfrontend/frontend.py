"""
author: Adrian Waldera
date: 25.11.2022
license: free
"""

from flask import Flask, render_template, request, make_response
import MySQLHandler as msh
from mysql.connector.errors import DatabaseError

app = Flask(__name__)
app.static_folder = 'static' # containing static data, such as css-files
mysqlh = msh.MySQLHandler() # object, that helps regarding mysql
results = [-1]
data = [-1]

@app.route("/", methods=["GET", "POST"])
def index():
  global data
  global results
  # search for data in database
  if request.method == "POST":
    data = dict(request.form)
    try:
      mysqlh.createMySQLConnection("mysql", 3306, "root", "mysql") # using Host mysql, Port: 3306, User: root, Password: mysql
    except DatabaseError:
      results = [-2]
    else:
      mysqlh.createMySQLCursor()
      if mysqlh.checkForConnection("mtg", "cards_basics"):
        stmt = mysqlh.stmtHelper("mtg.cards_basics", [["id", data["search"]], ["name", data["search"]], ["type", data["search"]]], ["id", "ASC"])  # create statement to query the database
        results = mysqlh.requestData(mysqlh.mysqlCursor, stmt)
      else:
        results = [-2]
      mysqlh.closeConnection() # close the connection again, otherwise it would not be possible to edit databases or tables
  else:
    results = [-1]
    data = {"search": -1} # -1 means, that no data has been requestet yet (e.g. when reloading page)

# display html page
  return render_template("index.html", word=data["search"], data=results, length=len(results))

# program jump-in
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000) # starting webserver at external-ip, port: 5000