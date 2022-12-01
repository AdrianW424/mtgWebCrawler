"""
author: Adrian Waldera
date: 25.11.2022
license: free
"""

from flask import Flask, render_template, request, make_response
import MySQLHandler as msh

app = Flask(__name__)
app.static_folder = 'static' # containing static data, such as css-files
mysqlh = msh.MySQLHandler() # object, that helps regarding mysql

@app.route("/", methods=["GET", "POST"])
def index():
  # search for data in database
  if request.method == "POST":
    data = dict(request.form)
    stmt = mysqlh.stmtHelper("mtg.cards_basics", ["id", data["search"]], ["name", data["search"]], ["type", data["search"]])  # create statement to query the database
    results = mysqlh.requestData(mysqlh.mysqlCursor, stmt)
  else:
    results = [-1]
    data = {"search": -1} # -1 means, that no data has been requestet yet (e.g. when reloading page)

# display html page
  return render_template("index.html", word=data["search"], data=results, length=len(results))

# program jump-in
if __name__ == "__main__":
    mysqlh.createMySQLConnNCursor("mysql", 3306, "root", "mysql") # using Host mysql, Port: 3306, User: root, Password: mysql
    app.run(host="0.0.0.0", port=5000) # starting webserver at external-ip, port: 5000