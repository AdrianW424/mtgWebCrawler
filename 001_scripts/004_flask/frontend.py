from flask import Flask, render_template, request, make_response
import MySQLHandler as msh

app = Flask(__name__)
mysqlh = msh.MySQLHandler()

@app.route("/", methods=["GET", "POST"])
def index():
  # (C1) SEARCH FOR USERS

  if request.method == "POST":
    data = dict(request.form)
    stmt = mysqlh.stmtHelper("mtg", ["id", data["search"]], ["name", data["search"]], ["type", data["search"]])
    results = mysqlh.requestData(mysqlh.mysqlCursor, stmt)
  else:
    results = []
  # (C2) RENDER HTML PAGE
  return render_template("index.html")
 
# (D) START
if __name__ == "__main__":
    #mysqlh.createMySQLConnNCursor("mysql", 3306, "root", "mysql")
    app.run()