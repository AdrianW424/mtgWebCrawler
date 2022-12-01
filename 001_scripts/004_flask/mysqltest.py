import MySQLHandler as msh

mysqlh = msh.MySQLHandler()
mysqlh.createMySQLConnNCursor("mysql", 3306, "root", "mysql")
stmt = mysqlh.stmtHelper("mtg", ["id", "Abandon"], ["name", "Abandon"], ["type", "Abandon"])
results = mysqlh.requestData(mysqlh.mysqlCursor, stmt)