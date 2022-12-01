import mysql.connector

class MySQLHandler():
    def createMySQLConnNCursor(self, host, port, user, password):
        self.mysqlConn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
        self.mysqlCursor = self.mysqlConn.cursor()
        return self.mysqlCursor

    def requestData(self, cursor=None, stmt=""):
        if cursor != None and stmt != "":
            cursor.execute(stmt)
            return cursor.fetchall()
        return None
    
    def stmtHelper(self, source, *params):
        stmt = f"SELECT * FROM {source}"
        if len(params) != 0:
            stmt += " WHERE"
            for param in params:
                stmt += f" {param[0]} LIKE \"%{param[1]}%\" OR"
            stmt = stmt[:-3]
        return stmt
    
    def closeConnection(self):
        self.mysqlConn.close()