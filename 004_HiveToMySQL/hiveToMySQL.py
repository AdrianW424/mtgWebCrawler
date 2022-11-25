"""_summary_

Returns:
    _type_: _description_
"""

from pyhive import hive
import mysql.connector
    
class HiveToMySQL():
    hiveConn = None
    mysqlConn = None
    
    def __init__(self, hiveHost=None, hivePort=10000, hiveUser="hadoop", mysqlHost=None, mysqlPort=3306, mysqlUser="root", mysqlPassword=""):
        if(hiveHost != None):
            self.createHiveConection(hiveHost, hivePort, hiveUser)
        if(mysqlHost != None):
            self.createMySQLConnection(mysqlHost, mysqlPort, mysqlUser, mysqlPassword)
    
    def createHiveConection(self, host, port, username):
        self.hiveConn = hive.Connection(
            host=host, 
            port=port, 
            username=username
        )
        
    def createHiveCursor(self):
        if self.hiveConn != None:
            return self.hiveConn.cursor()
    
    def requestHive(self, cursor, stmt):
        if cursor != None:   
            cursor.execute(stmt)
            return cursor.fetchall()
        return None
    
    def createMySQLConnection(self, host, port, username, password=""):
        self.mysqlConn = mysql.connector.connect(
            host=host,
            port=port,
            user=username,
            password=password
        )
        
    def createMySQLCursor(self):
        if self.mysqlConn != None:
            return self.mysqlConn.cursor()

    def requestMySQL(self, cursor, stmt, many=None, update=False):
        if cursor != None:
            if many != None:
                cursor.executemany(stmt, many)
            else:
                cursor.execute(stmt)
            if update:
                self.mysqlConn.commit()
            else:
                return cursor.fetchall()
        return None
    
    def exchangeHiveToMySQL(self, cursorHive, cursorMySQL, stmtHive, stmtMySQL):
        """ transmits data from hive to mysql
        Note, that the statement for Hive should be "SELECT" or something similar.
        Note, that the statement for MySQL should be "INSERT" or something similar.

        Args:
            cursorHive (): Cursor for Hive
            cursorMySQL (): Cursor for MySQL
            stmtHive (): Statement to be executed in Hive (should be something fetching)
            stmtMySQL (): Statement to be executed in MySQL (should be something inserting)

        Returns:
            tuple | str: Output of the MySQL-Query
        """
        data = self.requestHive(cursorHive, stmtHive)
        return self.requestMySQL(cursorMySQL, stmtMySQL, data, True)
        
    