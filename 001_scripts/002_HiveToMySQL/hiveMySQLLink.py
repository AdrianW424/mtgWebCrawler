"""
author: Adrian Waldera
date: 25.11.2022
license: free
"""

from pyhive import hive
import mysql.connector

class HiveToMySQL():
    """class that contains functions and data for a data export from hive to mysql
    """
    hiveConn = None
    mysqlConn = None

    def __init__(self, hiveHost = None, hivePort = 10000, hiveUser = "hadoop", mysqlHost = None, mysqlPort = 3306, mysqlUser = "root", mysqlPassword = ""):
        """constructor, that is able to create connections to hive and mysql if desired

        Args:
            hiveHost (str, optional): Hive hostname or host-ip. Defaults to None.
            hivePort (int, optional): Hive port. Defaults to 10000.
            hiveUser (str, optional): Hive username. Defaults to "hadoop".
            mysqlHost (str, optional): MySQL hostname or host-ip. Defaults to None.
            mysqlPort (int, optional): MySQL port. Defaults to 3306.
            mysqlUser (str, optional): MySQL username. Defaults to "root".
            mysqlPassword (str, optional): MySQL password. Defaults to "".
        """
        if(hiveHost != None):
            self.createHiveConection(hiveHost, hivePort, hiveUser)
        if(mysqlHost != None):
            self.createMySQLConnection(mysqlHost, mysqlPort, mysqlUser, mysqlPassword)

    def createHiveConection(self, host = None, port = 10000, user = "hadoop"):
        """create a connection to a Hive-server

        Args:
            host (str, optional): Hive hostname or host-ip. Defaults to None.
            port (int, optional): Hive port. Defaults to 10000.
            username (str, optional): Hive username. Defaults to "hadoop".
        """
        self.hiveConn = hive.Connection(
            host=host,
            port=port,
            username=user
        )

    def createHiveCursor(self):
        """create a cursor for Hive to access it

        Returns:
            hive.Cursor: Hive cursor
        """
        if self.hiveConn != None:
            return self.hiveConn.cursor()

    def requestHive(self, cursor, stmt):
        """send a request to hive (and get the answer, if not an updating statement)

        Args:
            cursor (hive.Cursor): cursor to use to execute the statement
            stmt (str): statement to be executed
            Returns:
            list | None: list of rows or None
        """
        if cursor != None:
            cursor.execute(stmt)
            return cursor.fetchall()
        return None

    def createMySQLConnection(self, host = None, port = 3306, user = "root", password = ""):
        """create a connection to a MySQL-server

        Args:
            host (str, optional): MySQL hostname or host-ip. Defaults to None.
            port (int, optional): MySQL port. Defaults to 3306.
            user (str, optional): MySQL username. Defaults to "root".
            password (str, optional): MySQL password. Defaults to "".
        """
        self.mysqlConn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )

    def createMySQLCursor(self):
        """create a cursor for MySQL to access it

        Returns:
            CMySQLCursor: MySQL cursor
        """
        if self.mysqlConn != None:
            return self.mysqlConn.cursor()

    def requestMySQL(self, cursor, stmt, many = None, update = False):
        """send a request to hive (and get the answer, if not an updating statement)

        Args:
            cursor (CMySQLCursor): cursor to use to execute the statement
            stmt (str): statement to be executed
            many (list | tuple, optional): list of data, that can be added to the query. If @many is not None, then cursor.executemany() will be used instead of cursor.execute(). Defaults to None.
            update (bool, optional): specifies if an updating statement is being used. Defaults to False.

        Returns:
            list | tuple | None: _description_
        """
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
            cursorHive (hive.Cursor): Cursor for Hive
            cursorMySQL (CMySQLCursor): Cursor for MySQL
            stmtHive (str): Statement to be executed in Hive (should be something fetching)
            stmtMySQL (str): Statement to be executed in MySQL (should be something inserting)

        Returns:
            tuple | str: Output of the MySQL-Query
        """
        data = self.requestHive(cursorHive, stmtHive)
        return self.requestMySQL(cursorMySQL, stmtMySQL, data, True)

def main():
    htm = HiveToMySQL("hadoop", 10000, "hadoop", "mysql", 3306, "root", "mysql")
    hiveCursor = htm.createHiveCursor()
    mysqlCursor = htm.createMySQLCursor()

    ret = htm.exchangeHiveToMySQL(hiveCursor, mysqlCursor, "SELECT id, image_id, name, type FROM default.cards_basics", "INSERT INTO mtg.cards_basics (id, image_id, name, type) VALUES (%s, %s, %s, %s)")
    print(ret)