"""
author: Adrian Waldera
date: 25.11.2022
license: free
"""

import mysql.connector

class MySQLHandler():
    """class that contains usefull functionalities when dealing with MySQL
    """
    
    def createMySQLConnection(self, host, port, user, password):
        """create a connection to a MySQL-system

        Args:
            host (str): hostname or host-ip of the network
            port (int): port that MySQL uses
            user (str): user to use, to access MySQL
            password (str): password of the user
        """
        self.mysqlConn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
    
    def createMySQLCursor(self):
        """create a cursor to a MySQL-system

        Returns:
            CMySQLCursor | bool: MySQL cursor
        """
        if not self.checkForConnection(None, None, True):
            return False
        
        self.mysqlCursor = self.mysqlConn.cursor()
        return self.mysqlCursor
    
    def checkForConnection(self, database, table, onlyConn=False):
        """check if a connection to the MySQL server can be established. Can also check for existence of database and table

        Args:
            database (str): database to look for
            table (str): table to look for
            onlyConn (bool, optional): If True, then only the connection will be checked, not the database and table. Defaults to False.

        Returns:
            bool: if connection is successfull or not
        """
        if not self.mysqlConn.is_connected():  # check for connection to mysql
            return False
        if onlyConn:
            return True
        
        self.mysqlCursor.execute(f"SHOW DATABASES LIKE \'{database}\'")
        res = self.mysqlCursor.fetchall()
        if len(res) == 0 or not (database in res[0]):  # check if database exists
            return False

        self.mysqlCursor.execute(f"SHOW TABLES FROM {database} LIKE \'{table}\'")
        res = self.mysqlCursor.fetchall()
        if len(res) == 0 or not (table in res[0]):  # check if database exists
            return False

        return True

    def requestData(self, cursor, stmt=""):
        """fetch data from the MySQL connection

        Args:
            cursor (CMySQLCursor): cursor to use, to access MySQL.
            stmt (str, optional): statement to execute. Defaults to "".

        Returns:
            list: data that was fetched
        """
        if cursor != None and stmt != "":
            cursor.execute(stmt)
            return cursor.fetchall()
        return None

    def stmtHelper(self, source, params, sort = None):
        """helper function, that allows to create search-request-statements easily

        Args:
            source (str): database and table to use
            *params (list): param[0] is the column name and param[1] is the value to search for in the column param[0]
            sort (list | None): sort[0] is the column name and sort[1] is the value, either ASC or DESC or None

        Returns:
            str: created statement
        """ 
        sort = sort
        stmt = f"SELECT * FROM {source}"
        if len(params) != 0:
            stmt += " WHERE"
            for param in params:
                stmt += f" {param[0]} LIKE \"%{param[1]}%\" OR"
            stmt = stmt[:-3]
        if sort != None:
            if len(sort) != 0 and (sort[1] == "ASC" or sort[1] == "DESC"):
                stmt += f" ORDER BY {sort[0]} {sort[1]}"
        return stmt

    def closeConnection(self):
        """close the connection to MySQL
        """
        self.mysqlConn.close()