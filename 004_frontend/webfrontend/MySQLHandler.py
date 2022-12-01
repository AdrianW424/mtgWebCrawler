"""
author: Adrian Waldera
date: 25.11.2022
license: free
"""

import mysql.connector

class MySQLHandler():
    """class that contains usefull functionalities when dealing with MySQL
    """
    
    def createMySQLConnNCursor(self, host, port, user, password):
        """create a connection to a MySQL-system and a cursor 

        Args:
            host (str): hostname or host-ip of the network
            port (int): port that MySQL uses
            user (str): user to use, to access MySQL
            password (str): password of the user

        Returns:
            CMySQLCursor: MySQL cursor
        """
        self.mysqlConn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )
        self.mysqlCursor = self.mysqlConn.cursor()
        return self.mysqlCursor

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