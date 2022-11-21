"""
author: Adrian Waldera
date: 21.11.2022
license: free
"""

class CSVCreator():
    
    def __init__(this):
        this.__csvCols = []
        this.__csvConstruct = []
        this.__csv = ''
    
    def defineCols(this, *params):
        this.__csvCols = []
        for param in params:
            this.__csvCols.append(param)
    
    def addCard(this, *params):
        this.__csvConstruct.append([])
        for param in params:
            if param[0] == "int" or param[0] == "float":
                this.__csvConstruct[-1].append(param[1])
            else:
                this.__csvConstruct[-1].append("\"" + str(param[1]) + "\"")
    
    def getCsv(this):
        this.__csv = this.__createRow(this.__csvCols)
        for row in this.__csvConstruct:
            this.__csv += this.__createRow(row)
        return this.__csv
    
    def __createRow(this, row):
        buf = ""
        for valorem in row:
            buf += str(valorem) + ","
        return buf[:-1] + "\n"
        