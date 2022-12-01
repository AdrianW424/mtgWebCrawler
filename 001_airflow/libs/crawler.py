"""
author: Adrian Waldera
date: 21.11.2022
license: free
"""

from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
from bs4 import BeautifulSoup
import math
import asyncio
import aiohttp
import re
import io
import sys

class MTGCrawler():
    """ Contains all functionality and data for crawling MTG cards
    """

    def __init__(self):
        """ Constructor of @MTGCrawler
            Disables insecure warnings
        """
        disable_warnings(InsecureRequestWarning)

    def __checkForEndOfPages(self, html, page):
        """ Checks if the underlined pagenumber in the @html string is equivilant to the number given as @page

        Args:
            html (str): string containing the html code of one page
            page (int): page number

        Returns:
            bool: if the page numbers are the same -> False (not end of Pages). If page numbers are not the same -> True
        """
        return int(BeautifulSoup(html, 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0]) < page

    def __deleteRedundantPages(self, res, page):
        """ Delete all pages from the end, that are redundant

        Args:
            res (list of string): list that contains all pages

        Returns:
            list of string: list that contains all pages (now without redundant pages at the end)
        """
        parse_page = int(BeautifulSoup(res[-1], 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0])

        if page > int(parse_page):
            res = res[:int(parse_page)-page]
        return res

    async def __fetch(self, s, url):
        """ Fetches the html code of the given URL

        Args:
            s (asyncio.session): Session to be used
            url (str): URL to fetch html code from

        Returns:
            str: html code of the page
        """
        async with s.get(f'https://gatherer.wizards.com/Pages/Search/Default.aspx?sort=cn+&page={url}&name=%20[]') as r:
            if r.status != 200:
                r.raise_for_status()
            return await r.text()

    async def __fetch_all(self, s, pageNum, offset, div):
        """ Fetches the html code of multiple pages 

        Args:
            s (asyncio.session): Session to be used
            pageNum (int): Maximum amount of pages to be fetched
            offset (int): Offset to start at a specific page
            div (int): Amount of pages to be fetched per iteration

        Returns:
            list: Fetched data
        """
        tasks = []
        res = []
        end = math.ceil(pageNum/div)
        if pageNum != 0:
            for i in range(int(offset/div), end + int(offset/div)):
                for i2 in range(i*div, (i + 1)*div):
                    task = asyncio.create_task(self.__fetch(s, i2))
                    tasks.append(task)
                res = await asyncio.gather(*tasks)
                print(str(i*div) + "..." + str((i+1)*div-1))
                if self.__checkForEndOfPages(res[-1], (i + 1)*div):
                    res = self.__deleteRedundantPages(res, (i + 1)*div)
                    break
            if pageNum < len(res):
                res = res[:pageNum]
        else:
            i = int(offset/div)
            while 1:
                for i2 in range(i*div, (i + 1)*div):
                    task = asyncio.create_task(self.__fetch(s, i2))
                    tasks.append(task)
                res = await asyncio.gather(*tasks)
                print(str(i*div) + "..." + str((i+1)*div-1))
                if self.__checkForEndOfPages(res[-1], (i + 1)*div):
                    res = self.__deleteRedundantPages(res, (i + 1)*div)
                    break
                i += 1
        return res

    async def main(self, pageNum = 0, div = 20, offset = 0):
        """_summary_

        Args:
            pageNum (int, optional): Maximum amount of pages to be fetched. Defaults to 0.
            div (int, optional): Offset to start at a specific page. Defaults to 20.
            offset (int, optional): Amount of pages to be fetched per iteration. Defaults to 0.

        Returns:
            list: List of html strings representing the pages
        """
        if div < 1:
            print("Div muss >= 1")
        else:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
                htmls = await self.__fetch_all(session, pageNum, offset, div)
                return htmls

class InfoExtractor():
    """ Contains functionality to extract information out of html strings
    """
    def getImageAndID(self, card):
        """ Extracts the Multiverse-ID and image-URL of the card

        Args:
            card (str): Html string to extract information from

        Returns:
            str, str: Image-URL, Multiverse-ID of the card
        """
        data = card.find('td', class_='leftCol').find('a').find('img')['src'][5:]
        return "https://gatherer.wizards.com" + data, re.search("multiverseid=(.*)&", data).group(1)

    def getName(self, card):
        """ Extracts the name of the card

        Args:
            card (str): Html string to extract information from

        Returns:
            str: name of the card
        """
        return card.find('td', class_='middleCol').find('div', class_='cardInfo').find('span', class_='cardTitle').find('a').contents[0]

    def getType(self, card):
        """ Extracts the type of the card

        Args:
            card (str): Html string to extract information from

        Returns:
            str: type of the card
        """
        return re.sub(' +', ' ', re.sub(r'\r\n', "", str(card.find('td', class_='middleCol').find('div', class_='cardInfo').find('span', class_='typeLine').contents[0]))).strip().replace("~@~T", "-").replace("~H~R", "-").replace("~H~^", "-")

class CSVCreator():
    """ Contains functionality to store data in a table-like way and export it as a CSV file
    """

    def __init__(self):
        """ Constructor of @CSVCreator
        """
        self.__csvCols = []
        self.__csvConstruct = []
        self.__csv = ''

    def defineCols(self, *params):
        """ Add columns to the table
        
            *params (list): Name of columns to create
        """
        self.__csvCols = []
        for param in params:
            self.__csvCols.append(param)

    def addCard(self, *params):
        """ Add cards (rows) to the table
        
            *params (list): Cards to add to the table
        """
        self.__csvConstruct.append([])
        for param in params:
            if param[0] == "int" or param[0] == "float":
                self.__csvConstruct[-1].append(param[1])
            else:
                self.__csvConstruct[-1].append("\"" + str(param[1]) + "\"")

    def getCsv(self):
        """ Create a CSV string with the table

        Returns:
            str: CSV string that represents the table
        """
        self.__csv = self.__createRow(self.__csvCols)
        for row in self.__csvConstruct:
            self.__csv += self.__createRow(row)
        return self.__csv

    def __createRow(self, row):
        """ Create a row of the CSV string

        Args:
            row (list): Elements of one row

        Returns:
            str: The given row in the form of a CSV string
        """
        buf = ""
        for valorem in row:
            buf += str(valorem) + ","
        return buf[:-1] + "\n"

# program jump in
if __name__ == "__main__":
    # inits
    crawler = MTGCrawler()
    extractor = InfoExtractor()
    csv = CSVCreator()
    loop = asyncio.get_event_loop()

    # run async function to fetch all html-pages with data on them
    htmls = loop.run_until_complete(crawler.main(pageNum=0,div=40,offset=0))

    csv.defineCols("ID", "Image-ID", "Name", "Type")
    i = 0
    for html in htmls:
        results = BeautifulSoup(html, 'html.parser')
        cards = results.find_all('tr', class_=['cardItem evenItem', 'cardItem oddItem'])
        for card in cards:
            img, id = extractor.getImageAndID(card)
            csv.addCard(("int", id), ("str", img), ("str", extractor.getName(card)), ("str", extractor.getType(card)))
        i += 1

    csvData = csv.getCsv()

    with io.open('/home/airflow/mtg/cards.basics_' + str(sys.argv[1]) + '.csv', 'w', encoding='utf8') as f:
        f.write(csvData)
        f.close()