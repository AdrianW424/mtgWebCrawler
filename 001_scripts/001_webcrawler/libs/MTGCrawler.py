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
            s (_type_): _description_
            pageNum (_type_): _description_
            offset (_type_): _description_
            div (_type_): _description_

        Returns:
            _type_: _description_
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
        #div = 20        # integer
        #offset = 200      # integer - sollte Vielfaches von div sein - bei div = 10 -> offset = 10 oder = 20 oder ... oder = 250
                        # wird als offset = 241 gewählt, so wird intern daraus 240 gemacht, genauso bei offset = 242 oder ... oder 249 ... usw.
        if div < 1:
            print("Div muss >= 1")
        else:
            async with aiohttp.ClientSession() as session:
                htmls = await self.__fetch_all(session, pageNum, offset, div)
                #print(htmls)
                return htmls