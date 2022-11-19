from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
from bs4 import BeautifulSoup
import math
import asyncio
import aiohttp

class MTGCrawler():
    
    def __init__(self):
        disable_warnings(InsecureRequestWarning)
        
    def __checkForEndOfPages(self, html, page):
        """checks if the underlined pagenumber in the @html string is equivilant to the number given as @page

    Args:
        html (str): html-string containing one page
        page (int): page number

    Returns:
        bool: if the page numbers are the same -> False (not end of Pages). If page numbers are not the same -> True
    """
        return int(BeautifulSoup(html, 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0]) != page

    def __deleteRedundantPages(self, res, div):
        # TODO: improve work -> better use inbuild search function
        """delete all pages from the end, that are redundant

        Args:
            res (list of string): list that contains all pages
            div (int): amount of pages per iteration

        Returns:
            list of string: list that contains all pages (now without redundant pages at the end)
        """
        if div == 1:
            res = res[:-1]
        else:
            buf = res[-div:]
            res = res[:-div]
            
            parse_buffer1 = int(BeautifulSoup(buf[-1], 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0])
            for i in reversed(range(1, div)):
                parse_buffer2 = int(BeautifulSoup(buf[i-1], 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0])
                if parse_buffer1 == parse_buffer2:
                    buf.pop()
                    if i == 1 and len(res) > 0:
                        if int(BeautifulSoup(buf[i-1], 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0]) == int(BeautifulSoup(res[-1], 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0]):
                            buf.pop()
                else:
                    break
                parse_buffer1 = parse_buffer2
            res += buf
        return res

    async def __fetch(self, s, url):
        async with s.get(f'https://gatherer.wizards.com/Pages/Search/Default.aspx?sort=cn+&page={url}&name=%20[]') as r:
            if r.status != 200:
                r.raise_for_status()
            return await r.text()

    async def __fetch_all(self, s, pageNum, offset, div):
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
                    res = self.__deleteRedundantPages(res, div)
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
                    res = self.__deleteRedundantPages(res, div)
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