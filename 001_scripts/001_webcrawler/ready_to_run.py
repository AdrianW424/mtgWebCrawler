from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings
from bs4 import BeautifulSoup
import math
import asyncio
import aiohttp
import re
import io

class MTGCrawler():
    
    def __init__(self):
        disable_warnings(InsecureRequestWarning)
        
    def __checkForEndOfPages(self, html, page):
        return int(BeautifulSoup(html, 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0]) < page

    def __deleteRedundantPages(self, res, page):
        parse_page = int(BeautifulSoup(res[-1], 'html.parser').find('div', class_='simpleRoundedBoxTitleGreyTall').find('div', class_='pagingcontrols').find('a', style='text-decoration:underline;').contents[0])
        
        if page > int(parse_page):
            res = res[:int(parse_page)-page]
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
        if div < 1:
            print("Div muss >= 1")
        else:
            async with aiohttp.ClientSession() as session:
                htmls = await self.__fetch_all(session, pageNum, offset, div)
                return htmls

class InfoExtractor():
    def getImageAndID(self, card):
        data = card.find('td', class_='leftCol').find('a').find('img')['src'][5:]
        return "https://gatherer.wizards.com" + data, re.search("multiverseid=(.*)&", data).group(1)

    def getName(self, card):
        return card.find('td', class_='middleCol').find('div', class_='cardInfo').find('span', class_='cardTitle').find('a').contents[0]

    def getType(self, card):
        return re.sub(' +', ' ', re.sub(r'\r\n', "", str(card.find('td', class_='middleCol').find('div', class_='cardInfo').find('span', class_='typeLine').contents[0]))).strip().replace("—", "-").replace("−", "-").replace("∞", "-")
    
    def getLink(self, card):
        # TODO: Link to the website of the card
        pass
    
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
    
# program jump in
if __name__ == "__main__":
    # inits
    crawler = MTGCrawler()
    extractor = InfoExtractor()
    csv = CSVCreator()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # run async function to fetch all html-pages with data on them
    htmls = loop.run_until_complete(crawler.main(div=40))
    
    csv.defineCols("ID", "Image-ID", "Name", "Type")
    i = 0
    for html in htmls:
        results = BeautifulSoup(html, 'html.parser')
        cards = results.find_all('tr', class_=['cardItem evenItem', 'cardItem oddItem'])
        for card in cards:
            img, id = extractor.getImageAndID(card)
            csv.addCard(("int", id), ("str", img), ("str", extractor.getName(card)), ("str", extractor.getType(card)))
        i += 1
        
    str = csv.getCsv()

    with io.open('Lukas.csv', 'w', encoding='utf8') as f:
        f.write(str)
        f.close()