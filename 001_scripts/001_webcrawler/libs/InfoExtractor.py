"""
author: Adrian Waldera
date: 21.11.2022
license: free
"""

import re

class InfoExtractor():
    def getImageAndID(self, card):
        data = card.find('td', class_='leftCol').find('a').find('img')['src'][5:]
        return "https://gatherer.wizards.com" + data, re.search("multiverseid=(.*)&", data).group(1)

    def getName(self, card):
        return card.find('td', class_='middleCol').find('div', class_='cardInfo').find('span', class_='cardTitle').find('a').contents[0]

    def getType(self, card):
        return re.sub(' +', ' ', re.sub(r'\r\n', "", str(card.find('td', class_='middleCol').find('div', class_='cardInfo').find('span', class_='typeLine').contents[0]))).strip().replace("—", "-")
    
    def getLink(self, card):
        # TODO: Link to the website of the card
        pass