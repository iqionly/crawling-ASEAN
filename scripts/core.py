import datetime

from httpx import request

import database
import requests

from configs import formatType
from configs import urls
from configs import country


def formatUrl(url: str, year:str = '', indicator:str = '', country:str = '') -> str: 
    today = datetime.date.today()

    yearBefore = str(today.year - 1)

    format = ''
    if formatType != None:
        format += '?' + formatType if format == '' else '&' + formatType
    if(year != ''):
        yearBefore = year
    
    result = getattr(urls, url).format(indicator=indicator,country=country)

    format += '?date=' + yearBefore if format == '' else '&date=' + yearBefore
    return result + format

def runScrapIndicators(url:str, indicators=[]) -> list[requests.Response]:
    result = []
    for i in indicators:
        print('Mengambil data...')
        result.append(requests.get(formatUrl(url, indicator=i, country=country)))
        print('Berhasil ambil data ' + i + '!')
    return result