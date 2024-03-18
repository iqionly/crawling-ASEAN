import datetime

from requests_cache import CachedSession
from datetime import timedelta

import database
import requests

from configs import formatType
from configs import urls
from configs import WBcountryASEAN

# Catch every request for one hour
session = CachedSession('cache_response', expire_after=timedelta(hours=1))

def formatUrl(url: str, year:str = '', indicator:str = '', country:str = '') -> str: 
    today = datetime.date.today()

    yearBefore = str(today.year - 1)

    format = ''
    if formatType != None:
        format += '?' + formatType if format == '' else '&' + formatType
    if(year != ''):
        yearBefore = year
    
    
    if(hasattr(urls, url)):
        result = getattr(urls, url).format(indicator=indicator,country=country)
    else:
        return url


    format += '?date=' + yearBefore if format == '' else '&date=' + yearBefore
    return result + format

def runScrapIndicators(url:str, indicators=[], page = 1) -> list[requests.Response]:
    result = []
    for i in indicators:
        print('Mengambil data...')
        response = session.get(formatUrl(url, indicator=i, country=WBcountryASEAN) + '&page=' + str(page))
        if response.from_cache:
            print('Cache result')
        if response.is_expired:
            response = session.get(formatUrl(url, indicator=i, country=WBcountryASEAN) + '&page=' + str(page))
        
        result.append(response)
        print('Berhasil ambil data ' + i + '!')
    return result

def runSingleQuery(url:str, perPage = 100,headers = {'Accept': '*/*'}) -> requests.Response:
    print('Mengambil data...')
    # get single response
    response = session.get(formatUrl(url),headers=headers)
    if response.from_cache:
        print('Cache result')
    if response.is_expired:
        response = session.get(formatUrl(url),headers=headers)
    print('Berhasil ambil data ' + url)    
    return response