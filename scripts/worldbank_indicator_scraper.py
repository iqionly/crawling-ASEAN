from datetime import datetime
from unittest import result
import database

import time
import logging

from requests import Response, session
from scripts import core

from requests_cache import CachedSession
from datetime import timedelta

from configs import formatType, urls, WBcountryASEAN

class engine:
    listIndicators = [
        'SL.TLF.0714.FE.ZS',
        'SL.TLF.0714.MA.ZS',
        'SL.TLF.0714.ZS',
        'SL.TLF.CACT.FE.ZS',
        'SL.TLF.CACT.MA.ZS',
        'SP.DYN.LE00.FE.IN',
        'SP.DYN.LE00.MA.IN',
        'SH.HIV.1524.FE.ZS',
        'SH.HIV.1524.MA.ZS',
        'SE.ENR.PRIM.FM.ZS',
        'SE.ENR.PRSC.FM.ZS',
        'SL.UEM.TOTL.FE.ZS',
        'SL.UEM.TOTL.MA.ZS',
        'SL.EMP.VULN.FE.ZS',
        'SL.EMP.VULN.MA.ZS',
        'SL.TLF.0714.FE.ZS',
        'SL.FAM.WORK.FE.ZS',
        'SL.FAM.WORK.MA.ZS',
        'SL.AGR.EMPL.FE.ZS',
        'SL.AGR.EMPL.MA.ZS',
        'SL.IND.EMPL.FE.ZS',
        'SL.IND.EMPL.MA.ZS',
        'SL.SRV.EMPL.FE.ZS',
        'SL.SRV.EMPL.MA.ZS',
        'SL.EMP.TOTL.SP.ZS',
        'SL.GDP.PCAP.EM.KD',
        'SL.TLF.TOTL.FE.ZS',
        'SL.TLF.TOTL.IN',
        'SL.UEM.TOTL.ZS',
        'SL.UEM.1524.FE.ZS',
        'SL.UEM.1524.MA.ZS',
        'SL.EMP.WORK.FE.ZS',
        'SL.EMP.WORK.MA.ZS',
        'SP.POP.0014.TO.ZS',
        'SP.POP.1564.TO.ZS',
        'SP.POP.65UP.TO.ZS',
        'SP.POP.GROW',
        'SP.POP.TOTL.FE.ZS',
        'SP.POP.TOTL',
        'SE.PRM.UNER.FE',
        'SE.PRM.UNER.MA',
        'SP.DYN.CONU.ZS',
        'SP.DYN.TFRT.IN',
        'IC.FRM.FEMO.ZS',
        'IC.FRM.FEMM.ZS',
        'SE.PRM.GINT.FE.ZS',
        'SE.PRM.GINT.MA.ZS',
        'SE.ADT.LITR.FE.ZS',
        'SE.ADT.LITR.MA.ZS',
        'SE.ADT.1524.LT.FE.ZS',
        'SE.ADT.1524.LT.MA.ZS',
        'SH.STA.MMRT',
        'SE.PRM.PRSL.FE.ZS',
        'SE.PRM.PRSL.MA.ZS',
        'SH.STA.ANVC.ZS',
        'SE.PRM.CMPT.FE.ZS',
        'SE.PRM.CMPT.MA.ZS',
        'SE.SEC.PROG.FE.ZS',
        'SE.SEC.PROG.MA.ZS',
        'SG.GEN.PARL.ZS',
        'SG.TIM.UWRK.FE',
        'SE.PRM.REPT.FE.ZS',
        'SE.PRM.REPT.MA.ZS',
        'SP.MTR.1519.ZS',
        'SG.DMK.SRCR.FN.ZS',
        'SE.XPD.TOTL.GD.ZS',
        'SE.XPD.TOTL.GB.ZS',
        'SE.XPD.PRIM.PC.ZS',
        'SE.XPD.SECO.PC.ZS',
        'SE.XPD.TERT.PC.ZS',
        'SE.ADT.LITR.ZS',
        'SE.ADT.1524.LT.ZS',
        'SE.PRM.CMPT.ZS',
        'SE.PRM.ENRL.TC.ZS',
        'SE.PRE.ENRR',
        'SE.PRM.ENRR',
        'SE.PRM.NENR',
        'SE.SEC.ENRR',
        'SE.SEC.NENR',
        'SE.TER.ENRR',
        'SE.PRM.TCAQ.ZS',
        'NY.ADJ.SVNG.GN.ZS',
        'NV.AGR.TOTL.ZS',
        'GC.DOD.TOTL.GD.ZS',
        'BM.GSR.ROYL.CD',
        'BX.GSR.ROYL.CD',
        'BN.CAB.XOKA.CD',
        'GC.XPN.TOTL.GD.ZS',
        'NE.EXP.GNFS.ZS',
        'DT.DOD.DECT.GN.ZS',
        'DT.DOD.DECT.CD',
        'BX.KLT.DINV.CD.WD',
        'NY.GDP.MKTP.CD',
        'NY.GDP.MKTP.KD.ZG',
        'NY.GDP.PCAP.CD',
        'NY.GDP.PCAP.KD.ZG',
        'NY.GDP.PCAP.PP.CD',
        'NY.GNP.PCAP.CD',
        'NY.GNP.PCAP.PP.CD',
        'NY.GNP.ATLS.CD',
        'NY.GNP.MKTP.PP.CD',
        'BX.GRT.EXTA.CD.WD',
        'NE.GDI.TOTL.ZS',
        'NY.GNS.ICTR.ZS',
        'NE.IMP.GNFS.ZS',
        'NV.IND.TOTL.ZS',
        'NY.GDP.DEFL.KD.ZG',
        'FP.CPI.TOTL.ZG',
        'NV.MNF.TECH.ZS.UN',
        'DT.ODA.ODAT.GN.ZS',
        'DT.ODA.ODAT.PC.ZS',
        'DT.ODA.ODAT.CD',
        'PA.NUS.PPP',
        'BX.TRF.PWKR.CD.DT',
        'PA.NUS.PPPC.RF',
        'GC.REV.XGRT.GD.ZS',
        'DT.DOD.DSTC.IR.ZS',
        'BX.GRT.TECH.CD.WD',
        'DT.TDS.DECT.EX.ZS',
        'FI.RES.TOTL.CD',
        'EN.POP.DNST',
        'EN.URB.LCTY.UR.ZS',
        'EN.URB.MCTY.TL.ZS',
        'EN.POP.SLUM.UR.ZS',
        'SP.URB.TOTL',
        'SP.URB.TOTL.IN.ZS'
    ]

    result: Response
    results: list[Response]
    jsonResult = []

    failed_counter = 0

    def __init__(self, typeUrl='urlCI', listIndicators = None, page = 1, single=False) -> None:
        # use default example list indicators, if user not specify
        if listIndicators != None:
            self.listIndicators = listIndicators


        # if(single):
        #     self.result = core.runSingleQuery(typeUrl, page)
        #     return
        # self.results = core.runScrapIndicators(typeUrl, self.listIndicators, page)
        # Catch every request for one hour
        self.session = CachedSession('cache_response', expire_after=timedelta(hours=24))

    # we need to get on page first of on indicator an loop single query init
    def scrapIndicatorsAutoAllPages(self):
        # loop all indicator first
        # we need to sleep script 5 seconds, each query save2db runned (to prevent server down)
        for indicator in self.listIndicators:
            statusPage = self.requestData(indicator)
            if(statusPage == False):
                continue
            for loop in range(2, statusPage[0]['pages'] + 1):
                stat = self.requestData(indicator,loop)
                if(stat == False):
                    continue
                if(stat[1] == False):
                    time.sleep(5)
        database.cursor.close()
        database.connection.close()
        pass

    def scrapCountryAutoAllPages(self):
        statusPage = self.requestDataCountry()
        if(statusPage == False):
            return False
        for loop in range(2, statusPage[0]['pages'] + 1):
            stat = self.requestDataCountry(loop)
            if(stat == False):
                continue
            if(stat[1] == False):
                time.sleep(5)
        database.cursor.close()
        database.connection.close()

    def requestData(self, indicator, page=1):
        url = urls.urlCI.format(country=WBcountryASEAN, indicator=indicator)
        # adding format json
        url += '?' + formatType
        # adding per_page data to default 50
        url += '&per_page=50'
        url += '&page=' + str(page)
        print(url)

        # get the data from cache or get new one
        response = self.session.get(url)
        if response.from_cache:
            print('Cache result')
        if response.is_expired:
            response = self.session.get(url)
        if(response.status_code != 200):
            database.cursor.close()
            database.connection.close()
            print('Not 200 OK trye again later')
            FORMAT = '%(asctime)s | %(message)s'
            logging.basicConfig(format=FORMAT)
            logger = logging.getLogger('failed_jobs')
            logger.warning('Failed Fetched Data: %s', 'World Bank, url: ' + url)
            self.failed_counter += 1
            # if failed error/fail 10 strike, we need to break jobs, and tell something went wrong
            if(self.failed_counter > 10):
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.error('Failed Fetched Data: %s', '============SOMETHING WRONG CANNOT GET DATA============')
                exit()
            return False
        else:
            self.failed_counter = 0
        resultData = response.json()
        statusPage = resultData[0]
        # Loop get data to all pages from 2 to length pages indicator results
        self.save2DB(resultData[1], statusPage['sourceid'])
        return [statusPage, response.from_cache]
    
    def requestDataCountry(self, page=1):
        url = urls.urlC.format(country='')
        # adding format json
        url += '?' + formatType
        # adding per_page data to default 50
        url += '&per_page=50'
        url += '&page=' + str(page)
        print(url)

        # get the data from cache or get new one
        response = self.session.get(url)
        if response.from_cache:
            print('Cache result')
        if response.is_expired:
            response = self.session.get(url)
        if(response.status_code != 200):
            database.cursor.close()
            database.connection.close()
            print('Not 200 OK trye again later')
            FORMAT = '%(asctime)s | %(message)s'
            logging.basicConfig(format=FORMAT)
            logger = logging.getLogger('failed_jobs')
            logger.warning('Failed Fetched Data: %s', 'World Bank, url: ' + url)
            self.failed_counter += 1
            # if failed error/fail 10 strike, we need to break jobs, and tell something went wrong
            if(self.failed_counter > 10):
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.error('Failed Fetched Data: %s', '============SOMETHING WRONG CANNOT GET DATA============')
                exit()
            return False
        else:
            self.failed_counter = 0
        resultData = response.json()
        statusPage = resultData[0]
        # Loop get data to all pages from 2 to length pages indicator results
        self.save2DBCountry(resultData[1])
        return [statusPage, response.from_cache]
    
    def save2DBCountry(self, data):
        table = 'wb_countries'
        countreplace = 0
        query = 'INSERT IGNORE INTO ' + table + ' (id, country_name, isocode, created_at, updated_at) VALUES '
        for r in data:
            query += '('
            query += '\'' + r['id'] + '\', '
            query += '\'' + r['name'].replace("'", "\\'") + '\', '
            query += '\'' + r['iso2Code'] + '\', '
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '

        database.cursor.execute(query.rstrip(', '))
        countreplace = countreplace + database.cursor.rowcount
        database.connection.commit()
        print(countreplace, " executing records replaced.")

    def save2DB(self, data, sourceid):
        table = 'wb_indicators'
        countreplace = 0
        query = 'INSERT IGNORE INTO ' + table + ' (indicator_id, wb_source_id, indicator_value, country, date, value, unit, `decimal`, created_at, updated_at) VALUES '
        for r in data:
            # we need create insert 
            query += '('
            query += '\'' + r['indicator']['id'] + '\', '
            query += '' + sourceid + ', '
            query += '\'' + r['indicator']['value'] + '\', '
            query += '\'' + r['country']['id'] + '\', '
            if(r['date'] != ''):
                query += '\'' + r['date'] + '\', '
            else:
                query += 'NULL, '
            if(r['value'] != '' and r['value'] != None and r['value'] != 'null'):
                query += str(r['value']) + ', '
            else:
                query += '0, '
            if(r['unit'] != ''):
                query += '\'' + r['unit'] + '\', '
            else:
                query += 'NULL, '
            if(r['decimal'] != '' and r['decimal'] != None):
                query += str(r['decimal']).replace('.','') + ', '
            else:
                query += '0, '
            
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
        
        database.cursor.execute(query.rstrip(', '))
        countreplace = countreplace + database.cursor.rowcount
        database.connection.commit()
        print(countreplace, " executing records replaced.")
    
    def save2Database(self):
        table = 'wb_indicators'
        countreplace = 0
        for response in self.results:
            # check if scrapper failed do not save to db
            if(response.status_code != 200):
                return
            
            source_id = response.json()[0]['sourceid']
            
            query = 'REPLACE INTO ' + table + ' (id, wb_source_id, indicator_value, country, date, value, unit, `decimal`, created_at, updated_at) VALUES '
            # we need create insert 
            for r in response.json()[1]:
                query += '('
                query += '\'' + r['indicator']['id'] + '\', '
                query += '' + source_id + ', '
                query += '\'' + r['indicator']['value'] + '\', '
                query += '\'' + r['country']['id'] + '\', '
                if(r['date'] != ''):
                    query += '\'' + r['date'] + '\', '
                else:
                    query += 'NULL, '
                if(r['value'] != '' and r['value'] != None and r['value'] != 'null'):
                    query += str(r['value']) + ', '
                else:
                    query += '0, '
                if(r['unit'] != ''):
                    query += '\'' + r['unit'] + '\', '
                else:
                    query += 'NULL, '
                if(r['decimal'] != '' and r['decimal'] != None):
                    query += str(r['decimal']).replace('.','') + ', '
                else:
                    query += '0, '
                
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
            
            database.cursor.execute(query.rstrip(', '))
            countreplace = countreplace + database.cursor.rowcount

        database.connection.commit()
        print(countreplace, " executing records replaced.")

    def save2MetaDatabase(self):
        countreplace = 0
        table = 'wb_meta_indicators'
        # check if scrapper failed do not save to db
        if(self.result.status_code != 200):
            return
        
        query = 'REPLACE INTO ' + table + ' (id, wb_source_id, name, created_at, updated_at) VALUES '
        # we need create insert 
        for r in self.result.json()[1]:
            query += '('
            query += '\'' + r['id'] + '\', '
            query += '' + r['source']['id'] + ', '
            query += '\'' + r['name'].replace("'","\\\'") + '\', '
            
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
        
        database.cursor.execute(query.rstrip(', '))
        countreplace = countreplace + database.cursor.rowcount

        database.connection.commit()
        print(countreplace, " executing records replaced.")
    
    def done(self):
        database.connection.close()
