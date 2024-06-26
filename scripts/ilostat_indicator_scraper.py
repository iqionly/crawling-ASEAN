
from datetime import datetime

import logging


import requests

import configs
import database
from scripts import core
import pandasdmx as sdmx
from pandasdmx import message
import numpy as np
import sys


class engine:
    listIndicators = [
        'EIP_5EIP_SEX_AGE_NB',
        'EIP_TEIP_SEX_AGE_NB',
        'EIP_TEIP_SEX_DSB_NB',
        'EIP_TEIP_SEX_EDU_NB',
        'EIP_TEIP_SEX_MTS_NB',
        'EIP_TEIP_SEX_GEO_NB',
        'EIP_TEIP_SEX_AGE_DSB_NB',
        'EIP_TEIP_SEX_AGE_EDU_NB',
        'EIP_TEIP_SEX_AGE_MTS_NB',
        'EIP_TEIP_SEX_AGE_GEO_NB',
        'EIP_TEIP_SEX_EDU_DSB_NB',
        'EIP_TEIP_SEX_EDU_MTS_NB',
        'EIP_TEIP_SEX_EDU_GEO_NB',
        'EIP_TEIP_SEX_MTS_DSB_NB',
        'EIP_TEIP_SEX_GEO_MTS_NB',
        'EIP_TEIP_SEX_GEO_DSB_NB',
        'POP_2POP_GEO_NB',
        'POP_2POP_SEX_AGE_NB',
        'POP_2POP_SEX_AGE_GEO_NB',
        'EIP_5PLF_SEX_AGE_NB',
        'EIP_2PLF_SEX_AGE_NB',
        'EIP_WPLF_SEX_AGE_NB',
        'EIP_WPLF_SEX_DSB_NB',
        'EIP_WPLF_SEX_EDU_NB',
        'EIP_WPLF_SEX_MTS_NB',
        'EIP_WPLF_SEX_GEO_NB',
        'EIP_WPLF_SEX_AGE_EDU_NB',
        'EIP_WPLF_SEX_AGE_MTS_NB',
        'EIP_2PLF_SEX_AGE_GEO_NB',
        'EIP_WPLF_SEX_AGE_GEO_NB',
        'EIP_WPLF_SEX_EDU_MTS_NB',
        'EIP_WPLF_SEX_EDU_GEO_NB',
        'EIP_WPLF_SEX_GEO_MTS_NB',
        'EIP_2PLF_SEX_AGE_RT',
        'GED_XLU2_SEX_HHT_CHL_RT',
        'GED_XLU2_SEX_HHT_GEO_RT',
        'GED_XLU3_SEX_HHT_CHL_RT',
        'GED_XLU3_SEX_HHT_GEO_RT',
        'GED_XLU4_SEX_HHT_CHL_RT',
        'GED_XLU4_SEX_HHT_GEO_RT',
        'GED_PEPR_SEX_HHT_CHL_RT',
        'GED_PEPR_SEX_HHT_GEO_RT',
        'GED_2LFP_SEX_NB',
        'GED_PLFP_SEX_HHT_CHL_RT',
        'GED_PLFP_SEX_HHT_GEO_RT',
        'GED_2LFP_SEX_RT',
        'GED_TWAP_HHT_NB',
        'GED_XLU1_SEX_HHT_CHL_RT',
        'GED_XLU1_SEX_HHT_GEO_RT',
        'PSE_TPSE_GOV_NB',
        'LAI_WOPL_NOC_NB',
        'SDG_0131_SEX_SOC_RT',
        'SDG_1041_NOC_RT',
        'SDG_T552_NOC_RT',
        'SDG_0552_NOC_RT',
        'SDG_A821_NOC_RT',
        'SDG_B821_NOC_RT',
        'SDG_0831_SEX_ECO_RT',
        'SDG_0852_SEX_AGE_RT',
        'SDG_0852_SEX_DSB_RT',
        'SDG_0861_SEX_RT',
        'SDG_A871_SEX_AGE_RT',
        'SDG_B871_SEX_AGE_RT',
        'SDG_F881_SEX_MIG_RT',
        'SDG_N881_SEX_MIG_RT',
        'SDG_0882_NOC_RT',
        'SDG_0922_NOC_RT',
        'CLD_XSNA_SEX_AGE_RT',
        'CLD_XSNA_SEX_AGE_GEO_RT',
        'CLD_XSNA_SEX_AGE_STU_RT',
        'CLD_XCHL_SEX_AGE_RT',
        'CLD_XCHL_SEX_AGE_GEO_RT',
        'CLD_XCHL_SEX_AGE_STU_RT',
        'CLD_XHAZ_SEX_AGE_RT',
        'CLD_XHAZ_SEX_AGE_GEO_RT',
        'CLD_XHAZ_SEX_AGE_STU_RT',
        'EMP_PIFL_AGE_EC2_RT',
        'EMP_PIFL_AGE_OC2_RT',
        'EMP_PIFL_ECO_OCU_RT',
        'EMP_PIFL_SEX_RT',
        'EMP_5PIF_SEX_AGE_RT',
        'EMP_PIFL_SEX_AGE_RT',
        'EMP_PIFL_SEX_DSB_RT',
        'EMP_PIFL_SEX_EC2_RT',
        'EMP_PIFL_SEX_ECO_RT',
        'EMP_PIFL_SEX_EDU_RT',
        'EMP_PIFL_SEX_EST_RT',
        'EMP_PIFL_SEX_MTS_RT',
        'EMP_PIFL_SEX_OC2_RT',
        'EMP_PIFL_SEX_OCU_RT',
        'EMP_PIFL_SEX_GEO_RT',
        'EMP_PIFL_SEX_HOW_RT',
        'EMP_PIFL_SEX_AGE_DSB_RT',
        'EMP_PIFL_SEX_AGE_ECO_RT',
        'EMP_PIFL_SEX_AGE_EDU_RT',
        'EMP_PIFL_SEX_AGE_MTS_RT',
        'EMP_PIFL_SEX_AGE_OCU_RT',
        'EMP_PIFL_SEX_AGE_GEO_RT',
        'EMP_PIFL_SEX_ECO_DSB_RT',
        'EMP_PIFL_SEX_ECO_EDU_RT',
        'EMP_PIFL_SEX_ECO_EST_RT',
        'EMP_PIFL_SEX_ECO_MTS_RT',
        'EMP_PIFL_SEX_ECO_GEO_RT',
        'EMP_PIFL_SEX_EDU_DSB_RT',
        'EMP_PIFL_SEX_EDU_MTS_RT',
        'EMP_PIFL_SEX_EDU_GEO_RT',
        'EMP_PIFL_SEX_EST_DSB_RT',
        'EMP_PIFL_SEX_EST_MTS_RT',
        'EMP_PIFL_SEX_EST_GEO_RT',
        'EMP_PIFL_SEX_MTS_DSB_RT',
        'EMP_PIFL_SEX_OCU_DSB_RT',
        'EMP_PIFL_SEX_OCU_EDU_RT',
        'EMP_PIFL_SEX_OCU_EST_RT',
        'EMP_PIFL_SEX_OCU_MTS_RT',
        'EMP_PIFL_SEX_OCU_GEO_RT',
        'EMP_PIFL_SEX_GEO_DSB_RT',
        'EMP_PIFL_SEX_GEO_MTS_RT',
        'EMP_PIFL_SEX_STE_RT',
        'EES_XTMP_SEX_RT',
        'EIP_NEET_SEX_RT',
        'EIP_NEET_SEX_AGE_RT',
        'EIP_NEET_SEX_CCT_RT',
        'EIP_NEET_SEX_DSB_RT',
        'EIP_NEET_SEX_EDU_RT',
        'EIP_NEET_SEX_MTS_RT',
        'EIP_NEET_SEX_CBR_RT',
        'EIP_NEET_SEX_GEO_RT',
        'EIP_2EET_SEX_GEO_RT',
        'MNA_TPOP_SEX_CRS_NB',
        'FOW_5SFP_SEX_NB',
        'FOW_5SFP_SEX_RT',
        'TRU_5TRU_SEX_AGE_NB',
        'EMP_2TRU_SEX_AGE_NB',
        'TRU_TTRU_SEX_AGE_EDU_NB',
        'EMP_2TRU_SEX_AGE_GEO_NB',
        'TRU_5EMP_SEX_AGE_RT',
        'EMP_2TRU_SEX_AGE_RT',
        'TRU_DEMP_SEX_AGE_RT',
        'TRU_DEMP_SEX_DSB_RT',
        'TRU_DEMP_SEX_EDU_RT',
        'TRU_DEMP_SEX_MTS_RT',
        'TRU_DEMP_SEX_GEO_RT',
        'TRU_DEMP_SEX_AGE_EDU_RT',
        'TRU_DEMP_SEX_AGE_MTS_RT',
        'TRU_DEMP_SEX_AGE_GEO_RT',
        'TRU_DEMP_SEX_EDU_MTS_RT',
        'TRU_DEMP_SEX_EDU_GEO_RT',
        'TRU_DEMP_SEX_GEO_MTS_RT',
        'ILR_TUMT_NOC_RT',
        'MST_TUNE_SEX_AGE_CCT_NB',
        'MST_TUNE_SEX_AGE_CBR_NB',
        'MST_TUNE_SEX_EDU_CCT_NB',
        'MST_TUNE_SEX_EDU_CBR_NB',
        'MST_TUNE_SEX_AGE_CCT_RT',
        'MST_TUNE_SEX_AGE_CBR_RT',
        'MST_TUNE_SEX_EDU_CCT_RT',
        'MST_TUNE_SEX_EDU_CBR_RT',
        'FOW_TVOL_AGE_VOL_RT',
        'FOW_TVOL_SEX_VOL_RT',
        'STR_WORK_ECO_NB',
        'MST_NCTP_SEX_CCT_NB',
        'HOW_2LSS_SEX_RT',
        'MST_FORP_SEX_CBR_NB',
        'POP_5WAP_SEX_AGE_NB',
        'POP_XWAP_SEX_AGE_NB',
        'POP_XWAP_SEX_DSB_NB',
        'POP_XWAP_SEX_EDU_NB',
        'POP_XWAP_SEX_LMS_NB',
        'POP_XWAP_SEX_MTS_NB',
        'POP_XWAP_SEX_GEO_NB',
        'MST_XWAP_SEX_AGE_CCT_NB',
        'POP_XWAP_SEX_AGE_DSB_NB',
        'POP_XWAP_SEX_AGE_EDU_NB',
        'POP_XWAP_SEX_AGE_LMS_NB',
        'POP_XWAP_SEX_AGE_MTS_NB',
        'MST_XWAP_SEX_AGE_CBR_NB',
        'POP_XWAP_SEX_AGE_GEO_NB',
        'POP_XWAP_SEX_DSB_LMS_NB',
        'MST_XWAP_SEX_EDU_CCT_NB',
        'POP_XWAP_SEX_EDU_DSB_NB',
        'POP_XWAP_SEX_EDU_LMS_NB',
        'POP_XWAP_SEX_EDU_MTS_NB',
        'MST_XWAP_SEX_EDU_CBR_NB',
        'POP_XWAP_SEX_EDU_GEO_NB',
        'POP_XWAP_SEX_MTS_DSB_NB',
        'POP_XWAP_SEX_MTS_LMS_NB',
        'POP_XWAP_SEX_GEO_MTS_NB',
        'POP_XWAP_SEX_GEO_DSB_NB',
        'POP_XWAP_SEX_GEO_LMS_NB',
        'EIP_3DIS_SEX_AGE_GEO_NB',
        'EES_3EES_SEX_AGE_JOB_NB',
        'EMP_3EMP_SEX_AGE_DSB_NB',
        'EMP_3EMP_SEX_AGE_ECO_NB',
        'EMP_3EMP_SEX_AGE_EDU_NB',
        'EMP_3EMP_SEX_AGE_OCU_NB',
        'EMP_3EMP_SEX_AGE_GEO_NB',
        'EMP_3EMP_SEX_AGE_STU_NB',
        'EMP_3EMP_SEX_AGE_STE_NB',
        'EMP_3EMP_SEX_AGE_HOW_NB',
        'EMP_3EMP_SEX_AGE_JOB_NB',
        'EMP_3WAP_SEX_AGE_DSB_RT',
        'EMP_3WAP_SEX_AGE_EDU_RT',
        'EMP_3WAP_SEX_AGE_GEO_RT',
        'EMP_3WAP_SEX_AGE_STU_RT',
        'EIP_3WAP_SEX_AGE_DSB_RT',
        'EIP_3WAP_SEX_AGE_EDU_RT',
        'EIP_3WAP_SEX_AGE_GEO_RT',
        'EIP_3WAP_SEX_AGE_STU_RT',
        'EAP_3EAP_SEX_AGE_DSB_NB',
        'EAP_3EAP_SEX_AGE_EDU_NB',
        'EAP_3EAP_SEX_AGE_GEO_NB',
        'EAP_3EAP_SEX_AGE_STU_NB',
        'EAP_3WAP_SEX_AGE_DSB_RT',
        'EAP_3WAP_SEX_AGE_EDU_RT',
        'EAP_3WAP_SEX_AGE_GEO_RT',
        'EAP_3WAP_SEX_AGE_STU_RT',
        'EIP_NEET_SEX_NB',
        'EIP_NEET_SEX_AGE_NB',
        'EIP_NEET_SEX_CCT_NB',
        'EIP_NEET_SEX_DSB_NB',
        'EIP_NEET_SEX_EDU_NB',
        'EIP_NEET_SEX_MTS_NB',
        'EIP_NEET_SEX_CBR_NB',
        'EIP_2EET_SEX_GEO_NB',
        'EIP_NEET_SEX_GEO_NB',
        'EIP_3EIP_SEX_AGE_DSB_NB',
        'EIP_3EIP_SEX_AGE_EDU_NB',
        'EIP_3EIP_SEX_AGE_GEO_NB',
        'EIP_3EIP_SEX_AGE_STU_NB',
        'TRU_3TRU_SEX_AGE_GEO_NB',
        'POP_3TED_SEX_ECO_NB',
        'POP_3TED_SEX_OCU_NB',
        'POP_3TED_SEX_STE_NB',
        'POP_3WAP_SEX_AGE_DSB_NB',
        'POP_3WAP_SEX_AGE_EDU_NB',
        'POP_3FOR_SEX_AGE_TRA_NB',
        'POP_3WAP_SEX_AGE_LMS_NB',
        'POP_3WAP_SEX_AGE_GEO_NB',
        'POP_3WAP_SEX_AGE_STU_NB',
        'POP_3STG_SEX_AGE_TRA_NB',
        'POP_3FOR_SEX_EDU_TRA_NB',
        'POP_3STG_SEX_EDU_TRA_NB',
        'POP_3FOR_SEX_GEO_TRA_NB',
        'POP_3STG_SEX_GEO_TRA_NB'
    ]

    def __init__(self) -> None:
        self.ilo = sdmx.Request(
            'ILO',
            backend='sqlite',
            fast_save=True,
            expire_after=3600,
        )
        
        # metadata = self.ilo.datastructure('EAR_4HRL_SEX_OCU_CUR_NB')
        # # print(metadata) 
        # print(sdmx.to_pandas(metadata.codelist))
        pass

    def getCountryList(self):
        table = 'ilo_countries'
        message = self.ilo.codelist('CL_AREA')
        # print(codelist)
        # print('=====')
        if message.response is not None:
            if message.response.status_code != 200:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, url: ' + message.response.url)
                return
            data = sdmx.to_pandas(message.codelist)
            query = 'REPLACE INTO ' + table + ' (id, name, created_at, updated_at) VALUES '
            for key, row in data[0].items():
                query += '('
                query += '\''+ key +'\', '
                query += '\''+ row.replace("'", "\\'") +'\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
            database.cursor.execute(query.rstrip(', '))
            database.connection.commit()
        else:
            print('ERROR!')
            FORMAT = '%(asctime)s | %(message)s'
            logging.basicConfig(format=FORMAT)
            logger = logging.getLogger('failed_jobs')
            logger.warning('Failed Fetched Data: %s', 'ILOSTAT, Cannot fetch response url')
            return
        
    def getFrequencyList(self):
        table = 'ilo_frequencies'
        message = self.ilo.codelist('CL_FREQ')  
        # print(codelist)
        # print('=====')
        if message.response is not None:
            if message.response.status_code != 200:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, url: ' + message.response.url)
                return
            data = sdmx.to_pandas(message.codelist)
            query = 'REPLACE INTO ' + table + ' (id, name, created_at, updated_at) VALUES '
            for key, row in data[0].items():
                query += '('
                query += '\''+ key +'\', '
                query += '\''+ row.replace("'", "\\'") +'\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
            database.cursor.execute(query.rstrip(', '))
            database.connection.commit()
        else:
            print('ERROR!')
            FORMAT = '%(asctime)s | %(message)s'
            logging.basicConfig(format=FORMAT)
            logger = logging.getLogger('failed_jobs')
            logger.warning('Failed Fetched Data: %s', 'ILOSTAT, Cannot fetch response url')
            return
        
    def getMeasuresList(self):
        table = 'ilo_measures'
        message = self.ilo.codelist('CL_MEASURE')  
        # print(codelist)
        # print('=====')
        if message.response is not None:
            if message.response.status_code != 200:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, url: ' + message.response.url)
                return
            data = sdmx.to_pandas(message.codelist)
            query = 'REPLACE INTO ' + table + ' (id, name, created_at, updated_at) VALUES '
            for key, row in data[0].items():
                query += '('
                query += '\''+ key +'\', '
                query += '\''+ row.replace("'", "\\'") +'\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
            database.cursor.execute(query.rstrip(', '))
            database.connection.commit()
        else:
            print('ERROR!')
            FORMAT = '%(asctime)s | %(message)s'
            logging.basicConfig(format=FORMAT)
            logger = logging.getLogger('failed_jobs')
            logger.warning('Failed Fetched Data: %s', 'ILOSTAT, Cannot fetch response url')
            return
        
    def getAgesList(self):
        table = 'ilo_ages'
        message = self.ilo.codelist('CL_AGE')  
        # print(codelist)
        # print('=====')
        if message.response is not None:
            if message.response.status_code != 200:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, url: ' + message.response.url)
                return
            data = sdmx.to_pandas(message.codelist)
            query = 'REPLACE INTO ' + table + ' (id, name, created_at, updated_at, parent_id) VALUES '
            query_array = {}
            for key, row in data[0].items():
                for key1, row1 in row.items():
                    if(key != 'parent'):
                        result_query = '('
                        result_query += '\''+ key1 +'\', '
                        result_query += '\''+ row1.replace("'", "\\'") +'\', '
                        result_query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                        result_query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                        query_array[key1] = result_query
                    else:
                        if row1 == '':
                            query_array[key1] += 'NULL), '
                        else:
                            query_array[key1] += '\'' + row1 + '\'), '
            database.cursor.execute(query + ''.join(str(v) for v in query_array.values()).rstrip(', '))
            database.connection.commit()
        else:
            print('ERROR!')
            FORMAT = '%(asctime)s | %(message)s'
            logging.basicConfig(format=FORMAT)
            logger = logging.getLogger('failed_jobs')
            logger.warning('Failed Fetched Data: %s', 'ILOSTAT, Cannot fetch response url')
            return

    def getIndicatorList(self):
        table = 'ilo_indicators'
        message = self.ilo.codelist('CL_INDICATOR')  
        # print(codelist)
        # print('=====')
        if message.response is not None:
            if message.response.status_code != 200:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, url: ' + message.response.url)
                return
            data = sdmx.to_pandas(message.codelist)
            query = 'REPLACE INTO ' + table + ' (id, name, created_at, updated_at) VALUES '
            for key, row in data[0].items():
                query += '('
                query += '\''+ key +'\', '
                query += '\''+ row.replace("'", "\\'") +'\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
            database.cursor.execute(query.rstrip(', '))
            database.connection.commit()
        else:
            print('ERROR!')
            FORMAT = '%(asctime)s | %(message)s'
            logging.basicConfig(format=FORMAT)
            logger = logging.getLogger('failed_jobs')
            logger.warning('Failed Fetched Data: %s', 'ILOSTAT, Cannot fetch response url')
            return
        
    def getIndicatorData(self):        
        table = 'ilo_data_indicators'
        # for indicator in self.listIndicators:
        # print(self.ilo)
        for indicate in self.listIndicators:
            try:
                message = self.ilo.data('ILO,' + indicate + '/IDN+BRN+KHM+LAO+MYS+MMR+PHL+SGP+THA+VNM.....', params={'startPeriod': '2012-01'})
            except requests.exceptions.HTTPError:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, indicator: ' + indicate)
                continue
            except requests.exceptions.ConnectionError:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, indicator: ' + indicate)
                continue

            if message.response is not None:
                if message.response.status_code != 200:
                    print('ERROR! Maybe webservice is down! try again later...')
                    FORMAT = '%(asctime)s | %(message)s'
                    logging.basicConfig(format=FORMAT)
                    logger = logging.getLogger('failed_jobs')
                    logger.warning('Failed Fetched Data: %s', 'ILOSTAT, url: ' + message.response.url)
                    continue
                # get dimension data
                dimension = ''

                
                for q in range(4, len(message.structure.dimensions.components)):
                    dimension += message.structure.dimensions.components[q].id + ';'
                    dimension = dimension.rstrip(';')
                

                data   = message.to_pandas() 
                query = 'REPLACE INTO ' + table + ' (indicator_id, country_id, frequency_id, measure_id, dimensions, dimensions_id, time_periode, value, created_at, updated_at, source) VALUES '
                
                start = -1
                for key , row in data.items():
                    source = ''
                    start  = start + 1
                    source = message.data[0].obs[start].attached_attribute.SOURCE
                    get_value = ''
                    nilai_indikator = ''
                    contoh = 0.0
                    get_value = str(row).replace("'", "\\'")
                    if(str(row).replace("'", "\\'") == 'nan'):
                        nilai_indikator  = str(contoh).replace("'", "\\'")
                    else:
                        nilai_indikator  = get_value

                    source = str(source).replace("'", "\\'")
                    
                    query += '('
                    query += '\''+ indicate +'\', '
                    query += '\''+ key[0] +'\', '
                    query += '\''+ key[1] +'\', '
                    query += '\''+ key[2] +'\', '
                    query += '\''+ dimension +'\', '
                    dimensions_id = '{'
                    for p in range(3, len(key)-1):
                        dimensions_id += '\"'+ key[p] +'\",'
                    query += '\''+ dimensions_id.rstrip(',') +'}\', '
                    query += '\''+ key[len(key)-1] +'\', '
                    query += '\''+ nilai_indikator  +'\', '
                    query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                    query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                    query += '\''+ source +'\'), '
                    
                database.cursor.execute(query.rstrip(', '))
                database.connection.commit()
                print('Execute Query: ' + indicate)
            else:
                print('ERROR!')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, Cannot fetch response url')
                continue


        def getMetadata(self):
        
            table = 'ilo_data_indicators'
        for indicate in self.listIndicators:
            try:
                message = self.ilo.data('ILO,' + indicate + '/IDN+BRN+KHM+LAO+MYS+MMR+PHL+SGP+THA+VNM.....', params={'startPeriod': '2012-01'})
            except requests.exceptions.HTTPError:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, indicator: ' + indicate)
                continue
            except requests.exceptions.ConnectionError:
                print('ERROR! Maybe webservice is down! try again later...')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, indicator: ' + indicate)
                continue

            if message.response is not None:
                if message.response.status_code != 200:
                    print('ERROR! Maybe webservice is down! try again later...')
                    FORMAT = '%(asctime)s | %(message)s'
                    logging.basicConfig(format=FORMAT)
                    logger = logging.getLogger('failed_jobs')
                    logger.warning('Failed Fetched Data: %s', 'ILOSTAT, url: ' + message.response.url)
                    continue
                # get dimension data
                dimension = ''
                for q in range(4, len(message.structure.dimensions.components)):
                    dimension += message.structure.dimensions.components[q].id + ';'
                dimension = dimension.rstrip(';')
                data = message.to_pandas()
                query = 'REPLACE INTO ' + table + ' (indicator_id, country_id, frequency_id, measure_id, dimensions, dimensions_id, time_periode, value, created_at, updated_at) VALUES '
                
                for key, row in data.items():

                    get_value = ''
                    nilai_indikator = ''
                    contoh = 0.0
                    get_value = str(row).replace("'", "\\'")
                    if(str(row).replace("'", "\\'") == 'nan'):
                        nilai_indikator  = str(contoh).replace("'", "\\'")
                    else:
                        nilai_indikator  = get_value

                    query += '('
                    query += '\''+ indicate +'\', '
                    query += '\''+ key[0] +'\', '
                    query += '\''+ key[1] +'\', '
                    query += '\''+ key[2] +'\', '
                    query += '\''+ dimension +'\', '
                    dimensions_id = '{'
                    for p in range(3, len(key)-1):
                        dimensions_id += '\"'+ key[p] +'\",'
                    query += '\''+ dimensions_id.rstrip(',') +'}\', '
                    query += '\''+ key[len(key)-1] +'\', '
                    query += '\''+ nilai_indikator  +'\', '
                    query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
                    query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
                database.cursor.execute(query.rstrip(', '))
                database.connection.commit()
                print('Execute Query: ' + indicate)
            else:
                print('ERROR!')
                FORMAT = '%(asctime)s | %(message)s'
                logging.basicConfig(format=FORMAT)
                logger = logging.getLogger('failed_jobs')
                logger.warning('Failed Fetched Data: %s', 'ILOSTAT, Cannot fetch response url')
                continue
    def die(message):
        sys.exit(message)
            
