from datetime import datetime

import logging

import requests

import configs
import database
from scripts import core
import pandasdmx as sdmx
from pandasdmx import message
import numpy as np


class engine:
    listIndicators = [
        'CLD_XCHL_SEX_AGE_NB',
        'EAR_4HRL_SEX_OCU_CUR_NB',
        'EAR_4MTH_SEX_DSB_CUR_NB',
        'EAR_4MTH_SEX_ECO_CUR_NB',
        'EAR_4MTH_SEX_OCU_CUR_NB',
        'INJ_FATL_ECO_NB',
        'INJ_FATL_SEX_MIG_NB',
        'INJ_NFTL_ECO_NB',
        'INJ_NFTL_SEX_MIG_NB',
        'INJ_NFTL_SEX_INJ_MIG_NB',
        'INJ_NFTL_INJ_ECO_NB',
        'CLD_TPOP_SEX_AGE_NB',
        'CLD_TPOP_SEX_AGE_GEO_NB',
        'CLD_TPOP_SEX_AGE_STU_NB',
        'CLD_XCHS_SEX_AGE_GEO_NB',
        'CLD_XCHL_SEX_AGE_ECO_NB',
        'CLD_XCHL_SEX_AGE_GEO_NB',
        'CLD_XCHL_SEX_AGE_STU_NB',
        'CLD_XCHL_SEX_AGE_STE_NB',
        'CLD_XCHD_SEX_AGE_NB',
        'CLD_XCHN_SEX_AGE_GEO_NB',
        'CLD_XSND_SEX_AGE_NB',
        'CLD_XSNS_SEX_AGE_GEO_NB',
        'CLD_XSNA_SEX_AGE_NB',
        'CLD_XSNA_SEX_AGE_ECO_NB',
        'CLD_XSNA_SEX_AGE_GEO_NB',
        'CLD_XSNA_SEX_AGE_STU_NB',
        'CLD_XSNA_SEX_AGE_STE_NB',
        'CLD_XSNN_SEX_AGE_GEO_NB',
        'CLD_XHAD_SEX_AGE_NB',
        'CLD_XHAS_SEX_AGE_GEO_NB',
        'CLD_XHAZ_SEX_AGE_NB',
        'CLD_XHAZ_SEX_AGE_ECO_NB',
        'CLD_XHAZ_SEX_AGE_GEO_NB',
        'CLD_XHAZ_SEX_AGE_STU_NB',
        'CLD_XHAZ_SEX_AGE_STE_NB',
        'CLD_XHAN_SEX_AGE_GEO_NB',
        'ILR_CBCT_NOC_RT',
        'EIP_WDIS_SEX_AGE_NB',
        'EIP_WDIS_SEX_DSB_NB',
        'EIP_WDIS_SEX_EDU_NB',
        'EIP_WDIS_SEX_MTS_NB',
        'EIP_WDIS_SEX_GEO_NB',
        'EIP_WDIS_SEX_AGE_EDU_NB',
        'EIP_WDIS_SEX_AGE_MTS_NB',
        'EIP_WDIS_SEX_AGE_GEO_NB',
        'EIP_WDIS_SEX_EDU_MTS_NB',
        'EIP_WDIS_SEX_EDU_GEO_NB',
        'EIP_WDIS_SEX_GEO_MTS_NB',
        'MST_NCTE_SEX_CCT_NB',
        'MST_FORE_SEX_CBR_NB',
        'EES_TEES_AGE_EC2_NB',
        'EES_TEES_AGE_OC2_NB',
        'EES_TEES_ECO_OCU_NB',
        'EES_TEES_SEX_AGE_NB',
        'EES_TEES_SEX_DSB_NB',
        'EES_TEES_SEX_EC2_NB',
        'EES_TEES_SEX_MTS_NB',
        'EES_TEES_SEX_MJH_NB',
        'EES_TEES_SEX_OC2_NB',
        'EES_TEES_SEX_GEO_NB',
        'EES_TEES_SEX_HOW_NB',
        'EES_TEES_SEX_AGE_DSB_NB',
        'EES_TEES_SEX_AGE_ECO_NB',
        'EES_TEES_SEX_AGE_EDU_NB',
        'EES_TEES_SEX_AGE_MTS_NB',
        'EES_TEES_SEX_AGE_OCU_NB',
        'EES_TEES_SEX_AGE_GEO_NB',
        'EES_TEES_SEX_ECO_DSB_NB',
        'EES_TEES_SEX_ECO_EDU_NB',
        'EES_TEES_SEX_ECO_EST_NB',
        'EES_TEES_SEX_ECO_MTS_NB',
        'EES_TEES_SEX_ECO_GEO_NB',
        'EES_TEES_SEX_EDU_DSB_NB',
        'EES_TEES_SEX_EDU_MTS_NB',
        'EES_TEES_SEX_EDU_GEO_NB',
        'EES_TEES_SEX_EST_DSB_NB',
        'EES_TEES_SEX_EST_MTS_NB',
        'EES_TEES_SEX_EST_GEO_NB',
        'EES_TEES_SEX_MTS_DSB_NB',
        'EES_TEES_SEX_OCU_DSB_NB',
        'EES_TEES_SEX_OCU_EDU_NB',
        'EES_TEES_SEX_OCU_EST_NB',
        'EES_TEES_SEX_OCU_MTS_NB',
        'EES_TEES_SEX_OCU_GEO_NB',
        'EES_TEES_SEX_GEO_DSB_NB',
        'EES_TEES_SEX_GEO_MTS_NB',
        'EMP_TEMP_AGE_EC2_NB',
        'EMP_TEMP_AGE_OC2_NB',
        'EMP_TEMP_ECO_OCU_NB',
        'EMP_5EMP_SEX_AGE_NB',
        'EMP_TEMP_SEX_AGE_NB',
        'EMP_TEM1_SEX_AGE_NB',
        'EMP_TEMP_SEX_DSB_NB',
        'EMP_TEMP_SEX_EC2_NB',
        'EMP_TEMP_SEX_ECO_NB',
        'EMP_TEM1_SEX_ECO_NB',
        'EMP_TEMP_SEX_EDU_NB',
        'EMP_TEMP_SEX_EST_NB',
        'EMP_TEMP_SEX_MTS_NB',
        'EMP_TEMP_SEX_MJH_NB',
        'EMP_TEMP_SEX_OC2_NB',
        'EMP_TEMP_SEX_OCU_NB',
        'EMP_TEMP_SEX_GEO_NB',
        'EMP_TEMP_SEX_STE_NB',
        'EMP_TEMP_SEX_HOW_NB',
        'MST_TEMP_SEX_AGE_CCT_NB',
        'EMP_TEMP_SEX_AGE_DSB_NB',
        'EMP_TEMP_SEX_AGE_ECO_NB',
        'EMP_TEMP_SEX_AGE_EDU_NB',
        'EMP_TEMP_SEX_AGE_MTS_NB',
        'EMP_TEMP_SEX_AGE_OCU_NB',
        'MST_TEMP_SEX_AGE_CBR_NB',
        'EMP_2EMP_SEX_AGE_GEO_NB',
        'EMP_TEMP_SEX_AGE_GEO_NB',
        'EMP_TEMP_SEX_AGE_STE_NB',
        'EMP_TEMP_SEX_AGE_JOB_NB',
        'MST_TEMP_SEX_ECO_CCT_NB',
        'EMP_TEMP_SEX_ECO_DSB_NB',
        'EMP_TEMP_SEX_ECO_EDU_NB',
        'EMP_TEMP_SEX_ECO_EST_NB',
        'EMP_TEMP_SEX_ECO_MTS_NB',
        'EMP_TEMP_SEX_ECO_MJH_NB',
        'MST_TEMP_SEX_ECO_CBR_NB',
        'EMP_TEMP_SEX_ECO_GEO_NB',
        'MST_TEMP_SEX_EDU_CCT_NB',
        'EMP_TEMP_SEX_EDU_DSB_NB',
        'EMP_TEMP_SEX_EDU_MTS_NB',
        'MST_TEMP_SEX_EDU_CBR_NB',
        'EMP_TEMP_SEX_EDU_GEO_NB',
        'EMP_TEMP_SEX_EST_DSB_NB',
        'EMP_TEMP_SEX_EST_MTS_NB',
        'EMP_TEMP_SEX_EST_GEO_NB',
        'EMP_TEMP_SEX_MTS_DSB_NB',
        'EMP_TEMP_SEX_MJH_DSB_NB',
        'EMP_TEMP_SEX_MJH_EDU_NB',
        'EMP_TEMP_SEX_MJH_MTS_NB',
        'EMP_TEMP_SEX_MJH_GEO_NB',
        'MST_TEMP_SEX_OCU_CCT_NB',
        'EMP_TEMP_SEX_OCU_DSB_NB',
        'EMP_TEMP_SEX_OCU_EDU_NB',
        'EMP_TEMP_SEX_OCU_EST_NB',
        'EMP_TEMP_SEX_OCU_MTS_NB',
        'EMP_TEMP_SEX_OCU_MJH_NB',
        'MST_TEMP_SEX_OCU_CBR_NB',
        'EMP_TEMP_SEX_OCU_GEO_NB',
        'EMP_TEMP_SEX_GEO_MTS_NB',
        'EMP_TEMP_SEX_GEO_DSB_NB',
        'EMP_2EMP_SEX_GEO_ECO_NB',
        'EMP_2EMP_SEX_GEO_OCU_NB',
        'EMP_2EMP_SEX_GEO_STE_NB',
        'EMP_NORM_SEX_STE_EDU_NB',
        'EMP_STAT_SEX_STE_EDU_NB',
        'MST_TEMP_SEX_STE_CCT_NB',
        'EMP_TEMP_SEX_STE_DSB_NB',
        'EMP_TEMP_SEX_STE_ECO_NB',
        'EMP_TEMP_SEX_STE_EDU_NB',
        'EMP_TEMP_SEX_STE_EST_NB',
        'EMP_TEMP_SEX_STE_MTS_NB',
        'EMP_TEMP_SEX_STE_MJH_NB',
        'EMP_TEMP_SEX_STE_OCU_NB',
        'MST_TEMP_SEX_STE_CBR_NB',
        'EMP_TEMP_SEX_STE_GEO_NB',
        'EMP_TEMP_SEX_HOW_DSB_NB',
        'EMP_TEMP_SEX_HOW_EDU_NB',
        'EMP_TEMP_SEX_HOW_MTS_NB',
        'EMP_TEMP_SEX_HOW_GEO_NB',
        'EMP_PIFL_AGE_EC2_NB',
        'EMP_PIFL_AGE_OC2_NB',
        'EMP_PIFL_ECO_OCU_NB',
        'EMP_PIFL_SEX_NB',
        'EMP_5PIF_SEX_AGE_NB',
        'EMP_PIFL_SEX_AGE_NB',
        'EMP_PIFL_SEX_DSB_NB',
        'EMP_PIFL_SEX_EC2_NB',
        'EMP_PIFL_SEX_ECO_NB',
        'EMP_PIFL_SEX_EDU_NB',
        'EMP_PIFL_SEX_EST_NB',
        'EMP_PIFL_SEX_MTS_NB',
        'EMP_PIFL_SEX_OC2_NB',
        'EMP_PIFL_SEX_OCU_NB',
        'EMP_PIFL_SEX_GEO_NB',
        'EMP_PIFL_SEX_STE_NB',
        'EMP_PIFL_SEX_HOW_NB',
        'EMP_PIFL_SEX_AGE_DSB_NB',
        'EMP_PIFL_SEX_AGE_ECO_NB',
        'EMP_PIFL_SEX_AGE_EDU_NB',
        'EMP_PIFL_SEX_AGE_MTS_NB',
        'EMP_PIFL_SEX_AGE_OCU_NB',
        'EMP_PIFL_SEX_AGE_GEO_NB',
        'EMP_PIFL_SEX_ECO_DSB_NB',
        'EMP_PIFL_SEX_ECO_EDU_NB',
        'EMP_PIFL_SEX_ECO_EST_NB',
        'EMP_PIFL_SEX_ECO_MTS_NB',
        'EMP_PIFL_SEX_ECO_GEO_NB',
        'EMP_PIFL_SEX_EDU_DSB_NB',
        'EMP_PIFL_SEX_EDU_MTS_NB',
        'EMP_PIFL_SEX_EDU_GEO_NB',
        'EMP_PIFL_SEX_EST_DSB_NB',
        'EMP_PIFL_SEX_EST_MTS_NB',
        'EMP_PIFL_SEX_EST_GEO_NB',
        'EMP_PIFL_SEX_MTS_DSB_NB',
        'EMP_PIFL_SEX_OCU_DSB_NB',
        'EMP_PIFL_SEX_OCU_EDU_NB',
        'EMP_PIFL_SEX_OCU_EST_NB',
        'EMP_PIFL_SEX_OCU_MTS_NB',
        'EMP_PIFL_SEX_OCU_GEO_NB',
        'EMP_PIFL_SEX_GEO_DSB_NB',
        'EMP_PIFL_SEX_GEO_MTS_NB',
        'EMP_5WAP_SEX_AGE_RT',
        'EMP_DWAP_SEX_AGE_RT',
        'EMP_DWA1_SEX_AGE_RT',
        'EMP_DWAP_SEX_DSB_RT',
        'EMP_DWAP_SEX_EDU_RT',
        'EMP_DWAP_SEX_MTS_RT',
        'EMP_DWAP_SEX_GEO_RT',
        'MST_TEMP_SEX_AGE_CCT_RT',
        'EMP_DWAP_SEX_AGE_DSB_RT',
        'EMP_DWAP_SEX_AGE_EDU_RT',
        'EMP_DWAP_SEX_AGE_MTS_RT',
        'MST_TEMP_SEX_AGE_CBR_RT',
        'EMP_2WAP_SEX_AGE_GEO_RT',
        'EMP_DWAP_SEX_AGE_GEO_RT',
        'MST_TEMP_SEX_EDU_CCT_RT',
        'EMP_DWAP_SEX_EDU_DSB_RT',
        'EMP_DWAP_SEX_EDU_MTS_RT',
        'MST_TEMP_SEX_EDU_CBR_RT',
        'EMP_DWAP_SEX_EDU_GEO_RT',
        'EMP_DWAP_SEX_MTS_DSB_RT',
        'EMP_DWAP_SEX_GEO_MTS_RT',
        'EMP_DWAP_SEX_GEO_DSB_RT',
        'INJ_FATL_ECO_RT',
        'INJ_FATL_SEX_MIG_RT',
        'EAR_XFLS_NOC_RT',
        'LAP_2FTM_NOC_RT',
        'EAR_GGAP_OCU_RT',
        'EIP_5WAP_SEX_AGE_RT',
        'EIP_DWAP_SEX_AGE_RT',
        'EIP_DWAP_SEX_DSB_RT',
        'EIP_DWAP_SEX_EDU_RT',
        'EIP_DWAP_SEX_MTS_RT',
        'EIP_DWAP_SEX_GEO_RT',
        'MST_TEIP_SEX_AGE_CCT_RT',
        'EIP_DWAP_SEX_AGE_DSB_RT',
        'EIP_DWAP_SEX_AGE_EDU_RT',
        'EIP_DWAP_SEX_AGE_MTS_RT',
        'MST_TEIP_SEX_AGE_CBR_RT',
        'EIP_2WAP_SEX_AGE_GEO_RT',
        'EIP_DWAP_SEX_AGE_GEO_RT',
        'MST_TEIP_SEX_EDU_CCT_RT',
        'EIP_DWAP_SEX_EDU_DSB_RT',
        'EIP_DWAP_SEX_EDU_MTS_RT',
        'MST_TEIP_SEX_EDU_CBR_RT',
        'EIP_DWAP_SEX_EDU_GEO_RT',
        'EIP_DWAP_SEX_MTS_DSB_RT',
        'EIP_DWAP_SEX_GEO_MTS_RT',
        'EIP_DWAP_SEX_GEO_DSB_RT',
        'EMP_PTER_SEX_RT',
        'MFL_NEMP_SEX_ECO_NB',
        'MFL_NEMP_SEX_OCU_NB',
        'MFL_FEMP_SEX_ECO_NB',
        'MFL_FEMP_SEX_OCU_NB',
        'MNA_XRET_SEX_CPR_NB',
        'MFL_NCIT_SEX_CCT_NB',
        'MFL_NWAP_SEX_EDU_NB',
        'MFL_FPOP_SEX_CBR_NB',
        'MFL_FWAP_SEX_EDU_NB',
        'EMP_NIFL_AGE_EC2_NB',
        'EMP_NIFL_AGE_OC2_NB',
        'EMP_NIFL_ECO_OCU_NB',
        'EMP_NIFL_SEX_NB',
        'EMP_5NIF_SEX_AGE_NB',
        'EMP_NIFL_SEX_AGE_NB',
        'EMP_NIFL_SEX_DSB_NB',
        'EMP_NIFL_SEX_EC2_NB',
        'EMP_NIFL_SEX_ECO_NB',
        'EMP_NIFL_SEX_EDU_NB',
        'EMP_NIFL_SEX_EST_NB',
        'EMP_NIFL_SEX_MTS_NB',
        'EMP_NIFL_SEX_OC2_NB',
        'EMP_NIFL_SEX_OCU_NB',
        'EMP_NIFL_SEX_GEO_NB',
        'EMP_NIFL_SEX_STE_NB',
        'EMP_NIFL_SEX_HOW_NB',
        'EMP_NIFL_SEX_AGE_DSB_NB',
        'EMP_NIFL_SEX_AGE_ECO_NB',
        'EMP_NIFL_SEX_AGE_EDU_NB',
        'EMP_NIFL_SEX_AGE_MTS_NB',
        'EMP_NIFL_SEX_AGE_OCU_NB',
        'EMP_NIFL_SEX_AGE_GEO_NB',
        'EMP_NIFL_SEX_ECO_DSB_NB',
        'EMP_NIFL_SEX_ECO_EDU_NB',
        'EMP_NIFL_SEX_ECO_EST_NB',
        'EMP_NIFL_SEX_ECO_MTS_NB',
        'EMP_NIFL_SEX_ECO_GEO_NB',
        'EMP_NIFL_SEX_EDU_DSB_NB',
        'EMP_NIFL_SEX_EDU_MTS_NB',
        'EMP_NIFL_SEX_EDU_GEO_NB',
        'EMP_NIFL_SEX_EST_DSB_NB',
        'EMP_NIFL_SEX_EST_MTS_NB',
        'EMP_NIFL_SEX_EST_GEO_NB',
        'EMP_NIFL_SEX_MTS_DSB_NB',
        'EMP_NIFL_SEX_OCU_DSB_NB',
        'EMP_NIFL_SEX_OCU_EDU_NB',
        'EMP_NIFL_SEX_OCU_EST_NB',
        'EMP_NIFL_SEX_OCU_MTS_NB',
        'EMP_NIFL_SEX_OCU_GEO_NB',
        'EMP_NIFL_SEX_GEO_DSB_NB',
        'EMP_NIFL_SEX_GEO_MTS_NB',
        'EMP_NIFL_AGE_EC2_RT',
        'EMP_NIFL_AGE_OC2_RT',
        'EMP_NIFL_ECO_OCU_RT',
        'EMP_NIFL_SEX_RT',
        'EMP_5NIF_SEX_AGE_RT',
        'EMP_NIFL_SEX_AGE_RT',
        'EMP_NIFL_SEX_DSB_RT',
        'EMP_NIFL_SEX_EC2_RT',
        'EMP_NIFL_SEX_ECO_RT',
        'EMP_NIFL_SEX_EDU_RT',
        'EMP_NIFL_SEX_EST_RT',
        'EMP_NIFL_SEX_MTS_RT',
        'EMP_NIFL_SEX_OC2_RT',
        'EMP_NIFL_SEX_OCU_RT',
        'EMP_NIFL_SEX_GEO_RT',
        'EMP_NIFL_SEX_HOW_RT',
        'EMP_NIFL_SEX_AGE_DSB_RT',
        'EMP_NIFL_SEX_AGE_ECO_RT',
        'EMP_NIFL_SEX_AGE_EDU_RT',
        'EMP_NIFL_SEX_AGE_MTS_RT',
        'EMP_NIFL_SEX_AGE_OCU_RT',
        'EMP_NIFL_SEX_AGE_GEO_RT',
        'EMP_NIFL_SEX_ECO_DSB_RT',
        'EMP_NIFL_SEX_ECO_EDU_RT',
        'EMP_NIFL_SEX_ECO_EST_RT',
        'EMP_NIFL_SEX_ECO_MTS_RT',
        'EMP_NIFL_SEX_ECO_GEO_RT',
        'EMP_NIFL_SEX_EDU_DSB_RT',
        'EMP_NIFL_SEX_EDU_MTS_RT',
        'EMP_NIFL_SEX_EDU_GEO_RT',
        'EMP_NIFL_SEX_EST_DSB_RT',
        'EMP_NIFL_SEX_EST_MTS_RT',
        'EMP_NIFL_SEX_EST_GEO_RT',
        'EMP_NIFL_SEX_MTS_DSB_RT',
        'EMP_NIFL_SEX_OCU_DSB_RT',
        'EMP_NIFL_SEX_OCU_EDU_RT',
        'EMP_NIFL_SEX_OCU_EST_RT',
        'EMP_NIFL_SEX_OCU_MTS_RT',
        'EMP_NIFL_SEX_OCU_GEO_RT',
        'EMP_NIFL_SEX_GEO_DSB_RT',
        'EMP_NIFL_SEX_GEO_MTS_RT',
        'EMP_NIFL_SEX_STE_RT',
        'LAI_INDE_NOC_RT',
        'POP_2LDR_NOC_RT',
        'EAP_5EAP_SEX_AGE_NB',
        'EAP_TEAP_SEX_AGE_NB',
        'EAP_TEA1_SEX_AGE_NB',
        'EAP_TEAP_SEX_DSB_NB',
        'EAP_TEAP_SEX_EDU_NB',
        'EAP_TEAP_SEX_MTS_NB',
        'EAP_TEAP_SEX_GEO_NB',
        'MST_TEAP_SEX_AGE_CCT_NB',
        'EAP_TEAP_SEX_AGE_DSB_NB',
        'EAP_TEAP_SEX_AGE_EDU_NB',
        'EAP_TEAP_SEX_AGE_MTS_NB',
        'MST_TEAP_SEX_AGE_CBR_NB',
        'EAP_2EAP_SEX_AGE_GEO_NB',
        'EAP_TEAP_SEX_AGE_GEO_NB',
        'MST_TEAP_SEX_EDU_CCT_NB',
        'EAP_TEAP_SEX_EDU_DSB_NB',
        'EAP_TEAP_SEX_EDU_MTS_NB',
        'MST_TEAP_SEX_EDU_CBR_NB',
        'EAP_TEAP_SEX_EDU_GEO_NB',
        'EAP_TEAP_SEX_MTS_DSB_NB',
        'EAP_TEAP_SEX_GEO_MTS_NB',
        'EAP_TEAP_SEX_GEO_DSB_NB',
        'EAP_5WAP_SEX_AGE_RT',
        'EAP_DWAP_SEX_AGE_RT',
        'EAP_DWA1_SEX_AGE_RT',
        'EAP_DWAP_SEX_DSB_RT',
        'EAP_DWAP_SEX_DSB_RT',
        'EAP_DWAP_SEX_EDU_RT',
        'EAP_DWAP_SEX_MTS_RT',
        'EAP_DWAP_SEX_GEO_RT',
        'MST_TEAP_SEX_AGE_CCT_RT',
        'EAP_DWAP_SEX_AGE_DSB_RT',
        'EAP_DWAP_SEX_AGE_EDU_RT',
        'EAP_DWAP_SEX_AGE_MTS_RT',
        'MST_TEAP_SEX_AGE_CBR_RT',
        'EAP_2WAP_SEX_AGE_GEO_RT',
        'EAP_DWAP_SEX_AGE_GEO_RT',
        'MST_TEAP_SEX_EDU_CCT_RT',
        'EAP_DWAP_SEX_EDU_DSB_RT',
        'EAP_DWAP_SEX_EDU_MTS_RT',
        'MST_TEAP_SEX_EDU_CBR_RT',
        'EAP_DWAP_SEX_EDU_GEO_RT',
        'EAP_DWAP_SEX_MTS_DSB_RT',
        'EAP_DWAP_SEX_GEO_MTS_RT',
        'EAP_DWAP_SEX_GEO_DSB_RT',
        'LAP_2LID_QTL_RT',
        'LAP_2GDP_NOC_RT',
        'LAI_VDIN_NOC_RT',
        'EAR_XTLP_SEX_RT',
        'LAC_4HRL_ECO_CUR_NB',
        'GED_PHOW_SEX_HHT_CHL_NB',
        'GED_PHOW_SEX_HHT_GEO_NB',
        'HOW_TEMP_AGE_EC2_NB',
        'HOW_TEMP_AGE_OC2_NB',
        'HOW_TEMP_ECO_OCU_NB',
        'HOW_TEMP_SEX_ECO_NB',
        'HOW_TEMP_SEX_EC2_NB',
        'HOW_TEMP_SEX_EST_NB',
        'HOW_TEMP_SEX_OCU_NB',
        'HOW_TEMP_SEX_OC2_NB',
        'HOW_TEMP_SEX_AGE_ECO_NB',
        'HOW_TEMP_SEX_AGE_OCU_NB',
        'HOW_TEMP_SEX_ECO_DSB_NB',
        'HOW_TEMP_SEX_ECO_EDU_NB',
        'HOW_TEMP_SEX_ECO_EST_NB',
        'HOW_TEMP_SEX_ECO_MTS_NB',
        'HOW_TEMP_SEX_ECO_GEO_NB',
        'HOW_TEMP_SEX_EST_DSB_NB',
        'HOW_TEMP_SEX_EST_MTS_NB',
        'HOW_TEMP_SEX_EST_GEO_NB',
        'HOW_TEMP_SEX_OCU_DSB_NB',
        'HOW_TEMP_SEX_OCU_EDU_NB',
        'HOW_TEMP_SEX_OCU_EST_NB',
        'HOW_TEMP_SEX_OCU_MTS_NB',
        'HOW_TEMP_SEX_OCU_GEO_NB',
        'HOW_XEES_AGE_EC2_NB',
        'HOW_XEES_AGE_OC2_NB',
        'HOW_XEES_ECO_OCU_NB',
        'HOW_XEES_SEX_ECO_NB',
        'HOW_XEES_SEX_EC2_NB',
        'HOW_XEES_SEX_EST_NB',
        'HOW_XEES_SEX_OCU_NB',
        'HOW_XEES_SEX_OC2_NB',
        'HOW_XEES_SEX_AGE_ECO_NB',
        'HOW_XEES_SEX_AGE_OCU_NB',
        'HOW_XEES_SEX_ECO_DSB_NB',
        'HOW_XEES_SEX_ECO_EDU_NB',
        'HOW_XEES_SEX_ECO_EST_NB',
        'HOW_XEES_SEX_ECO_MTS_NB',
        'HOW_XEES_SEX_ECO_GEO_NB',
        'HOW_XEES_SEX_EST_DSB_NB',
        'HOW_XEES_SEX_EST_MTS_NB',
        'HOW_XEES_SEX_EST_GEO_NB',
        'HOW_XEES_SEX_OCU_DSB_NB',
        'HOW_XEES_SEX_OCU_EDU_NB',
        'HOW_XEES_SEX_OCU_EST_NB',
        'HOW_XEES_SEX_OCU_MTS_NB',
        'HOW_XEES_SEX_OCU_GEO_NB',
        'HOW_UEMP_SEX_NB',
        'HOW_UEES_SEX_NB',
        'CPI_NWGT_COI_RT',
        'CPI_NCPD_COI_RT',
        'CPI_NCYR_COI_RT',
        'CPI_ACPI_COI_RT',
        'CPI_MCPI_COI_RT',
        'INJ_NFTL_ECO_RT',
        'INJ_NFTL_SEX_MIG_RT',
        'LAI_VIST_NOC_NB',
        'LAI_INSP_SEX_NB',
        'STR_TSTR_ECO_NB',
        'FOW_TVOL_AGE_VOL_NB',
        'FOW_TVOL_SEX_VOL_NB',
        'MNA_OPOP_SEX_CDS_NB',
        'MNA_OEMP_SEX_CDS_NB',
        'MNA_OEMP_SEX_ECO_NB',
        'MNA_OEMP_SEX_EDU_NB',
        'MNA_OEMP_SEX_OCU_NB',
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
                data = message.to_pandas()
                # print(data)
                query = 'REPLACE INTO ' + table + ' (indicator_id, country_id, frequency_id, measure_id, dimensions, dimensions_id, time_periode, value, created_at, updated_at) VALUES '
                for key, row in data.items():
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
                    query += '\''+ str(row).replace("'", "\\'") +'\', '
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
