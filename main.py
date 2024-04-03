from configs import urls


from sys import argv

parameter = 'ilo'
if(len(argv) > 1):
   parameter = argv[1]

# world bank
# from scripts.models import worldbank_models

# wbs = worldbank_sources_scraper.engine()
# wbs.save2Database('wb_sources')

# wbi = worldbank_indicator_scraper.engine()
# wbi.save2Database()

# TODO need to create management cron

# ilo.getCountryList()
# ilo.getFrequencyList()
# ilo.getMeasuresList()
# ilo.getAgesList()
# ilo.getIndicatorList()

def my_function(param):
  
    if(param == 'ilo'):
        from scripts import ilostat_indicator_scraper
        ilo = ilostat_indicator_scraper.engine()
        ilo.getIndicatorData()
    if(param == 'wb'):
        from scripts import worldbank_indicator_scraper, worldbank_sources_scraper
        from scripts.models import worldbank_models
        worldbank_models.getMetaIndicators()

        wbi = worldbank_indicator_scraper.engine('urlCI', single=True)
        wbi.scrapIndicatorsAutoAllPages()
        wbi.scrapCountryAutoAllPages()

        for i in range(1,31):
         wbi = worldbank_indicator_scraper.engine(urls.wbHost + '/source/2/indicators?format=json&page=' + str(i), page=i, single=True)
         wbi.save2Database('wb_indicators')
         wbi.save2MetaDatabase()

my_function(parameter)

# worldbank_models.getMetaIndicators()
# worldbank_models.done()
# wbi = worldbank_indicator_scraper.engine('urlCI', single=True)
# wbi.scrapIndicatorsAutoAllPages()
# wbi.scrapCountryAutoAllPages()

# for i in range(1,31):
#     wbi = worldbank_indicator_scraper.engine(urls.wbHost + '/source/2/indicators?format=json&page=' + str(i), page=i, single=True)
#     # wbi.save2Database('wb_indicators')
#     wbi.save2MetaDatabase()


