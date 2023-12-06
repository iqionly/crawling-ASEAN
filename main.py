from scripts import worldbank_indicator_scraper

wbi = worldbank_indicator_scraper.engine()
print(wbi.getJson())
