wbHost = 'http://api.worldbank.org/v2'
iloHost = 'https://www.ilo.org/sdmx-test/rest'
schemeType = '/ILO'
iloListIndicatorPath = iloHost + '/codelist/ILO/CL_INDICATOR'
iloIndicatorPath = (iloHost + '/data' + schemeType) + ",{indicator}/{filter}"
indicatorPath = '/indicator' # get all indicator with their id, we need update eventually several times in week or months
countryPath = '/country'
sourcesPath = '/sources'
urlS = wbHost + sourcesPath
urlC = wbHost + countryPath + '/{country}'
urlI = wbHost + indicatorPath + '/{indicator}'
urlCI = urlC + indicatorPath + '/{indicator}'
exampleIndicator = urlI.format(indicator='NY.GDP.MKTP.CD')
exampleIndicatorID = wbHost + countryPath + '/id' + indicatorPath + '/NY.GDP.MKTP.CD'
# get data by indicators year and format json 