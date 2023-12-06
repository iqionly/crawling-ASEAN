from unittest import result

from requests import Response
from scripts import core

class engine:
    listIndicators = [
        'SL.TLF.0714.FE.ZS',
        'SL.TLF.0714.MA.ZS',
        'SL.TLF.0714.ZS',
        'SL.TLF.CACT.FE.ZS',
        'SL.TLF.CACT.MA.ZS'
    ]

    result: list[Response] = []
    jsonResult = []

    def __init__(self, typeUrl='urlCI') -> None:
        self.result = core.runScrapIndicators(typeUrl, self.listIndicators)

    def getResult(self):
        return self.result
    
    def getJson(self):
        for r in self.result:
            self.jsonResult.append(r.json())
        return self.jsonResult