from datetime import date, datetime
import database

from requests import Response
from scripts import core

class engine:

    result: Response
    jsonResult = []

    def __init__(self, typeUrl='urlS', listIndicators = None) -> None:
        # use default example list indicators, if user not specify
        if listIndicators != None:
            self.listIndicators = listIndicators

        self.result = core.runSingleQuery(typeUrl)

    def getResult(self):
        return self.result
    
    def getJson(self):
        return self.result.json()
    
    def save2Database(self, table):
        # check if scrapper failed do not save to db
        if(self.result.status_code != 200):
            return
        
        query = 'REPLACE INTO ' + table + ' (id, last_updated, name, code, description, url, created_at, updated_at) VALUES '
        # we need create insert 
        for r in self.getJson()[1]:
            query += '('
            query += r['id'] + ', '
            query += '\'' + r['lastupdated'] + '\', '
            query += '\'' + r['name'] + '\', '
            query += '\'' + r['code'] + '\', '
            if(r['description'] != ''):
                query += '\'' + r['description'] + '\', '
            else:
                query += 'NULL, '
            if(r['url'] != ''):
                query += '\'' + r['url'] + '\', '
            else:
                query += 'NULL, '
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\', '
            query += '\'' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '\'), '
        
        database.cursor.execute(query.rstrip(', '))
        database.connection.commit()

        print(database.cursor.rowcount, "records inserted.")
    
    def done(self):
        database.connection.close()