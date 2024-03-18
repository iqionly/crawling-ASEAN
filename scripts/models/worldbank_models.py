import configs
import database
from scripts.models.model import Builder

table_indicator = 'wb_indicators'
table_meta_indicators = 'wb_meta_indicators'
table_source = 'wb_sources'

def getMetaIndicators(limit=10,page=1):
    result = Builder(table_meta_indicators).query(limit, page).withTrashed().get()
    for (id, name, wb_source_id, created_at, updated_at, deleted_at) in result:
        print(id)
    exit()  
    database.cursor.execute(query)

def done():
    database.connection.close()