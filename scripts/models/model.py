import database
class Builder:
    sQuery = ''
    sLimit = ''
    sWhere = ''
    sTrash = False
    sTable = ''
    def __init__(self, table) -> None:
        self.sTable = table
        pass
    def query(self, limit=10, page=1):
        offset = ((page - 1) * limit) + 1
        take = offset + (limit - 1)

        self.sQuery = 'SELECT * FROM ' + self.sTable + ' '
        self.sLimit = 'LIMIT ' + str(offset) + ', ' + str(take) + ' '

        return self
    def withTrashed(self):
        self.sTrash = True
        return self
    
    def get(self): 
        if(self.sTrash == False):
            if(self.sWhere == ''):
                self.sWhere = 'WHERE deleted_at IS NULL'
            else:
                self.sWhere += 'AND deleted_at IS NULL'

        database.cursor.execute(self.sQuery + self.sWhere + self.sLimit)
        return database.cursor
