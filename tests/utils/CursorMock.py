class CursorMock:
    used = False
    def fetchone(self):
        return [{'name':'test'}]
    
    def fetchmany(self, size=0):
        if self.used:
            return []
        list = []
        for i in range(0,size):
            list.append({'id':i})
        self.used = True
        return list