import plyvel
db = plyvel.DB("testdb/", create_if_missing = True)

for i in range(100000):
    #print(i)
    db.put(b'key', b'value')