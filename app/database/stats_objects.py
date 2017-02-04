from mongoengine import register_connection, Document

from app.settings import TESTING_DB, PRODUCTION_DB

register_connection(alias='test', name=TESTING_DB)
register_connection(alias='default', name=PRODUCTION_DB)

# ToDo: mongo $size can't query size of lists (see: https://docs.mongodb.com/manual/reference/operator/query/size/#_S_size)
#      workaround: "create a counter field that you increment when you add elements to a field."
# ToDo: Add language field to documents

class Users(Document):
    pass

class Pages(Document):
    pass

class PostStast(Document):
    pass

