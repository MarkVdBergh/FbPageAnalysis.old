import pprint

from mongoengine import EmbeddedDocument, StringField, DictField, BooleanField, URLField, DynamicEmbeddedDocument, IntField, EmbeddedDocumentField, \
    EmbeddedDocumentListField, DynamicDocument, ObjectIdField, register_connection, Document
from profilehooks import timecall

from app.settings import TESTING, TESTING_DB, PRODUCTION_DB

register_connection(alias='test', name=TESTING_DB)
register_connection(alias='default', name=PRODUCTION_DB)

# ToDo: mongo $size can't query size of lists (see: https://docs.mongodb.com/manual/reference/operator/query/size/#_S_size)
#      workaround: "create a counter field that you increment when you add elements to a field."
# ToDo: Add language field to documents


"""
    Get a set of posts via FbPosts. Iterate each post and save processed data to the appropriate collections.

    Example queries:
        - most active users for a page
        - top 10 post with most reactions, shares, comments
        - all text, messages, comments for a user, page during a period
        - average reactions, shares, comments during a period
        - histogram reactons, shares, comments
        - 

"""


class Users(Document):
    id = StringField()
    name = StringField()


class Pages(Document):
    pass


class PostStats(Document):
    pass


class PostTexts(Document):
    pass
