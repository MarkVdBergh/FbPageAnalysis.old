from mongoengine import StringField, BooleanField, URLField, register_connection, Document, ListField, ReferenceField

from app.settings import TESTING_DB, PRODUCTION_DB

register_connection(alias='test', name=TESTING_DB)

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

register_connection(alias='default', name=PRODUCTION_DB)


class Users(Document):
    id = StringField()
    name = StringField()
    picture = URLField()
    is_silhouette = BooleanField()
    link = URLField()
    pages_active = ListField(ReferenceField('Pages'))  # List of 'Pages' the user creacted, shared or commented on


class Pages(Document):
    id = StringField()
    name = StringField()
    users_active = ListField(ReferenceField('Users'))  # List of 'Users' that created, reacted, shared or commented on a post


class PostStats(Document):
    pass


class PostTexts(Document):
    pass
