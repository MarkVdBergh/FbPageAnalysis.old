from datetime import datetime

import pytz
from mongoengine import StringField, BooleanField, URLField, register_connection, Document, ListField, ReferenceField, ObjectIdField, DateTimeField, IntField

from app.settings import TESTING_DB, PRODUCTION_DB

register_connection(alias='test', name=TESTING_DB)
register_connection(alias='default', name=PRODUCTION_DB)

"""
        Get a set of posts via FbPosts. Iterate each post and save processed data to the appropriate collections.

        Example queries:
            - most active users for a page
            - top 10 post with most reactions, shares, comments
            - all text, messages, comments for a user, page during a period
            - average reactions, shares, comments during a period
            - histogram reactons, shares, comments
"""


class Pages(Document):
    # meta = {'collection': 'facebook', 'indexes': ['pageid', '$name', 'users']}

    id_ = ObjectIdField(db_field='_id', primary_key=True)
    page_id = StringField(db_field='pageid')
    name = StringField()
    posts = ListField(ReferenceField('Users'))  # List of 'Users' that created, reacted, shared or commented on a post

    @classmethod
    def extract_page(cls, fbp):
        p = Pages()
        p.page_id = fbp.profile.id
        p.name = fbp.profile.name
        return cls.id_

    def upsert_page(self):
        pass

    def __unicode__(self):
        return self.to_json()


class Users(Document):
    id_ = ObjectIdField(db_field='_id', primary_key=True)
    user_id = StringField()
    name = StringField()
    picture = URLField()
    is_silhouette = BooleanField()
    link = URLField()
    pages_active = ListField(ReferenceField('Pages'))  # List of 'Pages' the user creacted, shared or commented on

    def extract_users(self, fbp):
        from_user = Users(userid=fbp.pro)

        return self.id_

    def upsert_user(self):
        pass
        return self.id_

    def __unicode__(self):
        return self.to_json()


class PostStat(Document):
    id_ = ObjectIdField(db_field='_id', primary_key=True)

    shares = IntField()
    reactions = ListField(ReferenceField('Texts'))

    type = StringField()
    status_type = StringField()

    def build_(self, fb_post):
        return self.id_

    def __unicode__(self):
        return self.to_json()


class Texts(Document):
    id_ = ObjectIdField(db_field='_id', primary_key=True)

    language = StringField(default='nl')

    def build_(self, fb_post):
        return self.id_

    def __unicode__(self):
        return self.to_json()


class Posts(Document):
    id_ = ObjectIdField(db_field='_id', primary_key=True)
    updated = DateTimeField(datetime.utcnow())

    created = DateTimeField()
    post_id = StringField(db_field='oid', required=True)

    # Reference fields
    page_id = ReferenceField(Pages)

    from_user = ReferenceField(Users)
    to_user = ListField(ReferenceField(Users))  # Facebook users mentioned in message

    message = ReferenceField(Texts)
    story = ReferenceField(Texts)
    post_link = URLField()
    name = ReferenceField(Texts)
    picture = URLField()
    comments = ListField(ReferenceField(Texts))

    def build_post(self, fbp):
        self.created = datetime.fromtimestamp(fbp.created_time, tz=pytz.utc)
        self.post_id = fbp.postid

        self.page_id = Pages.extract_page(fbp)
        self.from_user = Users().extract_users(fbp)

        return self.id_

    def __unicode__(self):
        return self.to_json()
