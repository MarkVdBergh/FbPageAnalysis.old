from datetime import datetime

from mongoengine import StringField, BooleanField, URLField, register_connection, Document, ListField, ReferenceField, DateTimeField, IntField

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

#ToDo: check: https://www.mongodb.com/blog/post/6-rules-of-thumb-for-mongodb-schema-design-part-3

class Pages(Document):
    # meta = {'collection': 'facebook', 'indexes': ['pageid', '$name', 'users']}

    # id_ = ObjectIdField(db_field='_id')
    page_id = StringField(unique=True)
    name = StringField()
    posts = ListField(ReferenceField('Users'))  # List of 'Users' that created, reacted, shared or commented on a post

    def upstert_page(self):
        page = Pages.objects(page_id=self.page_id).upsert_one(name=self.name)
        return page

    def __unicode__(self):
        return self.to_json()


class Users(Document):
    # id_ = ObjectIdField(db_field='_id', default=ObjectId, primary_key=True)
    user_id = StringField()
    name = StringField()
    picture = URLField()
    is_silhouette = BooleanField()
    link = URLField()
    pages_active = ListField(ReferenceField('Pages'))  # List of 'Pages' the user creacted, shared or commented on

    def upsert_user(self):
        # user=Users.objects(user_id=self.user_id).upsert_one(name=self.name)
        u = self.to_mongo().to_dict()
        user = Users.objects(user_id=self.user_id).upsert_one(**u)
        return user

    # return self.id_

    def __unicode__(self):
        return self.to_json()


class PostStat(Document):
    # id_ = ObjectIdField(db_field='_id', default=ObjectId, primary_key=True)

    shares = IntField()
    reactions = ListField(ReferenceField('Texts'))

    type = StringField()
    status_type = StringField()

    def build_(self, fb_post):
        return self.id_

    def __unicode__(self):
        return self.to_json()


class Texts(Document):
    # id_ = ObjectIdField(db_field='_id', default=ObjectId, primary_key=True)

    language = StringField(default='nl')

    def build_(self, fb_post):
        return self.id_

    def __unicode__(self):
        return self.to_json()


class Posts(Document):
    # id_ = ObjectIdField(db_field='_id', default=ObjectId, primary_key=True)
    updated = DateTimeField(datetime.utcnow())

    created = DateTimeField()
    post_id = StringField(required=True)

    # Reference fields
    page_id = ReferenceField(Pages, validation=False, dbref=True)

    from_user = ReferenceField(Users, dbref=True)
    to_users = ListField(ReferenceField(Users), default=None, dbref=True)  # Facebook users mentioned in message

    message = ReferenceField(Texts, dbref=True)
    story = ReferenceField(Texts, dbref=True)
    post_link = URLField()
    name = ReferenceField(Texts, dbref=True)
    picture = URLField()
    comments = ListField(ReferenceField(Texts), default=None, dbref=True)

    def upsert_post(self):
        p = self.to_mongo().to_dict()
        print 'xxx', p
        post = Posts.objects(post_id=self.post_id).upsert_one(**p)
        return post

    def __unicode__(self):
        return self.to_json()
