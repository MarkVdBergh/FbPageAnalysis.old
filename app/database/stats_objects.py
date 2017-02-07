from datetime import datetime
from pprint import pprint

from mongoengine import StringField, BooleanField, URLField, register_connection, Document, ListField, ReferenceField, DateTimeField, IntField, connect, \
    ObjectIdField, DictField

from app.settings import TESTING_DB, PRODUCTION_DB

"""
        Get a set of posts via FbPosts. Iterate each post and save processed data to the appropriate collections.

        Example queries:
            - most active users for a page
            - top 10 post with most reactions, shares, comments
            - all text, messages, comments for a user, page during a period
            - average reactions, shares, comments during a period
            - histogram reactons, shares, comments
"""


class BaseDocument(Document):
    meta = {'allow_inheritance': False,
            'abstract': True,
            # Global index options
            'index_options': {},
            'index_background': True,
            'index_drop_dups': True,
            'index_cls': False
            }

    # id_ = ObjectIdField(db_field='_id', default=ObjectId, primary_key=True)
    updated = DateTimeField(default=datetime.utcnow())  # Default is set when the module starts
    # and is the same for all pages saved/updated in that batch

    unique_field = ''  # Used in the 'upsert_doc()' method

    def upsert_doc(self, ups_doc=None):
        """
        Upserts a document. If no 'ups_doc' provided, then upserts the object via class variables.
        :param ups_doc: dict: Dictionary keys are upsert fields. More flexible then using object arguments. Ex: {'inc__field':1, ...}
        :return: obj : The upserted class object
        """
        # Fix: Set all defaults in fields to None otherwise update of missing fields will set them to default
        if not ups_doc: ups_doc = self.to_mongo().to_dict()
        ups_doc['updated'] = datetime.utcnow()  # Update time for each upsert
        _uni = {self.unique_field: ups_doc[self.unique_field]}
        post = self.__class__.objects(**_uni).upsert_one(**ups_doc)
        return post

    def __unicode__(self):
        return self.to_json()


class Pages(BaseDocument):
    page_id = StringField(required=True, unique=True)

    name = StringField(default=None)
    posts = ListField(ReferenceField('Users'), default=None)  # List of 'Users' that created, reacted, shared or commented on a post
    test = StringField(default=None)

    unique_field = 'page_id'  # Used in the 'upsert_doc()' method


class Users(BaseDocument):
    user_id = StringField(required=True, unique=True)

    name = StringField()
    picture = URLField()
    is_silhouette = BooleanField()
    link = URLField()

    pages_active = ListField(default=None)  # List of 'Pages' the user creacted, shared or commented on
    tot_reactions=IntField()
    tot_comments=IntField()
    activity=DictField(default=None)

    unique_field = 'user_id'  # Used in the 'upsert_doc()' method


class PostStats(BaseDocument):
    stat_id = StringField(required=True, unique=True)
    created = DateTimeField()
    shares = IntField()

    reactions=DictField()
    tot_reactions=IntField()

    type = StringField()
    status_type = StringField()

    unique_field = 'stat_id'  # Used in the 'upsert_doc()' method


class Snippets(BaseDocument):
    snippet_id = StringField(required=True,unique=True)
    created = DateTimeField()

    language = StringField(default='nl')

    unique_field = 'snipped_id'  # Used in the 'upsert_doc()' method


class Posts(BaseDocument):
    post_id = StringField(required=True)

    created = DateTimeField()
    page_id = ObjectIdField()
    from_ref = ObjectIdField()
    to_ref = ListField(ObjectIdField(), default=None)  # Facebook users mentioned in message
    message_ref = ObjectIdField()
    story_ref = ObjectIdField()
    post_link = URLField()
    post_name_ref = ObjectIdField()
    picture_link = URLField()
    comments_ref = ListField(ObjectIdField, default=None)

    unique_field = 'post_id'  # Used in the 'upsert_doc()' method


if __name__ == '__main__':
    register_connection(alias='test', name=TESTING_DB)
    register_connection(alias='default', name=PRODUCTION_DB)
    connect(db='test')  # Assure we don't delete the politics/facebook collection !!!

    q = Posts.objects.first()

    p = Posts()
    p.post_id = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'

    pprint(p)
    pu = p.upsert_doc()
    pprint(pu)
