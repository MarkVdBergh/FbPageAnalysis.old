from datetime import datetime

from mongoengine import StringField, BooleanField, register_connection, ListField, DateTimeField, IntField, connect, \
    ObjectIdField, EmbeddedDocument, MapField, EmbeddedDocumentListField, DynamicDocument, DictField

from app.settings import TESTING_DB, PRODUCTION_DB


# ToDo: Indexes: If you create an index which contains all the fields you would query and all the fields
# that will be returned by that query, MongoDB will never need to read the data because it's all contained
# within the index. This significantly reduces the need to fit all data into memory for maximum performance.
# These are called covered queries. The explain output will show indexOnly as true if you are using a covered query.


# Fix: Set all defaults in fields to None otherwise update of missing fields will set them to default
class BaseDocument(DynamicDocument):  # Todo: If 'Document iso DynamicDocument, big problems with the oid, _id, id auto_id keys
    meta = {'allow_inheritance': False,
            'abstract': True,
            # Global index options
            'index_options': {},
            'index_background': True,
            'index_drop_dups': True,
            'index_cls': False}

    oid = ObjectIdField(db_field='_id', default=None, required=True, primary_key=True)
    updated = DateTimeField(default=datetime.utcnow())

    unique_field = ''  # Used in the 'upsert_doc()' method

    def upsert_doc(self, ups_doc=None):
        """
        Upserts a document. If no 'ups_doc' provided, then upserts the object via class variables.
        :param ups_doc: dict: Dictionary keys are upsert fields. More flexible then using object arguments. Ex: {'inc__field':1, ...}
        :return: obj : The upserted class object
        """
        # pprint(ups_doc)
        if not ups_doc: ups_doc = self.to_mongo().to_dict()  # Todo: Isn't it better to only accept ups_doc?
        ups_doc['updated'] = datetime.utcnow()  # Update time for each upsert
        _uni = {self.unique_field: ups_doc[self.unique_field]}
        doc = self.__class__.objects(**_uni).upsert_one(**ups_doc)
        return doc

    def get_doc_from_ref(self, ref):
        doc = self.__class__.objects.with_id(ref)  # Returns None if document doesn't exist
        return doc

    def __unicode__(self):
        return self.to_json()


class Pages(BaseDocument):
    page_id = StringField(required=True, unique=True)
    name = StringField(default=None)

    unique_field = 'page_id'  # Used in the 'upsert_doc()' method


class UserActivity(EmbeddedDocument):
    date = DateTimeField()
    action_type = StringField()  # Reaction, Post, Comment, ...
    action_subtype = StringField()  # Like, Angry, Comment_on_comment, ...
    page_ref = ObjectIdField()
    poststat_ref = ObjectIdField()
    comment_ref = ObjectIdField()
    content_ref = ObjectIdField()
    own_page = BooleanField()

    def __unicode__(self):
        return self.to_json()


class Users(BaseDocument):
    user_id = StringField(required=True, unique=True)
    name = StringField()
    picture = StringField()
    is_silhouette = BooleanField()
    pages_active = ListField(default=None)  # List of 'Pages' the user creacted, shared or commented on

    fromed = EmbeddedDocumentListField(UserActivity, default=None)
    toed = EmbeddedDocumentListField(UserActivity, default=None)
    posted = EmbeddedDocumentListField(UserActivity, default=None)
    reacted = EmbeddedDocumentListField(UserActivity, default=None)
    commented = EmbeddedDocumentListField(UserActivity, default=None)
    comment_liked = EmbeddedDocumentListField(UserActivity, default=None)
    tot_fromed = IntField()
    tot_toed = IntField()
    tot_posts = IntField()
    tot_reactions = IntField()
    tot_comments = IntField()
    tot_comments_liked = IntField()

    unique_field = 'user_id'  # Used in the 'upsert_doc()' method


class Contents(BaseDocument):
    content_id = StringField(required=True, unique=True)
    created = DateTimeField()
    user_ref = ObjectIdField()
    page_ref = ObjectIdField()
    poststat_ref = ObjectIdField()
    message = StringField()
    name = StringField()
    story = StringField()
    link = StringField()
    picture_link = StringField()

    nb_reactions = IntField()
    nb_comments = IntField()
    nb_shares = IntField()

    language = StringField(default='nl')

    unique_field = 'content_id'  # Used in the 'upsert_doc()' method


class Comments(BaseDocument):
    comment_id = StringField(required=True, unique=True)
    created = DateTimeField()
    user_ref = ObjectIdField()
    page_ref = ObjectIdField()
    poststat_ref = ObjectIdField()

    parent_ref = ObjectIdField()
    childs_ref = ListField(default=None)

    text = StringField()

    u_reacted = ListField()
    nb_reactions = IntField()
    nb_comments = IntField()

    language = StringField(default='nl')

    unique_field = 'comment_id'  # Used in the 'upsert_doc()' method


class PostStats(BaseDocument):
    post_id = StringField(required=True, unique=True)  #
    created = DateTimeField()  #
    page_ref = ObjectIdField()  #
    post_type = StringField()  #
    status_type = StringField()  #
    u_from_ref = ObjectIdField()  #
    u_to_ref = ListField(default=None)  #
    content_ref = ObjectIdField()

    nb_shares = IntField()  #
    nb_reactions = IntField()  #
    reactions = DictField()  #
    u_reacted = MapField(ListField(), default=None)  # # {like:[ObjectId, ...], ...}
    comments = ListField(default=None)
    u_commented = ListField(default=None)  # [ObjectId(), ...]
    u_comments_liked = ListField(default=None)
    nb_comments_likes = IntField()
    nb_comments = IntField()

    unique_field = 'post_id'  # Used in the 'upsert_doc()' method


if __name__ == '__main__':
    register_connection(alias='test', name=TESTING_DB)
    register_connection(alias='default', name=PRODUCTION_DB)
    connect(db='test')  # Assure we don't delete the politics/facebook collection !!!
