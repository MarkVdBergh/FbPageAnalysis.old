import pprint

from mongoengine import EmbeddedDocument, StringField, DictField, BooleanField, URLField, DynamicEmbeddedDocument, IntField, EmbeddedDocumentField, \
    EmbeddedDocumentListField, DynamicDocument, register_connection
from profilehooks import timecall

from app.settings import TESTING, TESTING_DB, PRODUCTION_DB

register_connection(alias='test', name=TESTING_DB)
register_connection(alias='default', name=PRODUCTION_DB)
print TESTING, TESTING_DB, PRODUCTION_DB



class Profile(EmbeddedDocument):
    id = StringField(required=True)
    name = StringField()


class User(EmbeddedDocument):
    id = StringField(required=False)
    name = StringField()
    link = StringField()
    picture = DictField(data={'url': StringField(),
                              'is_silhouette': BooleanField()})


class Reactions(
    DynamicEmbeddedDocument):  # Fix: error when not Dynamic ( mongoengine.errors.FieldDoesNotExist: The fields "set(['blacklisted'])" do not exist on the document "Reactions")
    # Fix: 'blacklisted' in reactions ??? see 223630074319030_103129139727064
    id = StringField()
    type = StringField()
    name = StringField()
    pic = URLField()

    def __unicode__(self):
        return self.to_json()


# ToDo: Why Dynamic?
class Comments(DynamicEmbeddedDocument):
    id = StringField()
    created_time = IntField()
    comment_from = EmbeddedDocumentField(db_field='from', document_type=User)
    message = StringField()
    likes = DictField(data=EmbeddedDocumentListField(document_type=User))
    like_count = IntField()
    comment_count = IntField()


class FbPosts(DynamicDocument):
    """
        This is a probably temporary class to read the posts saved by the old scraper in the 'politics/facebook' collection
    """
    # ToDo: Rename class variables
    # ToDo: Add indexes ?
    meta = {'collection': 'facebook'}  # Otherwise documents will be saved in the 'fb_post' collection. (Default collection is classname in smallcaps
    if TESTING:  # Swith to the test database, otherwise use default one.
        meta['db_alias'] = 'test'
    else:
        meta['db_alias'] = 'default'
    print meta

    # oid = ObjectIdField(db_field='_id', primary_key=True)
    created_time = IntField(min_value=0, max_value=5000000000, default=-1)  #
    postid = StringField(db_field='id', required=True)  #
    profile = EmbeddedDocumentField(document_type=Profile)#
    reactions = EmbeddedDocumentListField(document_type=Reactions)
    comments = EmbeddedDocumentListField(document_type=Comments)
    shares = DictField(default={'count': 0})  #
    from_user = EmbeddedDocumentField(db_field='from', document_type=User) #
    to_user = DictField(db_field='to') # # ToDo: Keeps returning 'to' iso 'to_user' ???
    # ToDo: Doesn't work !!!
    # to_user=EmbeddedDocumentListField(db_field='to.data', document_type=User)
    message = StringField()
    picture = StringField() #
    name = StringField()
    link = StringField() #
    type = StringField() #
    status_type = StringField() #
    story = StringField()

    @classmethod
    @timecall()
    def get_posts(cls, id_=None, pageid=None, since=None, until=None, batch_size=0, **query):
        """
            Method to get posts from the database and returns a queryset. All arguments are optional. No arguments returns all the posts from the database.

            :param id_: ObjectId()
            :param pageid: str: the profile.id_ of the page
            :param since: int: timestamp. If no 'until', returns all posts since 'since'
            :param until: int: timestamp: If no 'since', returns all posts until 'until'
            :param query: dict: flexible mongodb query

            :return q: queryset: mongoengine queryset

        """

        q = cls.objects(**query)
        if id_: q = q(id_=id_)
        if pageid: q = q(profile__id=pageid)
        if since: q = q(created_time__gte=since)
        if until: q = q(created_time__lte=until)
        if batch_size: q = q.batch_size(batch_size)  # nr of documentent per db call
        return q

    def __unicode__(self):
        return self.to_json()


# @profile()
@timecall()
def test():
    x = FbPosts
    # for i in x.objects(shares__count__gte=13000):
    # This doesn't work ! see: https://docs.mongodb.com/manual/reference/operator/query/size/#_S_size. "create a counter field that you increment when you add elements to a field."

    # for i in x.objects(comments__size__gte=100):
    # for i in x.objects(comments__1600__exists=True):
    #     print '=' * 100
    #     print 'http://facebook.com/{}'.format(i.postid)
    #     print('{}:   postid: {}, comments: {}'.format(i.profile.name,i.postid, len(i.comments)))
    #     print i.created_time_local, i.created_time_dt
    #     print i.message
    #     print '=' * 100



    # j=json.dumps({'oid':9999,'profile':{'oid':100,'name':'MMMM'}})
    # y=x.from_json(j)
    # print y.to_json()
    # x.set_collection('facebook_test')
    # y = x.objects(shares__count__gte=10000)
    print (type(x))
    y = x.objects.count()
    pprint.pprint(y)


if __name__ == '__main__':
    test()
