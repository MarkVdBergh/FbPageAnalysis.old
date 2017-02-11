from mongoengine import register_connection, connect
from profilehooks import timecall

from app.database.stats_objects import Users
from app.settings import TESTING_DB

"""
    Test looking up reference with hashtable, and database lookups
    Conclusion: hash table is 3-5 x faster
"""
register_connection(alias='test', name=TESTING_DB)
connect(db='test')  # Assure we don't delete the politics/facebook collection !!!



class UsersHash(dict):
    # Emmulate container type 'dict'
    def __init__(self):
        q = Users.objects.all().only('user_id', 'id').no_cache()
        q_lst = q.as_pymongo()
        self.update({d['user_id']: d['_id'] for d in q_lst})

    def __setitem__(self, key, item):
        self.__dict__[key] = item

    def __getitem__(self, key):
        return self.__dict__[key]

    def __repr__(self):
        return repr(self.__dict__)

    def __len__(self):
        return len(self.__dict__)

    def __delitem__(self, key):
        del self.__dict__[key]

    def clear(self):
        return self.__dict__.clear()

    def copy(self):
        return self.__dict__.copy()

    def has_key(self, k):
        return self.__dict__.has_key(k)

    def pop(self, k, d=None):
        return self.__dict__.pop(k, d)

    def update(self, *args, **kwargs):
        return self.__dict__.update(*args, **kwargs)

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def pop(self, *args):
        return self.__dict__.pop(*args)

    def __cmp__(self, dict):
        return cmp(self.__dict__, dict)

    def __contains__(self, item):
        return item in self.__dict__

    def __iter__(self):
        return iter(self.__dict__)

    def __unicode__(self):
        return unicode(repr(self.__dict__))

@timecall()
def setup():
    uh = UsersHash()
    sam = Users.objects.only('user_id').limit(1000)
    return uh, sam


def get_reference_for_id(id, collection, x):
    """
        Receives an string id (post_id, page_id, user_id, ...) and returns the corresponding ObjectId refference

        :param id: str: can be post_id, page_id,
        :param collection: str:
        :return: ObjectId: reference
    """
    # With hash
    if x == 1:
        return user_hash[id]
    # with db lookup, returning all fields
    if x == 2:
        q = Users.objects(user_id=id).no_cache().first()
        return q['id']
    # with db lookup returning only id
    if x == 3:
        q = Users.objects(user_id=id).no_cache().only('id').first()
        return q.id


user_hash, uid_samples = setup()


@timecall()
def test(t):
    for i in uid_samples:
        get_reference_for_id(i.user_id, '', t)


for t in [1, 2, 3]:
    test(t)
