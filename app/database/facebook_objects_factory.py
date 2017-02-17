import random

from bson import ObjectId
from factory import Dict, Faker, Sequence, SubFactory, sequence
from factory.fuzzy import reseed_random, FuzzyInteger, FuzzyChoice
from factory.mongoengine import MongoEngineFactory

from facebook_objects import *

# Set random to generate the same data set each time
seed = 41
random.seed(seed)
reseed_random(seed)  # set random seed for factory.fuzzy
Faker._get_faker().seed(seed)  # set random state for factory.Faker


# ToDo: implement different FbRawPost instances with the same profile
#   see: http://stackoverflow.com/questions/39345286/how-to-create-factory-boy-factories-for-django-models-with-the-same-foreign-key
# ToDo: make a separate factory with all non random fields for easy testing?

# Tweak: make length of lists (comment, likes,..) random, and not =n
# Tweak: Make like_count and comment_count the numer of likes and comments iso random int


class Profile_SubFactory(MongoEngineFactory):
    class Meta: model = Profile

    id = Sequence(lambda n: '1%02d' % n)
    # name = Faker(provider='name', locale='nl_NL')
    name = Sequence(lambda n: 'name_%02d' % n)


class User_SubFactory(MongoEngineFactory):
    class Meta: model = User

    id = Sequence(lambda n: '1%02d' % n)
    name = Sequence(lambda n: 'name_%02d' % n)
    # link = Faker(provider='uri')
    # Tweak: picture seems to take a lot of time
    # picture = Dict({'data': Dict({'uri': Faker(provider='uri'),
    #                               'is_silhouette': FuzzyChoice([True, False])})})


class Reactions_SubFactory(MongoEngineFactory):
    class Meta: model = Reactions

    id = Sequence(lambda n: '1%02d' % n)
    type = FuzzyChoice(['LIKE', 'LOVE', 'ANGRY', 'WOW', 'HAHA', 'SAD', 'THANKFUL'])
    name = Sequence(lambda n: 'name_%02d' % n)
    # pic = Faker(provider='uri')


class Comments_SubFactoy(MongoEngineFactory):
    class Meta: model = Comments

    id = Sequence(lambda n: '1%02d' % n)
    created_time = FuzzyInteger(low=1041379200, high=2524608000)
    comment_from = SubFactory(User_SubFactory)
    message = Sequence(lambda n: 'comment_%02d' % n)

    @sequence
    def likes(n):
        _r = random.randint(0, 5)
        _likes = [User_SubFactory() for _ in xrange(_r)]
        return {'data': _likes}

    like_count = FuzzyInteger(low=0, high=20)
    comment_count = FuzzyInteger(low=0, high=20)


class FbPost_Realistic_Factory(MongoEngineFactory):
    class Meta:
        model = FbPosts
        # exclude = ('_postid1', '_postid2')
        # exclude = ('_data')

    id = Sequence(lambda n: ObjectId('a' * 22 + '%02d' % n))
    postid = Sequence(lambda n: 'postid_%02d' % n)
    created_time = Sequence(lambda n: n)
    profile = SubFactory(Profile_SubFactory)

    # _postid1 = SelfAttribute(attribute_name='profile.id')
    # _postid2 = Sequence(lambda n: '2%05d' % n)
    # postid = LazyAttribute(lambda obj: '{}_{}'.format(obj._postid1, obj._postid2))

    @sequence
    def reactions(n):
        # Tweak: now reactions() generates lists of size 0,1,2,3,... Improve with random length
        # Tweak: Rewrite method to comments = Sequence([....])
        _r = random.randint(1, 10)
        _react = [Reactions_SubFactory() for _ in xrange(_r)]
        return _react

    @sequence
    def comments(p):
        _r = random.randint(0, 10)
        _comm = [Comments_SubFactoy() for _ in xrange(_r)]
        return _comm

    shares = Dict({'count': FuzzyInteger(low=0, high=100)})
    from_user = SubFactory(User_SubFactory)

    to_user = {'data': [{'id': 'aaaa', 'name': 'aaaa'}, {'id': 'bbb', 'name': 'bbb'}]}

    message = Sequence(lambda n: 'message_%02d' % n)
    # picture = Faker(provider='uri')
    name = Sequence(lambda n: 'postname_%02d' % n)
    # link = Sequence(lambda n: '1%02d' % n)
    type = FuzzyChoice(['event', 'uri', 'music', 'note', 'photo', 'status', 'video'])
    status_type = FuzzyChoice([None, 'added_photos', 'added_video', 'created_event', 'created_note', 'mobile_status_update', 'published_story', 'shared_story', 'wall_post'])
    story = Sequence(lambda n: 'story_%02d' % n)


if __name__ == '__main__':
    # @profile()

    def test():
        fbps = FbPost_Realistic_Factory.create_batch(2)
        pprint.pprint(fbps[0])
        print type(fbps[0])
        for fbp in fbps:
            fbp_doc = fbp.to_mongo().to_dict()
            fbp.upsert_doc(ups_doc=fbp_doc)


    test()
