from __future__ import absolute_import

import unittest
from datetime import datetime, timedelta

import pytz
from bson import ObjectId
from mongoengine import ValidationError

from app.database.facebook_objects import FbPosts, Profile


class Test_FbPosts_Mongo(unittest.TestCase):
    """
        Unittest for FbPosts class.
        The FbPosts class is used temporarly for loading facebook posts from the 'politics/facebook' database.
    """

    def setUp(self):
        # FbPost_Realistic_Factory.create_batch(1)
        pass

    def tearDown(self):
        FbPosts.drop_collection()
        print 'Collection dropped ...'
        pass

    # @unittest.skip('')
    def test_validation_errors(self):
        # Check validation errors. 'postid', 'id' is required and 'created_time' has min and max.
        post = FbPosts()
        with self.assertRaisesRegexp(ValidationError, r"^.*required: \['postid', 'id'] .*too small: \['created_time'].*"):
            post.save(validate=True)
        post.id = ObjectId('000000000000000000000000')
        post.created_time = 1041379300
        post.postid = '0'
        post.save()

    def test_get_posts(self):

        # Setup documents in the database
        date_time = datetime(2000, 1, 1, 0, 0, tzinfo=pytz.utc)
        i = 0
        posts = []
        for d in [0, 1, 365]:
            for h in [0, 1, 24]:
                _td = timedelta(days=d, hours=h)
                _dt = date_time + _td
                timestamp = (_dt - datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc)).total_seconds()
                id = ObjectId('00000000000000000000000{}'.format(i))
                postid = str(i)
                profile = Profile(id=str(i // 3))
                fbp = FbPosts(id=id, postid=postid, created_time=timestamp, profile=profile)
                fbp.save(validate=True)
                i += 1
                posts.append(fbp)

        # Test get all posts
        q = FbPosts.get_posts()
        [self.assertEqual(q[i].to_json(), posts[i].to_json()) for i in xrange(len(posts))]

        # Test get post by id
        [self.assertEqual(FbPosts.get_posts(id=ObjectId('00000000000000000000000{}'.format(i)))[0].to_json(), posts[i].to_json()) for i in xrange(len(posts))]




        # sleep(20)



if __name__ == '__main__':
    unittest.main()
    pass
