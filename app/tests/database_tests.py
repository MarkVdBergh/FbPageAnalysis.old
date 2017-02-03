from __future__ import absolute_import

import unittest
from datetime import datetime

import pytz
from bson import ObjectId
from mongoengine import ValidationError

from app.database.facebook_objects import FbPost


# ToDo: Test for min/max fields (timestamp)
class Test_FbPosts(object):
    """ Base class for testing FbRawPost class"""

    def test_set_collection(self):
        rawpost = FbPost()
        # self.assertEqual(rawpost.meta, {'collection': 'facebook'})
        # self.assertNotEqual(rawpost.meta, {'collection': 'temp'})

    def test_keyname_input_convertion(self):
        ''' _id->id, id->postid'''
        pass

    # def test_keyname_output_convertion(self):
    #     ''' _id->id, id->postid'''
    #     pass

    def test_empty_document(self):
        pass


class Test_FbPosts_Mongo(unittest.TestCase, Test_FbPosts):
    """
        Unittest for FbPost class.
        The FbPost class is used temporarly for loading facebook posts from the 'politics/facebook' database.
    """

    def setUp(self):
        # FbPost_Realistic_Factory.create_batch(1)
        pass

    def tearDown(self):
        # FbPost.drop_collection()
        print 'Collection dropped ...'
        pass

    def test_validation_errors(self):
        # Check validation errors. 'postid', 'id' is required and 'created_time' has min and max.
        post = FbPost()
        with self.assertRaisesRegexp(ValidationError, r"^.*required: \['postid', 'id'] .*too small: \['created_time'].*"):
            post.save(validate=True)
        post.id = ObjectId('000000000000000000000000')
        post.created_time = 1041379300
        post.postid = '0'
        post.save()

    def test_load_post_for_date_range(self):
        # for y in [2000,2001,2010]:
        #     for m in [1,2,12]:
        #         for d in [1,2,3]:
        #             for h in [0,12,23]:
        #                 for m in []
        dt = datetime(2000, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        print dt





        pass




def test_empty_document(self):
    pass


class Test_FbPosts_DictList(unittest.TestCase, Test_FbPosts):
    # def setUp(self):
    #     pass
    #
    # def tearDown(self):
    #     pass
    pass


if __name__ == '__main__':
    unittest.main()
    pass
