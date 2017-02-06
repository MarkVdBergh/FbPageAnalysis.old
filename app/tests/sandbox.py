# -*- coding: iso-8859-15 -*-

from datetime import datetime

import pytz
from profilehooks import timecall, profile

from app.database.facebook_objects import FbPosts
from app.database.stats_objects import Posts, Pages, Users
from app.tests.database_tests import CreateTestDb


def test():
    # ToDo: Use simple reference iso DbRef.
    # ToDo: Write method to upsert documents:
    # ToDo:                             - use pymongo for crud !
    # ToDo:                             - use mongoengine only as mongo-class mapper
    # ToDo:                             - put None as default everywhere. Fields which are not set will have None value. '.to_mongo().to_dict() and pop non keys.'

    q = FbPosts.get_posts()
    for fb_post in q:
        p = Posts()
        p.created = datetime.fromtimestamp(fb_post.created_time, tz=pytz.utc)
        p.post_id = fb_post.postid
        # Reference fields
        _page = Pages()
        _page.name = fb_post.profile.name
        _page.page_id = fb_post.profile.id
        p.page_id = _page.upstert_page()

        _user = Users()
        _user.user_id = fb_post.from_user.id
        _user.name = fb_post.from_user.name
        p.from_user = _user.upsert_user()



        p.upsert_post()

        # p.from_user = _user
        # p.save()
        # to_users=[]
        # for u in fb_post.to_user['data']:
        #     _user=Users()
        #     _user.user_id=u['id']
        #     _user.name=u['name']
        #     to_users.append(_user)
        # p.to_users=to_users

        # p.save(cascade=True) # Cascade doesn't work with ListField(referenceField)
        # p.save()


        # pprint(p)
        # print p.page_id

@timecall()
@profile()
def setup_testdb(limit):
    CreateTestDb(limit=limit).create_testdb()


if __name__ == '__main__':

    # setup_testdb(1000)
    test()
