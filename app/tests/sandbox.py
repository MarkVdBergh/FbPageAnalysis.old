# -*- coding: iso-8859-15 -*-

from datetime import datetime

import pytz

from app.database.facebook_objects import FbPosts
from app.database.stats_objects import Posts, Pages, Users
from app.tests.database_tests import CreateTestDb


def test():
    q = FbPosts.get_posts(oid='443387969094801_1029992413767684')
    p = Posts()
    for post in q:
        p.created = datetime.fromtimestamp(post.created_time, tz=pytz.utc)
        p.post_id = post.id
        _page = Pages()
        _page.page_id = post.profile.id
        _page.name = post.profile.name
        p.page_id = _page.upsert_page()
        _user = Users()
        _user.user_id = post.from_user.id_
        _user.name = post.from_user.name
        p.from_user=_user.upsert_user()

        print post
def setup_testdb():
    CreateTestDb(limit=100).create_testdb()

if __name__ == '__main__':
    # test()
    setup_testdb()