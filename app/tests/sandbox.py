# -*- coding: iso-8859-15 -*-

from datetime import datetime

import pandas as pd
import pytz
from profilehooks import timecall

from app.database.facebook_objects import FbPosts
from app.database.stats_objects import Posts, Pages, Users, PostStats
from app.tests.database_tests import CreateTestDb


@timecall()
# @profile()
def test():
    # ToDo: Use simple reference iso DbRef.
    # ToDo: Write method to upsert documents:
    # ToDo:                             - use pymongo for crud !
    # ToDo:                             - use mongoengine only as mongo-class mapper
    # ToDo:                             - put None as default everywhere. Fields which are not set will have None value. '.to_mongo().to_dict() and pop non keys.'

    q = FbPosts.get_posts()
    for fb_post in q:
        # sleep(10)
        p = Posts()
        p.created = datetime.fromtimestamp(fb_post.created_time, tz=pytz.utc)
        p.post_id = fb_post.postid
        # Reference fields
        _page = Pages()
        _page.name = fb_post.profile.name
        _page.page_id = fb_post.profile.id
        # p.page_id = _page.upsert_doc().pk

        _user = Users()
        _user.user_id = fb_post.from_user.id
        _user.name = fb_post.from_user.name
        # p.from_ref = _user.upsert_doc().pk

        s = PostStats()
        s.stat_id = fb_post.postid
        s.shares = fb_post.shares['count']

        # Reactions
        # Convert EmbeddedList to list of dics
        _reacts = [fb_post.reactions[i].to_mongo().to_dict() for i in xrange(len(fb_post.reactions))]  # Fix: only needed to convert EmbeddedList to list of dics
        if _reacts:
            _df_reacts = pd.DataFrame(_reacts)
            _df_reacts['type'] = _df_reacts['type'].str.lower()  # LIKE->like
            _dfg_reacts = _df_reacts.groupby(['type'])  # tuple of (str,df)
            # set the count per type
            s.reactions = _dfg_reacts['id'].count().to_dict()
            s.tot_reactions = sum(s.reactions.values())
            # Iterate reactions and extract userdata
            for i, u in _df_reacts.iterrows():  # u is pandas.Series
                user = Users()
                user.user_id = u['id']
                user.name = u['name']
                user.picture = u['pic']

                # Need to upsert with a dictionary because we use 'inc__' etc
                ups_doc = dict(inc__tot_reactions=1,
                               add_to_set__pages_active=fb_post.profile.id)  # Fix: should be better with ObjectId.
                ups_doc.update(user.to_mongo().to_dict())

                user.upsert_doc(ups_doc)

                # print 'xxxxx',u

        s.upsert_doc()

        # print _df_reacts.head()
        # print tot_reacts


        # break

        # print 'xxx', s.upsert_doc()

        p.upsert_doc()

        # p.from_ref = _user
        # p.save()
        # to_ref=[]
        # for u in fb_post.to_user['data']:
        #     _user=Users()
        #     _user.user_id=u['id']
        #     _user.name=u['name']
        #     to_ref.append(_user)
        # p.to_ref=to_ref

        # p.save(cascade=True) # Cascade doesn't work with ListField(referenceField)
        # p.save()


        # print p.page_id
        # pprint(p)


def setup_testdb(limit):
    CreateTestDb(limit=limit).create_testdb()


if __name__ == '__main__':
    # setup_testdb(1000)
    test()
