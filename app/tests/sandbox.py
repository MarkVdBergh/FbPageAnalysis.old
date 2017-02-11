# -*- coding: iso-8859-15 -*-

from datetime import datetime

import pandas as pd
import pprofile
import pytz
from mongoengine import register_connection
from mongoengine.context_managers import switch_db
from profilehooks import timecall

from app.database.facebook_objects import FbPosts
from app.database.stats_objects import Posts, Pages, Users, PostStats, Snippets
from app.settings import FB_PAGES_LIST
from app.tests.database_tests import CreateTestDb




# @timecall(immediate=False)
# @coverage
def test():
    # ToDo: Use simple reference iso DbRef.
    # ToDo: Write method to upsert documents:
    # ToDo:                             - use pymongo for crud !
    # ToDo:                             - use mongoengine only as mongo-class mapper
    # ToDo:                             - put None as default everywhere. Fields which are not set will have None value. '.to_mongo().to_dict() and pop non keys.'

    register_connection(alias='politics', name='politics')

    with switch_db(FbPosts, 'politics') as FbPostsProduction:
        for pid in FB_PAGES_LIST:
            q = FbPostsProduction.get_posts(flag=0, pageid=pid, batch_size=100)  # Tweak: check optimal batch_size
            iii = 0  # Fix: remove (testing)
            for fb_post in q:
                if iii % 100 == 0:
                    print datetime.now(), iii
                iii += 1


                # sleep(10)
                ps = Posts()
                ps.created = datetime.fromtimestamp(fb_post.created_time, tz=pytz.utc)
                ps.post_id = fb_post.postid
                # Reference fields
                pg = Pages()
                pg.name = fb_post.profile.name
                pg.page_id = fb_post.profile.id
                ps.page_id = pg.upsert_doc().pk

                u = Users()
                u.user_id = fb_post.from_user.id
                u.name = fb_post.from_user.name
                ps.from_ref = u.upsert_doc().pk

                s = PostStats()
                s.stat_id = fb_post.postid

                s.shares = fb_post.shares['count']

                # Reactions
                # Convert EmbeddedList to list of dics
                _reacts = [fb_post.reactions[r].to_mongo().to_dict() for r in xrange(len(fb_post.reactions))]  # Fix: only needed to convert EmbeddedList to list of dics
                if _reacts:
                    if _reacts[0].keys() != ['blacklisted']:  # Fix: Scraping
                        # print ps.post_id
                        # print _reacts[0]
                        _df_reacts = pd.DataFrame(_reacts)
                        _df_reacts['type'] = _df_reacts['type'].str.lower()  # LIKE->like
                        _dfg_reacts = _df_reacts.groupby(['type'])  # tuple of (str,df)
                        # set the count per type
                        s.reactions = _dfg_reacts['id'].count().to_dict()
                        s.nb_reactions = sum(s.reactions.values())
                        # Iterate reactions and extract userdata
                        for i, row in _df_reacts.iterrows():  # u is pandas.Series
                            user = Users()
                            user.user_id = row['id']
                            user.name = row['name']
                            user.picture = row['pic']

                            # Need to upsert with a dictionary because we use 'inc__' etc
                            ups_doc = dict(inc__tot_reactions=1,
                                           add_to_set__pages_active=fb_post.profile.id)  # Fix: should be better with ObjectId.
                            ups_doc.update(user.to_mongo().to_dict())
                            user.upsert_doc(ups_doc)
                    else:  # set reactions to []
                        FbPosts(id=fb_post.id).update(reactions=[])

                    # Message
                    sn = Snippets()
                    sn.snippet_id = fb_post.postid
                    sn.created = datetime.fromtimestamp(fb_post.created_time, tz=pytz.utc)
                    sn.snip_type = fb_post.type
                    sn.author = ps.from_ref
                    sn.post_ref = ps.upsert_doc().pk
                    sn.text = fb_post.message
                    sn.reactions = s.nb_reactions
                    sn.shares = s.shares

                    ps.message_ref = sn.upsert_doc().pk  # Fix: mongoengine.errors.ValidationError: u'231742536958_198055708174' is not a valid ObjectId, it must be a 12-byte input or a 24-character hex string

                    # References
                    # ToDo: Optimize all upserts
                    s.post_id = ps.upsert_doc().pk
                    s.page_id = pg.upsert_doc().pk
                    s.upsert_doc()

                    # Mark fb_post as done
                    # fb_post.flag=1
                    FbPosts(id=fb_post.id).update(flag=1)




@timecall()
def setup_testdb(limit):
    page_ids = FB_PAGES_LIST

    CreateTestDb(limit=limit, page_ids=page_ids).create_testdb()


if __name__ == '__main__':
    # setup_testdb(1000)
    prof=pprofile.Profile(verbose=False)
    with prof():
        test()
    prof.print_stats()
