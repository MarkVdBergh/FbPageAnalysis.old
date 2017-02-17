# -*- coding: utf-8 -*-
from datetime import datetime
from time import mktime

import pandas as pd
import pytz
from mongoengine import register_connection, connect
from mongoengine.context_managers import switch_db
from profilehooks import timecall

from app.database.facebook_objects import FbPosts
from app.database.mongo_singleton import MongoFacebook, MongoFacebookTest
from app.database.stats_objects import Pages, Users, PostStats, UserActivity, Contents, Comments
from app.settings import FB_PAGES_LIST


class DatabaseTools(object):
    def __init__(self):
        self.politics_db = MongoFacebook().get_database()
        self.facebook_col = self.politics_db['facebook']
        self.test_db = MongoFacebookTest().get_database()
        self.facebook_test_col = self.test_db['facebook']

    def facebook_set_flag(self, postid, flag=0):
        rslt = self.facebook_col.update({'id': postid}, {'$set': {'flag': flag}}, upsert=False)
        return rslt

    def facebook_reset_flag(self, page_id=None):
        query = {'flag': {'$ne': 0}}
        if page_id: query.update({'profile.id': page_id})
        rslt = self.facebook_col.update_many(query, {'$set': {'flag': 0}}, upsert=False)
        return rslt

    def facebook_create_flag_field(self):
        rslt = self.facebook_col.update_many({'flag': {"$exists": False}}, {'$set': {'flag': 0}}, upsert=False)
        return rslt


class DatabaseWorker(object):
    @timecall()
    # @profile()
    def __init__(self, fbpost):
        self.fbpost = fbpost
        # Initiate Pages and Content (need oid's)
        self.content = Contents()
        self.content.content_id = self.fbpost.postid
        self.content = self.content.upsert_doc(only=['oid', 'content_id'])  # Save to get the oid if exist
        self.poststat = PostStats()
        self.poststat.post_id = self.fbpost.postid
        self.poststat = self.poststat.upsert_doc()  # Save to get the oid if exist

        self.__make_page()
        self.__make_content()
        self.__make_poststat()
        self.__make_reaction()
        self.__make_comment_chain()

    def __make_page(self):
        page = Pages()
        page.page_id = self.fbpost.profile.id
        page.name = self.fbpost.profile.name
        page = page.upsert_doc(only=['oid'])
        self.page = page
        return None

    def __make_poststat(self):
        self.poststat.created = datetime.fromtimestamp(self.fbpost.created_time)
        self.poststat.post_type = self.fbpost.type
        self.poststat.status_type = self.fbpost.status_type
        self.poststat.page_ref = self.page.oid
        # u_from_ref
        user, _useractivity = self.__make_user(user_id=self.fbpost.from_user.id,
                                               user_name=self.fbpost.from_user.name,
                                               user_picture=None,
                                               date=self.poststat.created,
                                               action_type='from')
        user_upsdoc = user.to_mongo().to_dict()
        user_upsdoc.update(push__fromed=_useractivity)
        user_upsdoc.update(inc__tot_fromed=1)
        user = user.upsert_doc(ups_doc=user_upsdoc)
        self.poststat.u_from_ref = user.oid
        # u_to_ref
        u_to_ref = []
        if self.fbpost.to_user:
            for usr in self.fbpost.to_user.get('data'):
                user, _useractivity = self.__make_user(user_id=usr['id'],
                                                       user_name=usr['name'],
                                                       user_picture=None,
                                                       date=self.poststat.created,
                                                       action_type='from')
                user_upsdoc = user.to_mongo().to_dict()
                user_upsdoc.update(push__toed=_useractivity)
                user_upsdoc.update(inc__tot_toed=1)
                user.upsert_doc(ups_doc=user_upsdoc)
                u_to_ref.append(user.oid)
                self.poststat.u_to_ref = u_to_ref
        poststat_upsdoc = self.poststat.to_mongo().to_dict()
        self.poststat = self.poststat.upsert_doc(ups_doc=poststat_upsdoc)
        return None

    def __make_content(self):
        content = self.content
        content.created = self.poststat.created
        content.user_ref = self.poststat.u_from_ref
        content.page_ref = self.poststat.page_ref
        content.poststat_ref = self.poststat.oid
        if self.fbpost.message:
            content.message = self.fbpost.message
        if self.fbpost.name:
            content.name = self.fbpost.name
        if self.fbpost.story:
            content.story = self.fbpost.story
        content.link = self.fbpost.link
        content.picture_link = self.fbpost.picture
        content.nb_reactions = len(self.fbpost.reactions)
        content.nb_comments = len(self.fbpost.comments)
        content.nb_shares = self.fbpost.shares['count']
        content_upsdoc = content.to_mongo().to_dict()

        self.content = content.upsert_doc(content_upsdoc)
        return None

    def __make_reaction(self):
        # u_reacted / nb_reactions
        reacts = [self.fbpost.reactions[r].to_mongo().to_dict() for r in xrange(len(self.fbpost.reactions))]  # Fix: only needed to convert EmbeddedList to list of dics
        if reacts:
            # Fix: check if reacts[0].get('black..., None) is faster
            if reacts[0].keys() != ['blacklisted']:  # Fix: Scraping
                df_reacts = pd.DataFrame(reacts)
                df_reacts['type'] = df_reacts['type'].str.lower()  # LIKE->like
                dfg_reacts = df_reacts.groupby(['type'])  # tuple of (str,df)
                # set the count per type
                self.poststat.reactions = dfg_reacts['id'].count().to_dict()
                self.poststat.nb_reactions = sum(self.poststat.reactions.values())
                # Iterate reactions and extract userdata
                reacted = {}
                for i, usr in df_reacts.iterrows():  # row is pandas.Series
                    user, _useractivity = self.__make_user(user_id=usr['id'],
                                                           user_name=usr['name'],
                                                           user_picture=None,
                                                           date=self.poststat.created,
                                                           action_type='reaction',
                                                           action_subtype=usr['type'])
                    user_upsdoc = user.to_mongo().to_dict()
                    user_upsdoc.update(push__reacted=_useractivity)
                    user_upsdoc.update(inc__tot_reactions=1)
                    user.upsert_doc(ups_doc=user_upsdoc)
                    # Add user.oid to the correct list in 'poststat.u_reacted'
                    # see https://docs.quantifiedcode.com/python-anti-patterns/correctness/not_using_setdefault_to_initialize_a_dictionary.html
                    reacted.setdefault(usr['type'], []).append(user.oid)
                self.poststat.u_reacted = reacted
        return None

    def __make_comment_chain(self):
        comments = self.fbpost.comments
        teller = -1
        child_oids = []
        parent = parent_oid = None
        for fbcomment in comments:
            # Fix: Blacklisted
            if getattr(fbcomment, 'blacklisted)', None): continue   # is faster when the attribute is likely not present
            # fix: comment liek <Comments: {"updated": {"$date": 1487309377711}, "u_reacted": [], "language": "nl"}> exist !
            try: # is faster when the attibute is likely present
                _=fbcomment.id
            except ArithmeticError:
                continue

            comment = self.__make_comment(fbcomment)
            if teller == -1:  # parent
                # comment = comment.upsert_doc()  # save parent
                teller = fbcomment.comment_count
                parent = comment
                parent_oid = comment.oid
            else:  # child
                comment.parent_ref = parent_oid
                comment = comment.upsert_doc()  # Save child
                child_oids.append(comment.oid)

            if fbcomment.comment_count > 0:  # set teller to nb childs
                teller = fbcomment.comment_count

            if teller == 0:  # Last child
                if child_oids: parent.childs_ref = child_oids
                parent.upsert_doc()  # Update parent
                child_oids = []
            teller -= 1

    def __make_comment(self, fbcom):
        comment = Comments()
        comment.comment_id = fbcom.id
        comment = comment.upsert_doc()  # Need the oid in user.comment_ref
        comment.created = datetime.fromtimestamp(fbcom.created_time)
        user, _useractivity = self.__make_user(user_id=fbcom.comment_from.id,
                                               user_name=fbcom.comment_from.name,
                                               date=datetime.fromtimestamp(fbcom.created_time),
                                               action_type='comment',
                                               action_subtype=None,
                                               comment_ref=comment.oid)
        user_upsdoc = user.to_mongo().to_dict()
        user_upsdoc.update(push__commented=_useractivity)
        user_upsdoc.update(inc__tot_comments=1)
        user = user.upsert_doc(ups_doc=user_upsdoc)
        comment.user_ref = user.oid
        comment.page_ref = self.page.oid
        comment.poststat_ref = self.poststat.oid
        comment.parent_ref = None
        comment.childs_ref = None
        comment.text = fbcom.message
        # comment likes
        u_reacted = []
        for usr in fbcom.likes.get('data', []):
            user, _useractivity = self.__make_user(user_id=usr['id'],
                                                   user_name=usr['name'],
                                                   date=datetime.fromtimestamp(fbcom.created_time),
                                                   # user_picture=usr['pic'],
                                                   action_type='comment liked',
                                                   action_subtype=None,
                                                   comment_ref=comment.oid)
            comment_liked_upsdoc = user.to_mongo().to_dict()
            comment_liked_upsdoc.update(push__comment_liked=_useractivity)
            comment_liked_upsdoc.update(inc__tot_comments_liked=1)
            user = user.upsert_doc(ups_doc=comment_liked_upsdoc)
            u_reacted.append(user.oid)
        comment.u_reacted = u_reacted
        comment.nb_reactions = fbcom.like_count
        comment.nb_comments = fbcom.comment_count
        return comment

    def __make_user(self, user_id, user_name, user_picture=None, date=None, action_type=None, action_subtype=None, comment_ref=None):
        user = Users()
        user.user_id = user_id
        user = user.upsert_doc(only=['oid', 'user_id'])
        user.name = user_name
        user.picture = user_picture
        _useractivity = UserActivity()
        _useractivity.date = date
        _useractivity.action_type = action_type
        _useractivity.action_subtype = action_subtype
        _useractivity.page_ref = self.page.oid
        _useractivity.poststat_ref = self.poststat.oid
        _useractivity.comment_ref = comment_ref
        _useractivity.content_ref = self.content.oid
        _useractivity.own_page = user_id == self.page.page_id  # fix: seems to be always false
        return user, _useractivity


if __name__ == '__main__':
    pass

# Fix: PostStat : no content_ref, np_shares, nb_reactions, reactions, u_reacted, comments, ...
# Fix: Contentts: no page_ref,

connect('test5')
tools = DatabaseTools()
# DatabaseTools().facebook_reset_flag()
# 1/0

register_connection(alias='politics', name='politics')

flag = 1
d = datetime(2000, 1, 1, tzinfo=pytz.utc)
since = mktime(d.timetuple())
for pid in FB_PAGES_LIST:
    with switch_db(FbPosts, 'politics') as FbPostsProduction:
        q = FbPostsProduction.get_posts(pageid=pid, flag_ne=flag, since=since, batch_size=1)
        # q = FbPostsProduction.get_posts(oid=ObjectId("58a2eda0d8d09466f8cf594e"))
        # q = FbPostsProduction.get_posts()
    print datetime.now(), 'start: ', pid
    for fb_post in q:
        # pprint(fb_post)
        x = DatabaseWorker(fbpost=fb_post)
        # print BorgCounter.Borg
        tools.facebook_set_flag(postid=fb_post.postid, flag=flag)

# With Hashtables
"""
New MongoClient created
	4 97046

  get_posts (/home/marc/DATA/Projects/FbPageAnalysis/app/database/facebook_objects.py:85):
    0.001 seconds

2017-02-16 15:30:25.458572 start:  202064936858448
202064936858448_227859140945694

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    317.396 seconds

202064936858448_228917490839859

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    47.271 seconds

202064936858448_230673523997589

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    82.097 seconds

202064936858448_231050590626549

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    40.722 seconds
"""

# Without hashtables
"""
New MongoClient created

  get_posts (/home/marc/DATA/Projects/FbPageAnalysis/app/database/facebook_objects.py:85):
    0.001 seconds

2017-02-16 17:42:42.136167 start:  202064936858448
202064936858448_227859140945694

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    528.806 seconds

202064936858448_228917490839859

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    53.170 seconds

202064936858448_230673523997589

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    106.444 seconds

202064936858448_231050590626549

  __init__ (/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py:44):
    58.331 seconds

"""
