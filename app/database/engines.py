# -*- coding: utf-8 -*-
from datetime import datetime

import pandas as pd
from bson import ObjectId
from mongoengine import register_connection, connect
from mongoengine.context_managers import switch_db
from profilehooks import timecall

from app.database.facebook_objects import FbPosts
from app.database.stats_objects import Pages, Users, PostStats, UserActivity, Contents, Comments
from app.settings import FB_PAGES_LIST


class DbFactory(object):
    users_hashtbl = {}
    pages_hashtbl = {}
    # contents_hashtbl = {}
    comments_hashtbl = {}

    @timecall()
    def __init__(self, fbpost):
        self.fbpost = fbpost
        # Initiate Pages and Content (need oid's)
        self.content = Contents()
        self.content.content_id = self.fbpost.postid
        self.content = self.content.upsert_doc()  # Save to get the oid if exist
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
        page_ref = self.pages_hashtbl.get(self.fbpost.profile.id)
        page.oid = page_ref
        if not page_ref:
            page.page_id = self.fbpost.profile.id
            page.name = self.fbpost.profile.name
            page = page.upsert_doc()
            self.pages_hashtbl[page.page_id] = page.oid  # Update hashtable
        self.page = page
        return None

    def __make_poststat(self):
        poststat = self.poststat  # fix: ugly code
        poststat.created = datetime.fromtimestamp(self.fbpost.created_time)
        poststat.post_type = self.fbpost.type
        poststat.status_type = self.fbpost.status_type
        poststat.page_ref = self.page.oid
        # u_from_ref
        user, _useractivity = self.__make_user(user_id=self.fbpost.from_user.id,
                                               user_name=self.fbpost.from_user.name,
                                               user_picture=None,
                                               date=poststat.created,
                                               action_type='from')
        user_upsdoc = user.to_mongo().to_dict()
        user_upsdoc.update(push__fromed=_useractivity)
        user_upsdoc.update(inc__tot_fromed=1)
        user = user.upsert_doc(ups_doc=user_upsdoc)
        poststat.u_from_ref = user.oid
        # u_to_ref
        u_to_ref = []
        if self.fbpost.to_user:
            for usr in self.fbpost.to_user.get('data'):
                user, _useractivity = self.__make_user(user_id=usr['id'],
                                                       user_name=usr['name'],
                                                       user_picture=None,
                                                       date=poststat.created,
                                                       action_type='from')
                user_upsdoc = user.to_mongo().to_dict()
                user_upsdoc.update(push__toed=_useractivity)
                user_upsdoc.update(inc__tot_toed=1)
                user.upsert_doc(ups_doc=user_upsdoc)
                u_to_ref.append(user.oid)
                poststat.u_to_ref = u_to_ref
        poststat_upsdoc = poststat.to_mongo().to_dict()
        self.poststat = poststat.upsert_doc(ups_doc=poststat_upsdoc)
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
            if hasattr(fbcomment, 'blacklisted)'): continue  # Fix: Blacklisted
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
                                                   user_picture=usr['pic'],
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
        user.oid = self.users_hashtbl.get(user_id)
        user.user_id = user_id
        if not user.oid:  # User doesn't exist
            user.oid = ObjectId()  # Fix: is this slow?
            user.name = user_name
            user.picture = user_picture
            user = user.upsert_doc()
            self.users_hashtbl[user.user_id] = user.oid  # Update hashtable
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


# fix: ERROR : 53668151866_10154153272786867
# KeyError: 'comment_id',
# File "/home/marc/DATA/Projects/FbPageAnalysis/app/database/engines.py", line 169,
# in __make_comment    user_name=fbcom.comment_from.name,

if __name__ == '__main__':
    pass

connect('test3')
# # Initiate hashtables
q = Users.objects.all().only('user_id', 'oid')
DbFactory.users_hashtbl = {user.user_id: user.oid for user in q}
q = Pages.objects.all().only('page_id', 'oid')
DbFactory.pages_hashtbl = {page.page_id: page.oid for page in q}
print len(DbFactory.pages_hashtbl), len(DbFactory.users_hashtbl)

register_connection(alias='politics', name='politics')

for pid in FB_PAGES_LIST:
    with switch_db(FbPosts, 'politics') as FbPostsProduction:
        q = FbPostsProduction.get_posts(pageid=pid, batch_size=1)
    print datetime.now(), 'start: ', pid
    for fb_post in q:
        # print fb_post.postid
        # print fb_post.id
        x = DbFactory(fbpost=fb_post)

        # fix: flag doesn't work
        # print fb_post.flag
        # fb_post.flag = 999
        # fb_post.save()
        #
        # print fb_post.postid
        # print fb_post.id
        # print 1 / 0
