# -*- coding: utf-8 -*-
from datetime import datetime

from bson import ObjectId
from mongoengine import register_connection, connect
from mongoengine.context_managers import switch_db

from app.database.facebook_objects import FbPosts
from app.database.stats_objects import Pages, Users, PostStats, Snippets, UserActivity
from app.settings import FB_PAGES_LIST

register_connection(alias='politics', name='politics')
connect('test')

q = Users.objects.all().only('user_id', 'oid')
users_hashtbl = {user.user_id: user.oid for user in q}
q = Pages.objects.all().only('page_id', 'oid')
pages_hashtbl = {user.page_id: user.oid for user in q}
print users_hashtbl
print pages_hashtbl


def engine(fbpost):
    # Create new 'poststat' document
    poststat = PostStats()
    poststat.oid = ObjectId()
    poststat.post_id = fbpost.postid
    poststat.created = datetime.fromtimestamp(fbpost.created_time)  # No 'tz=pytz.utc' because Mongo stores a naive ISODate
    poststat.type = fbpost.type
    poststat.status_type = fbpost.status_type
    # page_ref
    page_ref = pages_hashtbl.get(fbpost.profile.id)
    if not page_ref:
        # Create a new 'page" document
        page = Pages()
        page.oid = ObjectId()
        page.page_id = fbpost.profile.id
        page.name = fbpost.profile.name
        # And save it
        page = page.upsert_doc()
        # Update hashtable
        page_ref = page.oid
        pages_hashtbl[page.page_id] = page_ref
    poststat.page_ref = page_ref
    # Create the upsert document for poststat
    poststat_upsdoc = poststat.to_mongo().to_dict()

    # u_from_ref
    user_ref = users_hashtbl.get(fbpost.from_user.id)
    # Create new user, for update. Don't get existing from db otherwise conflict with existing keys that will be updated
    user = Users()
    user.oid = user_ref
    user.user_id = fbpost.from_user.id
    if not user_ref:  # User doesn't exist
        user.oid = ObjectId()
        user.name = fbpost.from_user.name
        user = user.upsert_doc()
        # Update hashtable
        user_ref = user.oid
        users_hashtbl[user.user_id] = user_ref
    # Users / fromed
    _useractivity = UserActivity()
    _useractivity.date = poststat.created
    _useractivity.type = 'from'
    _useractivity.page_ref = page_ref
    _useractivity.poststat_ref = poststat.oid
    _useractivity.own_page = user.user_id == fbpost.profile.id
    # Save update/new user
    user_upsdoc = user.to_mongo().to_dict()
    user_upsdoc.update(push__fromed=_useractivity)
    user_upsdoc.update(inc__tot_fromed=1)
    user.upsert_doc(ups_doc=user_upsdoc)

    poststat_upsdoc.update(u_from_ref=user_ref)

    #fix: toed is with the pageowner, not the user !
    # u_to_ref
    u_to_ref = []
    if fbpost.to_user:
        for _usr in fbpost.to_user.get('data'):
            user_ref = users_hashtbl.get(_usr['id'])
            user = Users()
            user.oid = user_ref
            user.user_id = _usr['id']
            # Todo: make separate function. Is the same as above
            if not user_ref:  # If user doesn't exists, make it
                user.oid = ObjectId()
                user.name = _usr['name']
                user = user.upsert_doc()
                # Update hashtable
                user_ref = user.oid
                users_hashtbl[user.user_id] = user_ref
            # Add to the luist
            # Users / toed
            _useractivity = UserActivity()
            _useractivity.date = poststat.created
            _useractivity.type = 'to'
            _useractivity.page_ref = page_ref
            _useractivity.poststat_ref = poststat.oid
            _useractivity.own_page = user.user_id == fbpost.profile.id
            user_upsdoc = user.to_mongo().to_dict()
            user_upsdoc.update(push__toed=_useractivity)
            user_upsdoc.update(inc__tot_toed=1)
            user.upsert_doc(ups_doc=user_upsdoc)

    u_to_ref.append(user_ref)
    poststat_upsdoc.update(push_all__u_to_ref=u_to_ref)

    # s_message_ref
    mes_snippet = Snippets()
    mes_snippet.oid = ObjectId()
    mes_snippet.snippet_id = '{}_message'.format(poststat.post_id)
    mes_snippet.created = poststat.created
    mes_snippet.snip_type = 'message'
    mes_snippet.user_ref = poststat.u_from_ref
    mes_snippet.page_ref = poststat.page_ref
    mes_snippet.poststat_ref = poststat.oid
    mes_snippet.text = fbpost.message
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    mes_snippet.nb_reactions = 'xxx'
    mes_snippet.nb_comments = 'xxx'
    mes_snippet.nb_shares = 'xxx'

    # mes_snippet.upsert_doc()
    poststat.s_message_ref = mes_snippet.oid
    # s_story_ref
    stor_snippet = Snippets()
    stor_snippet.oid = ObjectId()
    stor_snippet.snippet_id = '{}_storry'.format(poststat.post_id)
    stor_snippet.created = poststat.created
    stor_snippet.snip_type = 'storry'
    stor_snippet.user_ref = poststat.u_from_ref
    stor_snippet.page_ref = poststat.page_ref
    stor_snippet.poststat_ref = poststat.oid
    stor_snippet.text = fbpost.story
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    stor_snippet.nb_reactions = 'xxx'
    stor_snippet.nb_comments = 'xxx'
    stor_snippet.nb_shares = 'xxx'
    # stor_snippet.upsert_doc()
    poststat.s_story_ref = stor_snippet.oid
    poststat.link = fbpost.link
    # s_post_name_ref
    name_snippet = Snippets()
    name_snippet.oid = ObjectId()
    name_snippet.snippet_id = '{}_name'.format(poststat.post_id)
    name_snippet.created = poststat.created
    name_snippet.snip_type = 'storry'
    name_snippet.user_ref = poststat.u_from_ref
    name_snippet.page_ref = poststat.page_ref
    name_snippet.poststat_ref = poststat.oid
    name_snippet.text = fbpost.name
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    name_snippet.nb_reactions = 'xxx'
    name_snippet.nb_comments = 'xxx'
    name_snippet.nb_shares = 'xxx'
    # name_snippet.upsert_doc()
    poststat.s_post_name_ref = name_snippet.oid
    poststat.picture_link = fbpost.picture
    poststat.shares = fbpost.shares['count']






    #     poststat.upsert_doc(poststat_upsdoc)
    #
    #     s_post_name_ref = ObjectIdField()
    #     poststat.picture_link = fbpost.picture
    #
    #     poststat.nb_shares = fbpost.shares
    #     u_reacted = MapField(ListField(), default=None)  # {like:[ObjectId], ...}
    #     nb_reactions = IntField()
    #     s_comments = ListField()
    #     u_commented = ListField  # [ObjectId(), ...]
    #     u_comment_liked = ListField()
    #     nb_comment_likes = IntField()
    #     nb_comments = IntField()
    #
    #     user.user_id = fbpost.from_user.oid
    #     user.name = fbpost.from_user.name
    #     post.from_ref = user.upsert_doc().oid
    #
    #     poststat.stat_id = fbpost.postid
    #     poststat.shares = fbpost.shares['count']
    #
    #     _reacts = [fbpost.reactions[r].to_mongo().to_dict() for r in xrange(len(fbpost.reactions))]  # Fix: only needed to convert EmbeddedList to list of dics
    #     if _reacts:
    #         if _reacts[0].keys() != ['blacklisted']:  # Fix: Scraping
    #             _df_reacts = pd.DataFrame(_reacts)
    #             _df_reacts['type'] = _df_reacts['type'].str.lower()  # LIKE->like
    #             _dfg_reacts = _df_reacts.groupby(['type'])  # tuple of (str,df)
    #             # set the count per type
    #             poststat.reactions = _dfg_reacts['id'].count().to_dict()
    #             poststat.nb_reactions = sum(poststat.reactions.values())
    #             # Iterate reactions and extract userdata
    #             for i, row in _df_reacts.iterrows():  # u is pandas.Series
    #                 user = Users()
    #                 user.user_id = row['id']
    #                 user.name = row['name']
    #                 user.picture = row['pic']
    #                 # Need to upsert with a dictionary because we use 'inc__' etc
    #                 ups_doc = dict(inc__tot_reactions=1,
    #                                add_to_set__pages_active=fbpost.profile.oid)  # Fix: should be better with ObjectId.
    #                 ups_doc.update(user.to_mongo().to_dict())
    #                 user.upsert_doc(ups_doc)
    #         else:  # set reactions to []
    #             FbPosts(id=fbpost.oid).update(reactions=[])
    #
    #         # Message
    #         sn = Snippets()
    #         sn.snippet_id = fbpost.postid
    #         sn.created = datetime.fromtimestamp(fbpost.created_time, tz=pytz.utc)
    #         sn.snip_type = fbpost.type
    #         sn.author = post.from_ref
    #         sn.poststat_ref = post.upsert_doc().oid
    #         sn.text = fbpost.message
    #         sn.reactions = poststat.nb_reactions
    #         sn.shares = poststat.shares
    #
    #         post.message_ref = sn.upsert_doc().oid  # Fix: mongoengine.errors.ValidationError: u'231742536958_198055708174' is not a valid ObjectId, it must be a 12-byte input or a 24-character hex string
    #
    #         poststat.post_id = post.upsert_doc().oid
    #         poststat.page_id = page.upsert_doc().oid
    #         poststat.upsert_doc()
    #
    #         # Mark fbpost as done
    #         # fbpost.flag=1
    #         FbPosts(id=fbpost.oid).update(flag=1)
    #     if iii % 100 == 0:
    #         print datetime.now(), iii
    #     iii += 1
    # print datetime.now(), 'end: ', pid


if __name__ == '__main__':
    # user_hashtbl = UsersHashtable()
    # page_hashtbl = PagesHashtable()
    for pid in FB_PAGES_LIST:
        with switch_db(FbPosts, 'politics') as FbPostsProduction:
            q = FbPostsProduction.get_posts(flag=0, pageid=pid, batch_size=100)  # Tweak: check optimal batch_size or convert to list
        print datetime.now(), 'start: ', pid
        for fb_post in q:
            engine(fbpost=fb_post)
