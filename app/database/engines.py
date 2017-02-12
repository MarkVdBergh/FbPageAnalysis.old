# -*- coding: utf-8 -*-
from datetime import datetime
from pprint import pprint

import pandas as pd
from bson import ObjectId
from mongoengine import register_connection, connect
from mongoengine.context_managers import switch_db
from profilehooks import timecall

from app.database.facebook_objects import FbPosts
from app.database.stats_objects import Pages, Users, PostStats, Snippets, UserActivity


@timecall()
def engine(fbpost):
    # Todo: Make values for fields consistent (not fbpost.xxx somewhere and poststat.xxx elseware
    # Create new 'poststat' document
    poststat = PostStats()
    poststat.post_id = fbpost.postid
    poststat.created = datetime.fromtimestamp(fbpost.created_time)  # No 'tz=pytz.utc' because Mongo stores a naive ISODate
    poststat.post_type = fbpost.type

    poststat.status_type = fbpost.status_type
    # page_ref
    page_ref = pages_hashtbl.get(fbpost.profile.id)
    if not page_ref:
        # Create a new 'page" document
        page = Pages()
        # page.oid = ObjectId()
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
    # Save to get oid
    poststat=poststat.upsert_doc(poststat_upsdoc)


    # u_from_ref
    user_ref = users_hashtbl.get(fbpost.from_user.id)
    # Create new user, for update only. Don't get existing from db otherwise conflict with existing keys that will be updated
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
    print page_ref
    print poststat.oid
    # print 1/0
    _useractivity.page_ref = page_ref
    _useractivity.poststat_ref = poststat.oid
    _useractivity.own_page = user.user_id == fbpost.profile.id
    # Save update/new user
    user_upsdoc = user.to_mongo().to_dict()
    user_upsdoc.update(push__fromed=_useractivity)
    user_upsdoc.update(inc__tot_fromed=1)
    user.upsert_doc(ups_doc=user_upsdoc)
    poststat_upsdoc.update(u_from_ref=user_ref)

    # u_to_ref
    u_to_ref = []
    if fbpost.to_user:
        for _usr in fbpost.to_user.get('data'):
            user_ref = users_hashtbl.get(_usr['id'])
            user = Users()
            user.oid = user_ref
            user.user_id = _usr['id']
            # Todo: make separate function. Is the almost same as above
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
    if fbpost.message:
        print 'message: ', fbpost.message
        snippet_ref = snippets_hashtbl.get(fbpost.postid)
        mes_snippet = Snippets()
        mes_snippet.oid = snippet_ref
        mes_snippet.snippet_id = fbpost.postid
        if not snippet_ref:  # Snippet doesn't exist
            mes_snippet.oid = ObjectId()
            mes_snippet = mes_snippet.upsert_doc()
            # Update hashtable
            snippet_ref = mes_snippet.oid
            snippets_hashtbl[mes_snippet.snippet_id] = snippet_ref
        mes_snippet.created = poststat.created
        mes_snippet.snip_type = 'message'
        mes_snippet.user_ref = poststat.u_from_ref
        mes_snippet.page_ref = poststat.page_ref
        mes_snippet.poststat_ref = poststat.oid
        mes_snippet.text = fbpost.message
        mes_snippet.nb_reactions = len(fbpost.reactions)
        mes_snippet.nb_comments = len(fbpost.comments)
        mes_snippet.nb_shares = fbpost.shares['count']
        mes_snippet_upsdoc = mes_snippet.to_mongo().to_dict()
        mes_snippet.upsert_doc(mes_snippet_upsdoc)
        poststat_upsdoc.update(s_message_ref=mes_snippet.oid)

    # s_story_ref
    if fbpost.story:
        print 'story: ', fbpost.story
        snippet_ref = snippets_hashtbl.get(fbpost.postid)
        stor_snippet = Snippets()
        stor_snippet.oid = snippet_ref
        stor_snippet.snippet_id = fbpost.postid
        if not snippet_ref:  # Snippet doesn't exist
            stor_snippet.oid = ObjectId()
            stor_snippet = stor_snippet.upsert_doc()
            # Update hashtable
            snippet_ref = stor_snippet.oid
            snippets_hashtbl[stor_snippet.snippet_id] = snippet_ref
        stor_snippet.created = poststat.created
        stor_snippet.snip_type = 'story'
        stor_snippet.user_ref = poststat.u_from_ref
        stor_snippet.page_ref = poststat.page_ref
        stor_snippet.poststat_ref = poststat.oid
        stor_snippet.text = fbpost.story
        stor_snippet.nb_reactions = len(fbpost.reactions)
        stor_snippet.nb_comments = len(fbpost.comments)
        stor_snippet.nb_shares = fbpost.shares['count']
        stor_snippet_upsdoc = stor_snippet.to_mongo().to_dict()
        stor_snippet.upsert_doc(stor_snippet_upsdoc)
        poststat_upsdoc.update(s_story_ref=stor_snippet.oid)

    # s_post_name_ref
    if fbpost.name:
        print 'name: ', fbpost.name
        snippet_ref = snippets_hashtbl.get(fbpost.postid)
        name_snippet = Snippets()
        name_snippet.oid = snippet_ref
        name_snippet.snippet_id =fbpost.postid
        if not snippet_ref:  # Snippet doesn't exist
            name_snippet.oid = ObjectId()
            name_snippet = name_snippet.upsert_doc()
            # Update hashtable
            snippet_ref = name_snippet.oid
            snippets_hashtbl[name_snippet.snippet_id] = snippet_ref
        name_snippet.created = poststat.created
        name_snippet.snip_type = 'name'
        name_snippet.user_ref = poststat.u_from_ref
        name_snippet.page_ref = poststat.page_ref
        name_snippet.poststat_ref = poststat.oid
        name_snippet.text = fbpost.name
        name_snippet.nb_reactions = len(fbpost.reactions)
        name_snippet.nb_comments = len(fbpost.comments)
        name_snippet.nb_shares = fbpost.shares['count']
        name_snippet_upsdoc = name_snippet.to_mongo().to_dict()
        name_snippet.upsert_doc(name_snippet_upsdoc)
        poststat_upsdoc.update(s_post_name_ref=name_snippet.oid)

    poststat_upsdoc.update(link=fbpost.link)
    poststat_upsdoc.update(picture_link=fbpost.picture)
    poststat_upsdoc.update(nb_shares=fbpost.shares['count'])

    # u_reacted / nb_reactions
    _reacts = [fbpost.reactions[r].to_mongo().to_dict() for r in xrange(len(fbpost.reactions))]  # Fix: only needed to convert EmbeddedList to list of dics
    if _reacts:
        if _reacts[0].keys() != ['blacklisted']:  # Fix: Scraping
            _df_reacts = pd.DataFrame(_reacts)
            _df_reacts['type'] = _df_reacts['type'].str.lower()  # LIKE->like
            _dfg_reacts = _df_reacts.groupby(['type'])  # tuple of (str,df)
            # set the count per type
            poststat_upsdoc.update(reactions=_dfg_reacts['id'].count().to_dict())
            poststat_upsdoc.update(nb_reactions=sum(poststat_upsdoc.get('reactions').values()))
            reacted = {}  # for 'poststat.u_reacted'
            # Iterate reactions and extract userdata
            for i, usr in _df_reacts.iterrows():  # row is pandas.Series
                user_ref = users_hashtbl.get(usr.id)
                # Create new user, for update only. Don't get existing from db otherwise conflict with existing keys that will be updated
                user = Users()
                user.oid = user_ref
                user.user_id = usr['id']
                if not user_ref:  # User doesn't exist
                    user.oid = ObjectId()
                    user.name = usr['name']
                    user = user.upsert_doc()
                    # Update hashtable
                    users_hashtbl[user.user_id] = user.oid
                    user.picture = usr['pic']
                # Users / fromed
                _useractivity = UserActivity()
                _useractivity.date = poststat.created
                _useractivity.type = 'reaction'
                _useractivity.sub_type = usr['type']
                _useractivity.page_ref = page_ref
                _useractivity.poststat_ref = poststat.oid
                _useractivity.own_page = user.user_id == fbpost.profile.id
                # Save update/new user
                user_upsdoc = user.to_mongo().to_dict()
                user_upsdoc.update(push__reacted=_useractivity)
                user_upsdoc.update(inc__tot_reactions=1)
                user.upsert_doc(ups_doc=user_upsdoc)
                # Add user.oid to the correct list in 'poststat.u_reacted'
                # see https://docs.quantifiedcode.com/python-anti-patterns/correctness/not_using_setdefault_to_initialize_a_dictionary.html
                reacted.setdefault(usr['type'], []).append(user.oid)
            poststat_upsdoc.update(u_reacted=reacted)
    poststat.upsert_doc(poststat_upsdoc)

    #
    #         user = Users()
    #         user.user_id = usr['id']
    #         user.name = row['name']
    #         user.picture = row['pic']
    #         # Need to upsert with a dictionary because we use 'inc__' etc
    #         ups_doc = dict(inc__tot_reactions=1,
    #                        add_to_set__pages_active=fbpost.profile.id)  # Fix: should be better with ObjectId.
    #         ups_doc.update(user.to_mongo().to_dict())
    #         user.upsert_doc(ups_doc)
    # else:  # set reactions to [] to fix 'blacklisted'
    #     FbPosts(id=fbpost.oid).update(reactions=[])
    #







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

    #
    #         FbPosts(id=fbpost.id).update(flag=1)
    #     if iii % 100 == 0:
    #         print datetime.now(), iii
    #     iii += 1
    # print datetime.now(), 'end: ', pid


    if __name__ == '__main__':
        pass



connect('test3')
q = Users.objects.all().only('user_id', 'oid')
users_hashtbl = {user.user_id: user.oid for user in q}
q = Pages.objects.all().only('page_id', 'oid')
pages_hashtbl = {page.page_id: page.oid for page in q}
q = Snippets.objects.all().only('snippet_id', 'oid')
snippets_hashtbl = {snippet.snippet_id: snippet.oid for snippet in q}
# q = PostStats.objects.all().only('post_id', 'oid')
# poststats_hashtbl = {snippet.snippet_id: snippet.oid for snippet in q}

print len(pages_hashtbl)
print len(users_hashtbl)
print len(snippets_hashtbl)


# print snippets_hashtbl


register_connection(alias='politics', name='politics')
# for pid in FB_PAGES_LIST:
for pid in ['202064936858448']:
    with    switch_db(FbPosts, 'politics') as FbPostsProduction:
        q = FbPostsProduction.get_posts(pageid=pid, batch_size=10)
        pprint(q.explain())


    print datetime.now(), 'start: ', pid
    for fb_post in q:
        engine(fbpost=fb_post)
    # fb_post.update(flag=1)
