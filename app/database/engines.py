from app.database.facebook_objects import FbPosts
from app.database.stats_objects import PostStats, Users, Pages, PostTexts


# connect('test')

def create_post_stats(id=None, pageid=None, since=None, until=None, **query):
    q_fb_posts = FbPosts.get_posts(id=id, pageid=pageid, since=since, until=until, **query)

    print 1111111111

    for post in q_fb_posts:
        page = Pages()
        user = Users()
        post_text = PostTexts()
        post_stat = PostStats()

        page.pageid = post.profile.id
        page.name = post.profile.name
        page.users=['x']
        print page




        # PostStats.modify(upsert=True, new=False, remove=False,
        #                          created_time=post.created_time)


        print '.',
    print 22222222222

    return q_fb_posts


if __name__ == '__main__':
    q = create_post_stats(pageid='571227032971640')

    # print q.average('shares.count')
    print q.count()
