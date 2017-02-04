from app.database.facebook_objects import FbPosts
from app.database.stats_objects import PostStats, Users, Pages, PostTexts


def create_post_stats():
    fb_posts = FbPosts.get_posts(profile__id='571227032971640')
    users = Users()
    pages = Pages()
    post_stats = PostStats()
    post_texts = PostTexts()

    fb_post=fb_posts[0]
    print fb_post



if __name__ == '__main__':
    create_post_stats()
    pass

