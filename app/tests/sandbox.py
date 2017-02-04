# -*- coding: iso-8859-15 -*-
from pprint import pprint

from bson import ObjectId

from app.database.facebook_objects import FbPosts

post = FbPosts()
post.id = ObjectId("5893bbd54520006bc3fad5c1")
post.created_time = 9991235556703

q = FbPosts.get_posts(profile__id='571227032971640')
pprint(q[398].to_json())
