# -*- coding: iso-8859-15 -*-
from datetime import datetime, timedelta
from random import randint

import pytz
from bson import ObjectId

from app.database.facebook_objects import FbPost

post = FbPost()
post.id = ObjectId("5893bbd54520006bc3fad5c1")
post.created_time = 9991235556703

date_time = datetime(3000, 1, 1, 0, 0, tzinfo=pytz.utc)
for d in [0, 1, 365]:
    for h in [0, 1, 24]:
        td = timedelta(days=d, hours=h)
        dt = date_time + td
        # print d, h, dt
        timestamp = (dt - datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc)).total_seconds()
        pi = randint(1, 2)
        fbp = FbPost(created_time=timestamp, pageid=pi)
        fbp.save(validate=False)

        # print fbp
        # convert to timestamp
# q = FbPost().get_posts(since=0, until=92503683600)
# q = FbPost().get_posts(id=ObjectId("5894d1bc452000658a6e2f61"), since=32503680000, until=32503680000)
q = FbPost().get_posts(since=32503680000, until=32503680000, pageid=2, **{})
# q=q(pageid=1)

for i in q:
    print '--> ', i
