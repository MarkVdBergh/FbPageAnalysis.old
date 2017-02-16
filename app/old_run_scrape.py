import datetime
import sys
import time

from app.database import old_engine
from old_settings import PAGE_LIST
from tools.old_general_tools import utc_now

pageids = PAGE_LIST


# Todo: when error it sleeps for 600sec, then start over from the beginning
print "CORRECT THE LISTS !!!!"
pageids_test = ['596946040405796']
resume = False

since= datetime.datetime.today() - datetime.timedelta(days=14)
ENG = old_engine.Engine(since=(since.year, since.month, since.day))

list_nr = int(raw_input('Enter (1:politics, 2:news, id): '))
if list_nr == 0:
    page_ids = pageids
    bulkdays = 2
    lst = 'ALL'
elif list_nr == 1:
    page_ids = pageids_pol
    bulkdays = 3
    lst = 'POLITICS'
elif list_nr == 2:
    page_ids = pageids_news
    bulkdays = 1
    lst = 'NEWS'
elif list_nr > 1000:
    page_ids = ['{}'.format(list_nr)]
    bulkdays = int(raw_input('Enter bulkdays: '))
    lst = list_nr
else:
    page_ids = ['37823307325']
    bulkdays = 2
    lst = 'TEST'
i = 0
print '\n{} Start scraping list: {}. Attempt: {}\n'.format(utc_now(), lst, i)
while i < 1:
    try:
        result = ENG.run_scraping(pageidlist=page_ids, resume=resume, bulkdays=bulkdays)
        if result:
            print '{} End scraping list {}'.format(utc_now(), lst)
            break
    except:
        e = sys.exc_info()[0]
        print e
        i += 1
        time.sleep(600)

# ENG.run_scraping(pageidlist=['446368145415026'], resume=False, bulkdays=30)
