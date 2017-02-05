# ToDo: Move to __init__.py ?

# DATABASE:
TESTING = True
PRODUCTION_DB = 'Production'  # should be 'politics'
# PRODUCTION_DB = 'politics'  # should be 'politics'
if (not TESTING) & (PRODUCTION_DB=='politics'):
    print '*'*100
    print ' '*20 + 'WARNING !!!!'
    print ' '*20 + 'WORKING ON "POLITICS" DATABASE'
    print '*'*100
TESTING_DB = 'test'
