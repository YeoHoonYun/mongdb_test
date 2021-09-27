import datetime
import itertools
import collections
import functools
import operator
import pymongo

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.8y00i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database('MonggoTest')
collection = db.get_collection('TestData')

from_date = datetime.datetime(2020, 1, 1)
to_date = datetime.datetime(2020, 12, 31)

result = list(collection.find({"type":"twitter","date": {"$gte": from_date, "$lt": to_date}}))
words = [x["words"] for x in result]

counter = functools.reduce(
  operator.add,
  (
    # collections.Counter(set(x))
    collections.Counter(x)
    for x in words
  )
)
print(counter)

# query = [
#       {
#         "$match" : {
#           "type" : "twitter",
#           "date" : {
#             "$gte": from_date,
#             "$lt": to_date
#           }
#         }
#       },
#       {
#         "$project": {
#           "_id" : "$words",
#           "count": {
#             "$size":"$words"
#           }
#         }
#       }
#     ]

# for x in collection.aggregate(query):
#   print(x)