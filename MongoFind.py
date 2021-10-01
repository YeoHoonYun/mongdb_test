import datetime
import itertools
import collections
import functools
import operator

import certifi
import pymongo

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.8y00i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority", tlsCAFile=certifi.where())
db = client.get_database('MonggoTest')
collection = db.get_collection('TestData')

contents_type = "twitter"
from_date = datetime.datetime(2020, 1, 1)
to_date = datetime.datetime(2020, 3, 31)

result = list(collection.find(
  {
    # "type":contents_type,
    "date": {
      "$gt": from_date,
      "$lt": to_date
    }
  }
))
words = [x["words"] for x in result]

counter = functools.reduce(
  operator.add,
  (
    # collections.Counter(set(x))
    collections.Counter(x)
    for x in words
  )
)
print(counter.most_common(10))

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