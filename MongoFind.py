import datetime
import re
import time

import pymongo
import pandas as pd
from PyKomoran import *
import asyncio

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.8y00i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database('MonggoTest')
collection = db.get_collection('TestData')

from_date = datetime.datetime(2020, 9, 1)
to_date = datetime.datetime(2020, 10, 1)
for post in collection.find({"date": {"$gte": from_date, "$lt": to_date}}):
    print(post)