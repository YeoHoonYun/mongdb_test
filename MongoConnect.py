import datetime
import re
import time

import pymongo
import numpy as np
import pandas as pd
from PyKomoran import *
import asyncio

async def twiter_async(df_twiter):
    num = 13112
    for id, nickname, date, content in df_twiter.values[13112:]:
        num += 1
        print(id, nickname, date, content)
        try:
            date = datetime.datetime.strptime(str(date).replace("년 ","-").replace("월 ","-").replace("일",""), "%Y-%m-%d") if date != None else None
        except:
            pass
        words = komoran.get_nouns(content) if content != None else None
        data = {
            "num" : num,
            "type": "twitter",
            "id": id,
            "nickname": nickname,
            "date": date,
            "content": content,
            "words": words
        }
        print(data)
        try:
            collection.insert_one(data)
        except:
            pass
        await asyncio.sleep(0.001)

async def insta_async(df_insta):
    num = 0
    for date, content, like, place, tags in df_insta.values:
        num += 1
        # print(date, content, like, place, tags)
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        try:
            data = {
                "num": num,
                "type": "instagram",
                "date": date,
                "content": content,
                "like": like,
                "place": place,
                "tags": tags,
                "words": komoran.get_nouns(content)
            }
            print(data)
            collection.insert_one(data)
        except:
            pass
        await asyncio.sleep(0.001)

    print("twitter exit")

async def youtube_async(df_youtube):
    num = 0
    for title, address, count, like, unlike, comment, main, channel, subscriber in df_youtube.values:
        num += 1
        print(title,address,count,like,unlike,comment,main,channel,subscriber)
        date = "-".join(str(main.split(" • ")[1]).split(".")[0:3]).replace(" ", "").replace("최초공개:","").replace("실시간스트리밍시작일:","")
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        try:
            content = str(main.split(" • ")[2])
            words = komoran.get_nouns(content)
        except:
            content = None
            words = []
        data = {
            "num": num,
            "type": "youtube",
            "title": title,
            "address": "https://www.youtube.com" + address,
            "count": int(re.sub("[^0-9]", "", count)) if count == None else 0,
            "like": int(re.sub("[^0-9]", "", like)) if count == None else 0,
            "unlike": int(re.sub("[^0-9]", "", unlike)) if count == None else 0,
            "comment": int(re.sub("[^0-9]", "", comment)) if count == None else 0,
            "date": date,
            "content": content,
            "channel": channel,
            "subscriber": subscriber,
            "words": words
        }
        print(data)
        try:
            collection.insert_one(data)
        except:
            pass
        await asyncio.sleep(0.001)
    print("youtube exit")

async def news_total_async(df_nt):
    num = 0
    for article, date, url, title, content in df_nt.values:
        num += 1
        print(article, date, url, title, content)
        try:
            words = komoran.get_nouns(content)
        except:
            content = []

        date = "-".join(date.split(".")[0:3])
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        data = {
            "num": num,
            "type": "news_total",
            "article": article,
            "date": date,
            "url": url,
            "title": title,
            "content": content,
            "words": words
        }
        print(data)
        try:
            collection.insert_one(data)
        except:
            pass
        await asyncio.sleep(0.001)
    print("news total exit")

async def news_comment_async(df_nc):
    num = 0
    for id,date_to,content,url in df_nc.values[6551:]:
        num += 1
        # print(id,date_to,content,url)
        date = "-".join(str(date_to).split(" ")[0].split(".")[0:3])
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        data = {
            "num": num,
            "type": "news_comment",
            "id": id,
            "date": date,
            "url": url,
            "content": content,
            "words": komoran.get_nouns(content)
        }
        print(data)
        collection.insert_one(data)
        await asyncio.sleep(0.001)
    print("news comment exit")

async def naver_blog_async(df_nb):
    num = 0
    for title,nickname,date,content in df_nb.values:
        num += 1
        print(title,nickname,date,content)
        date = "%s-%s-%s" % (date.split(". ")[0], str(date.split(". ")[1]).zfill(2), date.split(". ")[2].zfill(2))
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        try:
            words = komoran.get_nouns(content)
        except:
            content = []
        data = {
            "num": num,
            "type": "naver_blog",
            "title": title,
            "nickname":nickname,
            "date": date,
            "content": content,
            "words": words
        }
        print(data)
        try:
            collection.insert_one(data)
        except:
            pass
        await asyncio.sleep(0.001)
    print("news blog exit")

komoran = Komoran("EXP")

client = pymongo.MongoClient("mongodb+srv://admin:admin@cluster0.8y00i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.get_database('MonggoTest')
collection = db.get_collection('TestData')

db['TestData'].create_index(
    [("num", pymongo.DESCENDING),("date", pymongo.DESCENDING), ("type", pymongo.ASCENDING)],
    unique=True
)

# 트위터
df_twiter = pd.read_csv("data/twit_list.csv", encoding="utf-8-sig")
df_twiter = df_twiter.where(pd.notnull(df_twiter), None)
print(df_twiter.keys())

# # 인스타그램
# df_insta = pd.read_excel("data/인스타그램_v1.0.xlsx", encoding="utf-8-sig")
# print(df_insta.keys())

# # 유튜브
# df_youtube = pd.read_excel("data/youtubeCrawling_최종.xlsx", encoding="utf-8-sig")
# df_youtube = df_youtube.where(pd.notnull(df_youtube), None)
# print(df_youtube.keys())

# # news_total
# df_nt = pd.read_excel("data/news_total.xlsx", encoding="utf-8-sig")
# print(df_nt.keys())

# # news_comment
# df_nc = pd.read_excel("data/news_comment.xlsx", encoding="utf-8-sig")
# print(df_nc.keys())

# # naver_blog
# df_nb = pd.read_excel("data/naver_blog_result.xlsx", encoding="utf-8-sig")
# print(df_nb.keys())

async def process_async():
    start = time.time()
    await asyncio.wait([
        twiter_async(df_twiter[["id","nickname","date","contents"]]),
        # youtube_async(df_youtube[["제목","주소","조회수","좋아요","싫어요", "댓글수",'본문', '채널명','구독자수']]),
        # insta_async(df_insta[["data","content","like","place","tags"]]),
        # news_total_async(df_nt[["gubn", "date", "url", "title", "contents"]]),
        # news_comment_async(df_nc[["id","date_to","context","url"]]),
        # naver_blog_async(df_nb[["title","nickname","date","content"]]),
    ])
    end = time.time()
    print(f'>>> 비동기 처리 총 소요 시간: {end - start}')

if __name__ == '__main__':
    asyncio.run(process_async())

# collection.delete_many({
#     'type': 'twitter'
# })

# from_date = datetime.datetime(2020, 1, 1)
# to_date = datetime.datetime(2020, 12, 31)
# for post in collection.find({"date": {"$gte": from_date, "$lt": to_date}}):
#     print(post)