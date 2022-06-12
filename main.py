
import os
from google.cloud import pubsub_v1
import sys
import time
from datetime import timedelta
from pip import main
import yaml
from config import read_config
from google.cloud import logging
from sqlalchemy import Column, ForeignKey, Integer, String, DateTime, Boolean, Interval
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import time
import datetime
import feedparser
import re
import urllib.request
import json
import random
from objects.user import User
from objects.wish_list import WishList

logging_client = logging.Client()
log_name = "my-log"
logger = logging_client.logger(log_name)

topic_path = 'projects/images-backup-352308/topics/ds-de-ml-posts'

Base = declarative_base()

class PostDataCollector(object):
    '''
    Collects the data from the posts and store them in a db.
    '''
    def __init__(self):
        self.cfg = read_config()
        self.feed_url_list = self.cfg['feed_url_list']

    def get_links(self):
        post_links = []
        for post in self.feed.entries:
            # We will also remove the ?source=rss .... from the end of link:
            post_link = post.link.split('?source=rss-')[0]
            # Now for every link I need to get the extra information ...
            # We will do that by adding the term `?format=json` to the end of url!

            post_links = post_links + [post_link]
        return post_links

    def discover_post(self):

        blog_url = self.feed_url_list[random.randint(0, len(self.feed_url_list) - 1)]
        logger.log_text('Url is {}'.format(blog_url))

        self.feed = feedparser.parse(blog_url)
        logger.log_text('Here1')

        post_urls = self.get_links()
        logger.log_text('Here2')

        if len(post_urls) > 0:
            post_url = post_urls[random.randint(0, len(post_urls) - 1)]

            post_url_json = post_url + '?format=json'
            post_json = feedparser.parse(post_url_json)
            logger.log_text('Here3')

            #req = urllib.request.Request(post_url_json, headers={'User-Agent': 'Mozilla/5.0'})
            req = urllib.request.Request(post_url_json, headers={ 'User-Agent': 'Mozilla/5.0' })
            NotConnected = True
            while NotConnected:
                try:
                    html = urllib.request.urlopen(req).read()
                    NotConnected = False
                except:
                    time.sleep(self.cfg['other']['sleep_time_when_not_connected'])

            try:
                post_json = json.loads(repair(html))
            except Exception as e:
                logger.log_text('Exception: {}'.format(e))
                post_json = None

            if post_json is not None:
                author, post = retive_stats(post_json, post_url)

                # Publish the post and author to pub/sub
                return post, author

        return None, None
            
    def discover_and_store_posts(self):

        blog_url = self.feed_url_list[random.randint(0, len(self.feed_url_list) - 1)]
        self.feed = feedparser.parse(blog_url)

        post_urls = self.get_links()

        if len(post_urls) > 0:
            post_url = post_urls[random.randint(0, len(post_urls) - 1)]

            post_url_json = post_url + '?format=json'
            post_json = feedparser.parse(post_url_json)

            req = urllib.request.Request(post_url_json, headers={'User-Agent': 'Mozilla/5.0'})
            NotConnected = True
            while NotConnected:
                try:
                    html = urllib.request.urlopen(req).read()
                    NotConnected = False
                except:
                    time.sleep(self.cfg['other']['sleep_time_when_not_connected'])

            try:
                post_json = json.loads(repair(html))
            except:
                post_json = None

            if post_json is not None:
                author, post = retive_stats(post_json, post_url)
                post.author = author

                if post.firstPublishedAt > datetime.datetime.now() - datetime.timedelta(hours=self.cfg['old_if_hours_passed']):
                    post.oldPost = False
                else:
                    post.oldPost = True

class Post(Base):
    __tablename__ = 'post'
    # Here we define columns for the table posts.
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    url = Column(String(1500))
    postedOnTwitter = Column(Boolean, default = False)
    postedOnTelegram = Column(Boolean, default = False)
    postedOnR1 = Column(Boolean, default = False)
    postedOnR2 = Column(Boolean, default = False)
    postedOnR3 = Column(Boolean, default = False)
    postedOnR4 = Column(Boolean, default = False)
    oldPost = Column(Boolean, default = False)
    title = Column(String(1500))
    datetime = Column(DateTime)
    timedelta = Column(Interval)
    createdAt = Column(DateTime)
    firstPublishedAt = Column(DateTime)
    latestPublishedAt = Column(DateTime)
    isSponsored = Column(String(250))
    allowResponses = Column(String(250))
    subtitle = Column(String(250))
    allowNotes = Column(String(250))
    readingList = Column(String(250))
    sectionCount = Column(String(250))
    links = Column(String(250))
    numberOfLinks = Column(Integer)
    imageCount = Column(Integer)
    metaDescription = Column(String(250))
    wordCount = Column(Integer)
    readingTime = Column(String(250))
    tags_dict = Column(String(250))
    isBookmarked = Column(String(250))
    totalClapCount = Column(Integer)
    totalClapCountDelta = Column(Integer)
    usersBySocialRecommends = Column(String(250))
    usersBySocialRecommendsDelta = Column(String(250))
    responsesCreatedCount = Column(String(250))
    responsesCreatedCountDelta = Column(String(250))
    recommends = Column(String(250))
    recommendsDelta = Column(String(250))
    publishedInCount = Column(String(250))
    socialRecommendsCount = Column(Integer)
    socialRecommendsCountDelta = Column(Integer)

def utf8_to_char(_utf8):
    try:
        __utf8 = _utf8.replace("**", "\\x")
        return eval("b'%s'" % __utf8).decode("utf-8")
    except:
        return _utf8


def retive_stats(post_json, post_url):
    current_timestamp = time.time()
    key = list(post_json['payload']['references']['SocialStats'].keys())
    assert len(key) == 1

    # Author data
    username = post_json['payload']['references']['User'][key[0]]['username']
    username = utf8_to_char(username)
    name = post_json['payload']['references']['User'][key[0]]['name']
    name = utf8_to_char(name)
    bio = post_json['payload']['references']['User'][key[0]]['bio']
    bio = utf8_to_char(bio)
    SocialStats = post_json['payload']['references']['SocialStats'][key[0]]
    usersFollowedByCount = SocialStats['usersFollowedByCount']
    usersFollowedCount = SocialStats['usersFollowedCount']

    try:
        twitterScreenName = post_json['payload']['references']['User'][key[0]]['twitterScreenName']
        twitterScreenName = utf8_to_char(twitterScreenName)
    except:
        twitterScreenName = "Not known"

    new_author = Author(name=name,
                        bio=bio,
                        username=username,
                        twitterScreenName=twitterScreenName,
                        usersFollowedByCount=usersFollowedByCount,
                        usersFollowedCount=usersFollowedCount,
                        )

    # Post data
    title = post_json['payload']['value']['title']
    title = utf8_to_char(title)

    if "\\" in title:
        if "Understanding Flutter " not in title:
            assert 1 == 2

    createdAt = datetime.datetime.fromtimestamp(int(str(post_json['payload']['value']['createdAt'])[:-3]))
    firstPublishedAt = datetime.datetime.fromtimestamp(int(str(post_json['payload']['value']['firstPublishedAt'])[:-3]))
    latestPublishedAt = datetime.datetime.fromtimestamp(
        int(str(post_json['payload']['value']['latestPublishedAt'])[:-3]))
    allowResponses = post_json['payload']['value']['allowResponses']
    subtitle = post_json['payload']['value']['virtuals']['subtitle']
    subtitle = utf8_to_char(subtitle)
    isBookmarked = post_json['payload']['value']['virtuals']['isBookmarked']
    allowNotes = post_json['payload']['value']['virtuals']['allowNotes']
    recommends = post_json['payload']['value']['virtuals']['recommends']
    totalClapCount = post_json['payload']['value']['virtuals']['totalClapCount']
    usersBySocialRecommends = post_json['payload']['value']['virtuals']['usersBySocialRecommends']
    usersBySocialRecommends = ", ".join(usersBySocialRecommends)
    readingList = post_json['payload']['value']['virtuals']['readingList']
    sectionCount = post_json['payload']['value']['virtuals']['sectionCount']
    try:
        links = post_json['payload']['value']['virtuals']['links']
        links = [x['url'] for x in links['entries']]
        numberOfLinks = len(links)
        links = ", ".join(links)
    except:
        links = ""
        numberOfLinks = 0
    responsesCreatedCount = post_json['payload']['value']['virtuals']['responsesCreatedCount']
    imageCount = post_json['payload']['value']['virtuals']['imageCount']
    metaDescription = post_json['payload']['value']['virtuals']['metaDescription']
    try:
        publishedInCount = post_json['payload']['value']['virtuals']['publishedInCount']
    except:
        publishedInCount = "Not Known"
    wordCount = post_json['payload']['value']['virtuals']['wordCount']
    readingTime = post_json['payload']['value']['virtuals']['readingTime']
    tags_list = post_json['payload']['value']['virtuals']['tags']
    tags_dict = dict()
    for tag in tags_list:
        # print(tag)
        tags_dict[tag['name']] = tag['postCount']

    tags_dict = ", ".join([k + ":" + str(v) for k, v in tags_dict.items()])
    socialRecommendsCount = post_json['payload']['value']['virtuals']['socialRecommendsCount']

    post = Post(url=post_url,
                title=title,
                createdAt=createdAt,
                datetime=datetime.datetime.now(),
                timedelta=datetime.datetime.now() - datetime.datetime.now(),
                firstPublishedAt=firstPublishedAt,
                latestPublishedAt=latestPublishedAt,
                allowResponses=allowResponses,
                subtitle=subtitle,
                allowNotes=allowNotes,
                readingList=readingList,
                sectionCount=sectionCount,
                links=links,
                numberOfLinks=numberOfLinks,
                imageCount=imageCount,
                metaDescription=metaDescription,
                wordCount=wordCount,
                readingTime=readingTime,
                tags_dict=tags_dict,
                isBookmarked=isBookmarked,
                totalClapCount=totalClapCount,
                usersBySocialRecommends=usersBySocialRecommends,
                responsesCreatedCount=responsesCreatedCount,
                recommends=recommends,
                socialRecommendsCount=socialRecommendsCount,
                publishedInCount=publishedInCount
                )

    return new_author, post


# if post.url not in [x.url for x in author_post_session.query(Post).all()]:

def replace_with_byte(match):
    return chr(int(match.group(0)[1:], 8))

def repair(brokenjson):
    invalid_escape = re.compile(r'\\[0-7]{1,3}')  # up to 3 digits for byte values up to FF
    brokenjson = str(brokenjson)
    brokenjson = brokenjson.replace("b'])}while(1);</x>", "")
    brokenjson = brokenjson.replace("}'", "}")
    brokenjson = brokenjson.replace("\\x", "**")
    brokenjson = brokenjson.replace("\\'", " ")
    brokenjson = brokenjson.replace('\\\"', " ")
    brokenjson = brokenjson.replace('\\', " ")

    return invalid_escape.sub(replace_with_byte, brokenjson)


class Author(Base):
    __tablename__ = 'author'
    # Here we define columns for the table author
    username = Column(String(250), nullable=False)
    bio = Column(String(1500))
    twitterScreenName = Column(String(250))
    id = Column(Integer, primary_key=True)
    usersFollowedByCount = Column(Integer)
    usersFollowedCount = Column(Integer)
    name = Column(String(250), nullable=False)


#credential_path = 'twitterbot-privatekey-352308-b644e7ef2646.json'
#os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path    

#publisher = pubsub_v1.PublisherClient()
#PROJECT_ID = os.getenv('crypto-song-284811')

def hello_world1(request):
    data = request.data
    if data is None: 
        print("request.data is empty")
        return("request.data is empty", 400)
    
    print(f'request data: {data}')
    data_json = json.loads(data)
    print(f'json = {data_json}')

    sensor_name = data_json['sensorName']
    tempetature = data_json['temperature']
    humidity = data_json['humidity']

    print(f'snesor_name = {sensor_name}')
    print(f'temperature = {tempetature}')
    print(f'humidity = {humidity}')

    # Take the data and push it to pub/sub
    topic_path = 'projects/images-backup-352308/topics/ds-de-ml-posts'

    message_json = json.dumps({
        'data' : {'message': 'sensor readings!'},
        'readings' : {
            'sensorName': sensor_name, 
            'temperature': tempetature, 
            'humidity': humidity
        }
    })
    message_bytes = message_json.encode('utf-8')

    try: 
        publish_futute = publisher.publish(topic_path, data = message_bytes)
        publish_futute.result()
    except Exception as e:
        logger.log_text('Exception: {}'.format(e))
        return(e, 500)

    return('Message recived and published to pubsub', 200)

publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.getenv('crypto-song-284811')

def hello_world(request):
    data = request.data
    if data is None: 
        print("request.data is empty")
        return("request.data is empty", 400)

    logger.log_text('Instantiating PostDataCollector()')
    PDC = PostDataCollector()
    logger.log_text('Lets discover some liks and post .... ')
    post, author = PDC.discover_post()

    if post:
        print(post.url)
        message_json = json.dumps({
            'data' : {'message': post.url},
            'readings' : {
                'title': post.title, 
                'author': author.name, 
                'datetime': str(post.datetime),
                'subtitle' : post.subtitle
            }
        })

        message_bytes = message_json.encode('utf-8')
        logger.log_text('message_bytes {}'.format(message_bytes))


        try: 
            publish_futute = publisher.publish(topic_path, data = message_bytes)
            publish_futute.result()
            logger.log_text('Done with publishing the post to pub/sub')
        except Exception as e:
            logger.log_text('Exception: {}'.format(e))
            return(e, 500)

        return('post found and published to pubsub', 200)
    else:
        logger.log_text('No new post found ....')
        return('post not found', 404)
