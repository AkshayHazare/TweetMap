#Import the necessary methods from tweepy library
import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from geopy.geocoders import Nominatim
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from textwrap import TextWrapper
import cgi
import sys
import boto.sqs
from boto.sqs.message import Message
import time
from multiprocessing import *
import threading
import multiprocessing
import boto.sns
import ast

import watson_developer_cloud
import watson_developer_cloud.natural_language_understanding.features.v1 as features

nlu = watson_developer_cloud.NaturalLanguageUnderstandingV1(version='2017-02-27',
                                                            username='86f8e4e7-c5b5-4e04-af59-98cebbc26668',
                                                            password='6hKKOFvJG78s')

conn = boto.sqs.connect_to_region("us-west-2",
aws_access_key_id = 'AKIAJD27C4BSAXDDCOIQ',
aws_secret_access_key = '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')

conn = boto.sqs.connect_to_region("us-west-2")
q = conn.create_queue('tweets-queue')

host = 'search-twittmap-mtvfhojv7a2yfejtlkpqrvetcy.us-west-2.es.amazonaws.com'
awsauth = AWS4Auth('AKIAJD27C4BSAXDDCOIQ','xW9UbWfwYkKekZZgv5HDm10ffOE1WUzwITjyGWu8','us-west-2', 'es')

es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)

#Variables that contains the user credentials to access Twitter API
access_token = "1194341250-VVXcuVmt0naHfNvCFL7kPYINHF0XbQuWebYm7qN"
access_token_secret = "pDDZDUFBYErje7RLnFYYuFfvdl9f5KwKHeHRK3BxngTwd"
consumer_key = "6R8WSg5LtlyIdlT08zdpjXU1S"
consumer_secret = "arA8mSAdUDs4XosjOjdC7YPI4VTpHx7aCFUFbcK2xH1ZML3nIo"

def read_msgs(q):

    while (True):
        c = boto.sns.connect_to_region("us-west-2")
        for m in q.get_messages():
            try:
                my_dict = json.loads(m.get_body())

                if ('coordinates' in my_dict.keys()):
                    if (my_dict['coordinates'] is not None):
                        myText = my_dict['text']
                        response = nlu.analyze(
                            text=myText,
                            features=[features.Sentiment()])
                        status = response.get('sentiment')
                      #  print status
                        if status == 'ERROR':
                            print response['statusInfo']
                        else:
                            my_dict['sentiment'] = status['document']['label']
                            #send the data to elastic search
                            es.index(index="tweets", doc_type="twitter_twp", body=my_dict)
                            topicarn = "arn:aws:sns:us-west-2:150887107369:TweetMap"
                            message = json.dumps(my_dict)
                            message_subject = "tweet"
                            publication = c.publish(topicarn, message, subject=message_subject)
                            print publication
                            q.delete_message(m)
            except Exception, e:
                print e.message

            #print multiprocessing.current_process()
        time.sleep(1)

if __name__ == '__main__':
    #pool = Pool(processes=5)
    pool = multiprocessing.Pool()
    pool.map(read_msgs,(q,q,q,q,q))