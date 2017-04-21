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

conn = boto.sqs.connect_to_region("us-west-2",
aws_access_key_id = 'AKIAJD27C4BSAXDDCOIQ',
aws_secret_access_key = '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')

conn = boto.sqs.connect_to_region("us-west-2")
q = conn.create_queue('tweets-queue')

#Variables that contains the user credentials to access Twitter API
access_token = "1194341250-VVXcuVmt0naHfNvCFL7kPYINHF0XbQuWebYm7qN"
access_token_secret = "pDDZDUFBYErje7RLnFYYuFfvdl9f5KwKHeHRK3BxngTwd"
consumer_key = "6R8WSg5LtlyIdlT08zdpjXU1S"
consumer_secret = "arA8mSAdUDs4XosjOjdC7YPI4VTpHx7aCFUFbcK2xH1ZML3nIo"


#This is a basic listener that just prints received tweets to stdout.
class TListener(StreamListener):

    def on_data(self, data):
        try:
            status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')
            twitt_data = json.loads(data)
            print data
            m = Message()
            if ('coordinates' in twitt_data.keys()):
                if (twitt_data['coordinates'] is not None):
                    tweet = {
                        'id': twitt_data['id'],
                        'time': twitt_data['timestamp_ms'],
                        'text': twitt_data['text'].lower().encode('ascii', 'ignore').decode('ascii'),
                        'coordinates': twitt_data['coordinates'],
                        'place': twitt_data['place'],
                        'handle': twitt_data['user']['screen_name']
                    }
                    m.set_body(json.dumps(tweet))
                    # m.message_attributes = twitt_data
                    q.write(m)
        except Exception, e:
            pass



        return True

    def on_error(self, status):
        print status
        exit()


if __name__ == '__main__':

    l = TListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    try:
        stream.filter(languages=["en"], track=["love","job","the","new york","work","music","happy","photo","like"])
    except Exception, e:
        pass
