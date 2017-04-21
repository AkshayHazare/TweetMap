
from time import sleep
import json
import boto.sqs
import boto.sns
from boto.sqs.message import Message
import ast
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import sys
from django.views.decorators.csrf import csrf_exempt
from django.views import generic
from django.http import *
from urllib import urlopen
from django.shortcuts import render
import signals


############
host = 'search-twittmap-mtvfhojv7a2yfejtlkpqrvetcy.us-west-2.es.amazonaws.com'
awsauth = AWS4Auth('AKIAJD27C4BSAXDDCOIQ', 'xW9UbWfwYkKekZZgv5HDm10ffOE1WUzwITjyGWu8', 'us-west-2', 'es')
es = Elasticsearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection
)


@csrf_exempt
def sns_subscription(request):
    print ("Handling GET Request")
    if request.method == "GET":
        context = {"title": "Home"}
        return render(request, "index.html", context)
    else:
        headers = json.loads(request.body)
        print("Serving SNS POST Request")
        if 'Type' in headers.keys():
            if headers['Type'] == "SubscriptionConfirmation":
                print("Received Confirmation Request")
                url = headers['SubscribeURL']
                result = urlopen(url).read()
                signals.subscription.send(
                    sender='bouncy_approve_subscription',
                    result=result,
                    notification=headers)
                print ("Subscribed to SNS")

            elif headers['Type'] == "Notification":
                print ("Received a new message: " + str(headers["Message"]))
                message = json.loads(json.loads(headers["Message"]).get('default'))
                try:
                    es.index(index="tweets", doc_type="twitter_twp", body=message)
                except Exception, e:
                    print e.message
        return JsonResponse({'hi': 'hello'})


class IndexView(generic.ListView):
    """
    Creates the view for the index page of the question
    """
    template_name = 'TwittTrends/index.html'

    def get_queryset(self):
        return True


@csrf_exempt
def create_user(request):
    if request.method == 'POST':
        searchStr=request.POST['searchString']
        res = es.search(index="tweets", size = 10000, body={"query": {"match":{"text": "%"+searchStr+"%"}}},)
        print("Got %d Hits:" % res['hits']['total'])
        print res.keys()
        output = []
        for hit in res['hits']['hits']:
            result = {}
            print(hit["_source"].keys())
            if ('sentiment' in hit["_source"].keys() ):
                result['sentiment'] = hit["_source"]['sentiment']
            if ('coordinates' in hit["_source"].keys()):
                if (hit["_source"]['coordinates'] is not None):
                    result['lat'] = hit["_source"]['coordinates']['coordinates'][0]
                    result['lng'] = hit["_source"]['coordinates']['coordinates'][1]
            output.append(result)
    return HttpResponse(json.dumps(output))