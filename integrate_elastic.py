import re
import nltk
import pandas as  pd
from fastapi import FastAPI
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import  word_tokenize
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

nltk.download('punkt')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')

from decouple import config

ES_HOST = config('HOST')
ES_PORT = config('PORT')
ES_USERNAME = config('USERNAME')
ES_PASSWORD = config('PASSWORD')

def preprocessing(text):
    """
    # Python
    def preprocessing(text):
        text = re.sub("[^-9A-Za-z ]", "", text).lower()
        stop = stopwords.words("indonesian")
        tokens = [word for word in (token for token in word_tokenize(text)) if word not in stop]
        return tokens
    
    # The function above is a simple function that takes a text as input and then performs the following
    tasks:
    # 
    # 1. Removes all characters that are not alphanumeric using regex.
    # 2. Converts all characters to lowercase.
    # 3. Removes all stopwords using the stopwords list.
    # 4. Returns a list of tokens.
    #
    :param text: The text to be processed
    :return: The preprocessing function returns a list of tokens.
    """
    text = re.sub("[^-9A-Za-z ]", "", text).lower()
    stop = stopwords.words("indonesian")
    tokens = [word for word in (token for token in word_tokenize(text)) if word not in stop]
    return tokens

def wordFreq(listToken):
    """
    The function takes in a list of tokenized words and returns a dataframe of the most frequent words
    
    :param listToken: a list of lists of tokens
    :return: A list of dictionaries, where each dictionary contains the word and its frequency.
    """
    combToken = []
    for token in listToken:
        combToken += token
    [str(i) for i in combToken]
    counts = Counter(combToken)
    count_df = pd.DataFrame(counts.most_common(), columns = ['Word', 'Count'])
    return count_df.to_dict("records")

from datetime import datetime

def days_between(d1, d2):
    diffDate = abs((d2 - d1).days)
    n_days_ago = d1 - timedelta(days=diffDate)
    return n_days_ago

    
es = Elasticsearch([{'host': '{}'.format(ES_HOST), 'port': ES_PORT}],
                   http_auth= (ES_USERNAME,ES_PASSWORD))

app = FastAPI()

#TOP AUTHOR
@app.get("/top-authors/{projectId}_{start_date}_{end_date}")
def getTopAuthorsKeyword(projectId, start_date, end_date):
    query_body = {
    "sort" : [
        { "followers_count" : "desc" }
    ],
    "query": { 
        "bool": { 
            "must": [
                { "match": { "projectId":"{}".format(projectId) }}
            ],
        "filter": [ 
            { "range": { "created_at": { "gte": "{}".format(start_date) ,
                                        "lt": "{}".format(end_date)
                                        },}}
            ]
        }
    }
    }
    result = es.search(index="sentinel_index", body=query_body)
    result_list = [[data['_source']['followers_count'],
                    data['_source']['name'],
                    data['_source']['screen_name'],
                    data['_source']['statuses_count'],
                    data['_source']['favorite_count'],
                    data['_source']['profile_image_url_https'] 
                    ] for data in result['hits']['hits']]
    result_df = pd.DataFrame(result_list, columns= ['followers_count','name','screen_name','statuses_count','favorite_count','profile_image_url_https'])
    result_list_json = result_df.to_dict("records")
    return result_list_json

#TOP KEYWORDS
@app.get("/top-keywords/{projectId}_{start_date}_{end_date}")
def getTopKeywordUsername(projectId, start_date, end_date):
    query_body = {
    "sort" : [
        { "followers_count" : "desc" }
    ],
    "query": { 
        "bool": { 
            "must": [
                { "match": { "projectId":"{}".format(projectId) }}
            ],
        "filter": [ 
            { "range": { "created_at": { "gte": "{}".format(start_date) ,
                                        "lt": "{}".format(end_date)
                                        },}}
            ]
        }
    }
    }
    result = es.search(index="sentinel_index", body=query_body)
    result_list = [[data['_source']['full_text']
                    ] for data in result['hits']['hits']]
    result_df = pd.DataFrame(result_list, columns= ['full_text'])
    result_df['full_text'] = result_df['full_text'].astype(str)
    freq = result_df['full_text'].apply(preprocessing)
    result_freq = wordFreq(freq)
    return result_freq

#MENTION IN TIME CHART
@app.get("/mention-in-time-chart/{projectId}_{start_date}_{end_date}")
def getMentionInTime(projectId, start_date, end_date):
    query_body = {
    "query": { 
        "bool": { 
            "must": [
                { "match": { "projectId":"{}".format(projectId) }}
            ],
            "filter": [ 
                { "range": { "created_at": { "gte": "{}".format(start_date) ,
                                            "lt": "{}".format(end_date)
                                            },}}
                ]
            }
    }
    }
    result = es.search(index="sentinel_index", body=query_body)
    result_list = [[data['_source']['created_at']
                    ] for data in result['hits']['hits']]
    result_df = pd.DataFrame(result_list, columns= ['created_at'])
    counts = Counter(result_df['created_at'])
    return counts

#MENTION IN TIME CHART BY SENTMENT
@app.get("/mention-in-time-by-sentiment-chart/{projectId}_{start_date}_{end_date}")
def getMentionInTimeBySentiment(projectId, start_date, end_date):
    resultList = []
    sentiment = ['NEUTRAL','POSITIVE','NEGATIVE']
    sentimentIndex = 0
    for i in sentiment: 
        query_body = {
                "query": {
                    "bool": {
                        "filter": [ 
                             { "range": { "created_at": { "gte": "{}".format(start_date) ,
                                            "lt": "{}".format(end_date)
                                            },}}
                                ],
                        "should": {
                            "match": {
                            "projectId": "{}".format(projectId)
                            }
                        },
                        "must": {
                            "bool": {
                            "should": [
                                {
                                "match": {
                                    "sentiment": "{}".format(sentiment[sentimentIndex])
                                    }
                                }
                            ]
                            }
                        }
                }
            }
        }
        result = es.search(index="sentinel_index", body=query_body)
        result_list = [[data['_source']['sentiment']
                    ] for data in result['hits']['hits']]
        result_df = pd.DataFrame(result_list, columns= ['sentiment'])
        counts = Counter(result_df['sentiment'])
        resultList.append(counts)
        sentimentIndex += 1
    return resultList


#BRAND HEALTH INDEX
@app.get("/brand-health-index/{projectId}_{start_date}_{end_date}")
def getMentionInTimeBySentiment(projectId, start_date, end_date):
    resultList = []
    sentiment = ['NEUTRAL','POSITIVE','NEGATIVE']
    sentimentIndex = 0
    for i in sentiment: 
        query_body = {
                "query": {
                    "bool": {
                        "filter": [ 
                             { "range": { "created_at": { "gte": "{}".format(start_date) ,
                                            "lt": "{}".format(end_date)
                                            },}}
                                ],
                        "should": {
                            "match": {
                            "projectId": "{}".format(projectId)
                            }
                        },
                        "must": {
                            "bool": {
                            "should": [
                                {
                                "match": {
                                    "sentiment": "{}".format(sentiment[sentimentIndex])
                                    }
                                }
                            ]
                            }
                        }
                }
            }
        }
        result = es.search(index="sentinel_index", body=query_body)
        result_list = [[data['_source']['sentiment']
                    ] for data in result['hits']['hits']]
        result_df = pd.DataFrame(result_list, columns= ['sentiment'])
        counts = Counter(result_df['sentiment'])
        resultList.append(counts)
        sentimentIndex += 1
    neutral = resultList[0]['NEUTRAL']
    positive = resultList[1]['POSITIVE']
    negative = resultList[2]['NEGATIVE']
    
    try:
        bhi_score = positive/ (positive + negative)
    except ZeroDivisionError:
        bhi_score = 0
            
    return {
        "ok": "true",
        "code" : 200,
        "data" : {
            "score" : bhi_score
        }
    }
    

def resultSummaryBodyReq(type, start_date, end_date, projectId, sentiment):
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    
    n_days_ago = days_between(d1=start_date, d2=end_date)
    
    if type == "now":
        start_date_used = start_date
        end_date_used = end_date
    elif type == "ago":
        start_date_used = n_days_ago
        end_date_used = start_date
    
    query_body = {
                "query": {
                    "bool": {
                        "filter": [ 
                             { "range": { "created_at": { "gte": "{}".format(start_date_used.date()) ,
                                            "lt": "{}".format(end_date_used.date())
                                            },}}
                                ],
                        "should": {
                            "match": {
                            "projectId": "{}".format(projectId)
                            }
                        },
                        "must": {
                            "bool": {
                            "should": [
                                {
                                "match": {
                                    "sentiment": "{}".format(sentiment)
                                    }
                                }
                            ]
                            }
                        }
                }
            }
        }
    result = es.search(index="sentinel_index", body=query_body)
    result_list = [[data['_source']['sentiment']
                ] for data in result['hits']['hits']]
    result_df = pd.DataFrame(result_list, columns= ['sentiment'])
    counts = Counter(result_df['sentiment'])
    return counts
    

def getDirectionAndPercent(now, ago):
    increase = abs(now - ago)
    if now > ago:
        direction = "Up"
        try:
            percent = abs((increase/ago) * 100)
        except ZeroDivisionError:
            percent = None
    elif now < ago:
        direction = "Down"
        try:
            percent = abs((increase/ago) * 100)
        except ZeroDivisionError:
            percent = None
    else:
        direction = "Same"
        try:
            percent = abs((increase/ago) * 100)
        except ZeroDivisionError:
            percent = None
    return direction, percent
        
    

#RESULT SUMMARY
@app.get("/result-summary/{projectId}_{start_date}_{end_date}")
def getResultSummary(projectId, start_date, end_date):
    sentiment = ['NEUTRAL','POSITIVE','NEGATIVE']
    sentimentIndex = 0
    
    resultListNow = []
    resultListAgo = []
    result = es.search(index="sentinel_index")
    value_hits  = result['hits']['total']['value']
    for data in sentiment:
        countsNow = resultSummaryBodyReq("now", start_date, end_date, projectId, data)
        countsAgo = resultSummaryBodyReq("ago", start_date, end_date, projectId, data)
        resultListNow.append(countsNow)
        resultListAgo.append(countsAgo)
        sentimentIndex +=1
    
    
    neutralNow = resultListNow[0]['NEUTRAL']
    positiveNow = resultListNow[1]['POSITIVE']
    negativeNow = resultListNow[2]['NEGATIVE']
    totalNow = positiveNow + negativeNow
    
    neutralAgo = resultListAgo[0]['NEUTRAL']
    positiveAgo = resultListAgo[1]['POSITIVE']
    negativeAgo = resultListAgo[2]['NEGATIVE'] 
    totalAgo = positiveAgo + negativeAgo
    
    totalDirection, totalPercent = getDirectionAndPercent(totalNow, totalAgo)
    positiveDirection, positivePercent = getDirectionAndPercent(positiveNow, positiveAgo)
    negativeDirection, negativePercent = getDirectionAndPercent(negativeNow, negativeAgo)
        
    return {
        "ok": "true",
        "code" : 200,
        "data" : {
            "total" : value_hits,
            "positive": positiveNow,
            "negative": negativeNow,
            "totalDirection": totalDirection,
            "positiveDirection": positiveDirection,
            "negativeDirection": negativeDirection,
            "totalPercent": totalPercent,
            "positivePercent": positivePercent ,
            "negativePercent": negativePercent 
        }
    }
