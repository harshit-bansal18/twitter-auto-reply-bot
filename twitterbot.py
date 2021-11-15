import time
import tweepy
import pandas as pd
city_db = pd.read_csv('../GitRepo/data/city.csv')




consumer_key = "wy6jVJaAgnj8dW0BTiMBjLAP8"
consumer_secret = "hhq8N7KKLGXQyCgqu4w7AD7x6Ozs9nJdaKPZI1ErAYaeIE3g0O"

access_token= '1388449564751917063-JOxvegsNppwhsP1n8Jfy7sR8H8B8qy'
access_token_secret= 'i1A9RnY5vuRx8pup6JEEAA2K4H66JJuJWbhHd1rpe7xpR'

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
try:
    api = tweepy.API(auth)
    
except ConnectionError :
    print("Error")
def updated_database(link):
  try:
      verf_data = pd.read_json(link)
      supply_data = verf_data.get('data').get('covid')
      upd_data = pd.DataFrame(supply_data)
      upd_city = upd_data['city']
      upd_city = upd_city.str.lower()
      upd_city = upd_city.str.replace(" ","") 
      upd_data['newcity'] = upd_city
      upd_data['prev_used'] = [0]*len(upd_city)
      upd_data['name'] = upd_data['name'].str.replace("#nan","")
      upd_data['name'] = upd_data['name'].str.replace("nan#","")
      upd_data['state'] = upd_data['state'].str.lower()
      upd_data['entity'] = upd_data['entity'].str.replace("_","")
      #print(upd_data['state'])
      #print(upd_data['entity'])
      #print(upd_city)
      return upd_data
  except ConnectionError:
      print("Cannot connect to database.")
      return None




class TimerError(Exception):
    #Exception to handle time related issues
    """Do nothing"""
class Timer :
    def __init__(self):
        self._start_time = None
    def start_timer(self):
        if self._start_time is not None:
            raise TimerError("Timer is alredy running!")
        self._start_time = time.perf_counter()
    def stop(self):
        if self._start_time is None:
            raise TimerError("Timer is not running!")
        elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None
        print("Elapsed Time: {elapsed_time:d} seconds.")
    def get_elapsed_time(self):
        if self._start_time is None:
            raise TimerError("Timer is not running!")
        elapsed_time = (int)(time.perf_counter() - self._start_time)
        return elapsed_time





# OS for getting environment variables
import os

# print("Imported")

def Authenticate():
    print("Getting credentials")
    """consumer_key = ""
    consumer_secret = "ITM3ImSIh014tpMd279VQAm5bGAwb7Lt7r6Uy0UHS3wEccXkNr"

    access_token = "308933143-C4KshNr6qJ76KNphXxwewvfdyBBOjrt9elpTLOAg"
    access_token_secret = "QkEUoWbZN8SXxcajXR0gSI4O3DAINBaosoLKFTTzQp0Iu"""
    consumer_key = "wy6jVJaAgnj8dW0BTiMBjLAP8"
    consumer_secret = "hhq8N7KKLGXQyCgqu4w7AD7x6Ozs9nJdaKPZI1ErAYaeIE3g0O"

    access_token= '1388449564751917063-JOxvegsNppwhsP1n8Jfy7sR8H8B8qy'
    access_token_secret= 'i1A9RnY5vuRx8pup6JEEAA2K4H66JJuJWbhHd1rpe7xpR'


    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    print("Auth object created!")
    auth.set_access_token(access_token, access_token_secret)
    print("Set access token: Done")

    print("Authenticating...")
    api = tweepy.API(auth, wait_on_rate_limit=True)
    try:
        api.verify_credentials()
        print("Connected")
    except:
        print("Authentication error")
    return auth, api


from nltk.tokenize import sent_tokenize, word_tokenize
import string
import nltk

import numpy as np


nltk.download("stopwords")
nltk.download("punkt")

# Read City database and preprocess
# city_db = pd.read_csv("city.csv")

city_db["City"] = city_db["City"].str.lower()
city_db["State"] = city_db["State"].str.lower()
city_db["State"] = city_db["State"].str.replace(" ", "")

commodities = {
    "bed": "Hospital Bed",
    "beds": "beds",
    "icu": "ICU",
    "oxygen": "Oxygen Cylinder",
    "ventilator": "Hospital Bed",
    "ventilators": "Hospital Bed",
    "test": "CoVID Test",
    "tests": "CoVID Test",
    "testing": "CoVID Test",
    "fabiflu": "FabiFlu",
    "remdesivir": "Remdesivir",
    "favipiravir": "FabiFlu",
    "tocilizumab": "Tocilizumab/Altizumab",
    "plasma": "Plasma Donor",
    "tiffin": "Homemade Food",
    "food": "Homemade Food",
    "ambulance": "Ambulance",
}
help_words = ["required", "needed", "need", "needs"]

city_stopwords = set(
    [
        "road",
        "cantonment",
        "chowki",
        "cantt",
        "pur",
        "nagar",
        "estate",
        "north",
        "south",
        "town",
        "gaon",
        "bazar",
        "township",
        "city",
        "camp",
        "old",
        "new",
        "uttar",
        "bad",
    ]
)

import re


def entity_recognition(tokens):
    commodity_required = set()
    for t in tokens:
        if t in commodities:
            commodity_required.add(commodities[t])
    if len(commodity_required) == 0:
        return None

    location = None
    for word in tokens:
        for city1 in set(city_db["City"]):
            if city1 in word.lower():
                if location == None:
                    location = set([city1])
                else:
                    location.add(city1)
        for state1 in set(city_db["State"]):
            if state1 in word.lower():
                if location == None:
                    location = set([state1])
                else:
                    location.add(state1)
        # else:
        #   print('Found username=>',word)

    if location == None:
        return None
    return {"commodities": commodity_required, "location": location}


twitter_re = r"@([A-Za-z0-9_]+)"


def preprocess_tweet(text):
    # Text remove username
    pre_text = re.sub(twitter_re, " ", text)

    # Tokenize
    tokens = word_tokenize(pre_text)
    # python based removal
    # display(f"Tokens: {tokens}")
    tokens_without_punct_python = [t for t in tokens if t not in string.punctuation]
    alphabet_string = string.ascii_lowercase
    alphabet_list = list(alphabet_string)
    word2 = [
        word
        for word in [
            first + second for second in alphabet_list for first in alphabet_list
        ]
    ]
    # display(f"Python based removal: {tokens_without_punct_python}")
    nltk_stop_words = nltk.corpus.stopwords.words("english") + alphabet_list + word2
    text_without_stop_words = [
        t for t in tokens_without_punct_python if t not in nltk_stop_words
    ]
    # display(f"nltk text without stop words: {text_without_stop_words}")
    return text_without_stop_words


def process_tweet(tweet):
    tokens = preprocess_tweet(tweet)
    flag = True
    for t in tokens:
        if t in help_words:
            flag = False
            break
    if flag:
        return
    required = entity_recognition(tokens)
    return required
#start timer to keep track of time. Update database every 6 hours
timer = Timer()
timer.start_timer()
#pull database from api
res = updated_database("https://fierce-bayou-28865.herokuapp.com/api/v1/covid/?page=1&limit=100000")
#implement queue to store tweets and time between 2 consecutive tweets is 2 seconds
from queue import Queue
import threading
tweets_queue = Queue(maxsize=20)
def send_pulled_tweet():
    while True:
        tweet = tweets_queue.get()
        if timer.get_elapsed_time() > 21600:
            timer.stop()
            res = updated_database("https://fierce-bayou-28865.herokuapp.com/api/v1/covid/?page=1&limit=100000")
            timer.start_timer()
        tweet_on_twitter(tweet[0],tweet[1],tweet[2])
        tweets_queue.task_done()
        #time to rest!
        time.sleep(2)
#Thread to start sending pulled to tweets to tweet_on_twitter
threading.Thread(target=send_pulled_tweet,daemon=True).start()
"""########################"""
class TweetStreamListener(tweepy.StreamListener):
    def on_error(self, status_code):
        print("Error Found", status_code)
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False

    def on_status(self, status):
        if  status.user._json.get('screen_name') != 'incovidsupport1':
            if not status.truncated:
                text = status.text
            else:
                text = status.extended_tweet["full_text"]
            required = process_tweet(text)
            if required != None :
                tweet_prev_replied = False
                USER = status.user._json
                for tweet in tweepy.Cursor(api.search,q='to:'+USER.get('screen_name'),result_type='recent',timeout=9999).items(200):
                    if hasattr(tweet,'in_reply_to_status_id_str'):
                        if tweet.in_reply_to_status_id == USER.get('id'):
                            if tweet.user._json.get('screen_name') in ['incovidsupport1','Pratham75355775']:
                                tweet_prev_replied = True
                                break
                if (tweet_prev_replied == False):
                    tweet = [required['location'],required['commodities'],USER.get('screen_name')]
                    tweets_queue.put(tweet)
                    print("###########################################")
                    print(text)
                    print(required["commodities"], required["location"])
                    print(status.user._json)
                    print("###########################################")


auth, api = Authenticate()
tweetStreamListener = TweetStreamListener()
covidTweetStream = tweepy.Stream(auth=api.auth, listener=tweetStreamListener)
covidTweetStream.filter(track=["covid,corona,hospital,bed,oxygen"])
tweets_queue.join()
#######################################
##########
#######





def tweet_on_twitter(city,keyword,username):
    #std_msg = "Please visit www.indiacovidsupport.com for more details."
    output =""
    #res = updated_database("https://fierce-bayou-28865.herokuapp.com/api/v1/covid/?page=1&limit=100000")#process the whole database and get covid related data
    used_list = res['prev_used']
    city_list = res['newcity']
    entity_list = res['entity']
    state_list = res['state']
    city_ind = list(range(0,len(city_list)))
    keyword = ((list)(keyword))[0]
    city = ((list)(city))[0]
    if city == None:
        output = ""
    else:
        city = city.lower().replace(" ","")
        #print(str(city)+". "+str(keyword))
        #print("")
        filter_cities_ind = list(filter(lambda x: str(city_list[x])== str(city) or str(state_list[x]) == str(city) and str(entity_list[x]) == (str(keyword)).lower(),city_ind))
        num_data = len(filter_cities_ind)
        #info = city + ": "
        if num_data > 0:
            output += "Hey @"+ str(username)+ "," + "\n"
            output += "The leads for "+ keyword+ " in "+ city.upper()+":"
            count = 1
            skipped_contacts = 0
            for ind in filter_cities_ind:
                if count > 2:
                    break
                if used_list[ind] == 0 or num_data - skipped_contacts < 3:
                    try:
                        #output += "\n" +"Option"+str(count)+": "
                        contact = res['contact'][ind]
                        name = (str)(res['name'][ind])
                        time = res['filedAt'][ind]
                        if name == None or name == "" or name == "nan":
                            name = "Name not known"
                        output += "\n"+str(contact) + " (" + name+")"
                        
                    except IndexError:
                        print("Couldnt fetch info")
                    count += 1
                else:
                    skipped_contacts += 1
                    used_list[ind] = 0
                    continue
            output += "\n"+"might be helpful. You can check verified live leads on the India COVID Support website indiacovidsupport.com"+"\n"+"#indiacovidsupport #icsiitk #covid #verified"
        else:
            output = ""
        if output != "":
            fil = open("BetaTesting6.txt","a")
            try:
                #api.update_status(output)
                fil.write("Tweeted!")
            except:
                #dfsdf
                print("Not Tweeted!!!")
            
            fil.write(output+"\n")
            fil.close()
            print(output)
