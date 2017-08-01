import json
import tweepy
import time
import configparser
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

class TweeterStreamListener(tweepy.StreamListener):
    
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        self.producer = KafkaProducer(bootstrap_servers='ip-172-31-19-70.ec2.internal:6667')

    def on_data(self, data):
        msg = json.loads(data)[u'text']
        msg = json.dumps(msg).encode('utf-8')              
        print(msg)
        try:
            self.producer.send('twitter', msg)
        except KafkaError:
            log.exception()
            pass
        except Exception as e:
            print(e)
            return False
        return True

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False
        print("Error received in kafka producer")
        print(status_code)
        return True

    def on_timeout(self):
        return True 
    
if __name__ == '__main__':

    # Get the authorization keys
    config = configparser.ConfigParser()
    config.read_file(open('/home/ec2-user/datapipeline/stream/twitter-app-credentials'))
    #consumer_key = config.defaults()['consumerkey']
    consumer_key = 'z5lTf3sAbJDjGnGFsRFcvv1MK'
    consumer_secret = 'HbXFtsDHCuFllDI2MBhxjUThK0Yv96JuPzn3lf3aHmVNpZCI0V'
    access_key = '852055580361588736-vN0Fn0AjT7zCnRZ4Ljj7pQc4r06dtGd'
    access_secret = 'JZlVDeK9PDJhlj0ndJrDGLoYD9q4JmtZBBmZsLFYEE1V7'
    #consumer_secret = config.defaults()['consumersecret']
    #access_key = config.defaults()['accesstoken']
    #access_secret = config.defaults()['accesstokensecret']

    # Call twitter api using Tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
        
    # Start listening
    listener_twitter = TweeterStreamListener(api)
    stream = tweepy.Stream(auth, listener = listener_twitter)
    
    # Filter tweets
    stream.filter(track=['flight', 'aviation'], languages = ['en'], async=True)
