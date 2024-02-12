import pika
import json
from user import User
from youtuber import Youtuber
import sys
                    
class YoutubeServer:
    def __init__(self,channel,channel2):
        self.channel = channel
        self.channel.queue_declare(queue='user_requests')
        self.channel.queue_declare(queue='youtuber_requests')

        self.youtubers={}
        self.users = {} # Dictionary with key as user name and tuple (1st element of tuple : status (offline/online),2nd element of tuple : list of subscriptions)
        self.channel2 = channel2
        self.channel2.exchange_declare(exchange='notifications',exchange_type='direct')
    
    def consume_youtuber_requests(self): #Instantiates new youtuber
        def callback(ch,method,properties,body):
            body  = body.decode('utf-8')
            message = json.loads(body)
            youtuber_name = message['youtuber_name']
            video_name = message['video_name']

            # Creating an object of the new youtuber
            new_youtuber = Youtuber(youtuber_name)
            if youtuber_name not in self.youtubers:
                # Adding the youtuber object in the dictionary having username as key
                self.youtubers[youtuber_name] = new_youtuber
                new_youtuber.add_video(video_name)  
            else:
                self.youtubers[youtuber_name].add_video(video_name)
            print(f"{youtuber_name} uploaded  {video_name}")
            self.notify_users(self.youtubers[youtuber_name],youtuber_name,video_name)
            
        self.channel.basic_consume(queue='youtuber_requests',on_message_callback=callback,auto_ack=True)
               
        
    
    def consume_user_requests(self):
        def callback(ch,method,properties,body):
            message = body.decode('utf-8')
            message = json.loads(body)
            user_name = message['user_name']
            youtuber_name = message['youtuber_name']
            request_type = message['subscribe']

            if user_name not in self.users:
                user=User(user_name)
                self.users[user_name]=user

            if request_type=='s':
                if(youtuber_name in self.youtubers):
                    self.users[user_name].add_subscription(youtuber_name)
                    self.youtubers[youtuber_name].add_subscriber(user_name)
                    print(f"{user_name} subscribed to {youtuber_name}")
            
            elif request_type=='u':
                if youtuber_name in self.users[user_name].get_subscriptions():
                    self.users[user_name].delete_subscription(youtuber_name)
                    self.youtubers[youtuber_name].remove_subscriber(user_name)
                    print(f"{user_name} unsubscribed to {youtuber_name}")
    
        channel.basic_consume(queue='user_requests',on_message_callback=callback,auto_ack=True)

    def notify_users(self,youtuber,youtuber_name,video_name):
        subscribers = youtuber.get_subscribers()
        for subscriber in subscribers:
            notification =f"{youtuber_name} uploaded {video_name}"
            self.channel2.basic_publish(exchange='notifications',routing_key=subscriber,body=notification) 
       
if __name__ == "__main__":
    host=sys.argv[1]
    connection = pika.BlockingConnection(pika.ConnectionParameters(host))
    channel = connection.channel()
    channel2 = connection.channel()

    server = YoutubeServer(channel,channel2)
    server.consume_user_requests()
    server.consume_youtuber_requests()
    channel.start_consuming()
