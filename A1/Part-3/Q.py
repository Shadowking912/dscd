import pika
import json
import uuid

# class Youtuber:
#     def __init__(self,youtuber_name,video_name):
#         self.videos=[]
#         self.youtuber_name=youtuber_name
#         self.videos.append(video_name)
    
#     def add_video(self,video_name):
#         self.videos.append(video_name)

class YoutubeServer:
    def __init__(self):

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='user_requests')
        self.channel.queue_declare(queue='youtuber_requests')
        self.channel.queue_declare(queue='notifications')
        self.youtubers={}
        self.users = {} # Dictionary with key as user name and tuple (1st element of tuple : status (offline/online),2nd element of tuple : list of subscriptions)

    def consume_youtuber_requests(self): #Instatties new youtuber
        def callback(ch,method,properties,body):
            body  = body.decode('utf-8')
            message = json.loads(body)
            youtuber_name = message['youtuber_name']
            video_name = message['video_name']
            if youtuber_name not in self.youtubers:
                self.youtubers[youtuber_name] =[video_name]  
            else:
                self.youtubers[youtuber_name].append(video_name)
            print(f"{youtuber_name} uploaded  {video_name}")
            

        self.channel.basic_consume(queue='youtuber_requests',on_message_callback=callback,auto_ack=True)
        self.channel.start_consuming()
    
    # def notify_users(self):
    #     def callback(ch, method, properties, body):
    #         print("New Notification:", body.decode('utf-8'))

    #     self.channel.basic_consume(queue='notifications', on_message_callback=callback, auto_ack=True)
    #     print("Waiting for notifications...")
    #     self.channel.start_consuming()
    
    def consume_user_requests(self):
        def callback(ch,method,properties,body):
            message = body.decode('utf-8')
            message = json.loads(body)
            print(message)
            user_name = message['user_name']
            youtuber_name = message['youtuber_name']
            request_type = message['subscribe']

            if youtuber_name=="":
                if user_name not in self.users:
                    self.users[user_name] =(True,[])
                    print(f"{user_name} logged in")
                else:
                    self.users[user_name][0]=~(self.users[user_name][0])
                    if(not self.users[user_name][0]):
                        print(f"{user_name} logged out")             
            else:
                if user_name not in self.users:
                    self.users[user_name] =(True,[])
                if request_type=='s':
                    self.users[user_name][1].append(youtuber_name)
                    print(f"{user_name} subscribed to {youtuber_name}")
                else:
                    if youtuber_name in self.users[user_name][1]:
                        self.users[user_name][1].remove(youtuber_name)
                        print(f"{user_name} unsubscribed to {youtuber_name}")
    
        self.channel.basic_consume(queue='user_requests',on_message_callback=callback,auto_ack=True)
        self.channel.start_consuming()
        
if __name__ == "__main__":
    server = YoutubeServer()
    server.consume_user_requests()
    server.consume_youtuber_requests()
    # server.notify_users()
'''



'''