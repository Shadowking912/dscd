import pika
import json
import sys
   
class User:
    def __init__(self,username):
        self.username = username
        self.subscriptions=[]
    
    def get_username(self):
        return self.username
    
    def add_subscription(self,youtuber_name):
        self.subscriptions.append(youtuber_name)
    
    def delete_subscription(self,user_name):
        self.subscriptions.remove(user_name)
    
    def get_subscriptions(self):
        return self.subscriptions

    
def updateSubscription(channel, user_name,youtuber_name, subscribe):
    channel.queue_declare(queue='user_requests')
    message = {
        "user_name": user_name,
        "youtuber_name": youtuber_name,
        "subscribe": subscribe
    }
    channel.basic_publish(exchange='', routing_key='user_requests', body=json.dumps(message))
    if youtuber_name!="":
        print("SUCCESS: Subscription updated")
    else:
        print("Logged In")

def receiveNotifications(username,connection):
    def callback(ch,method,properties,body):
        body = body.decode('utf-8')
        print(body)

    channel2  = connection.channel()
    channel2.exchange_declare(exchange='notifications', exchange_type='direct')
    result = channel2.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue
    channel2.queue_bind(exchange='notifications', queue=queue_name, routing_key=username)
    channel2.basic_consume(queue=queue_name,on_message_callback=callback,auto_ack=True)
    channel2.start_consuming()
    
if __name__ == "__main__":
    if len(sys.argv)==2:
        username=sys.argv[1]
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        updateSubscription(connection.channel(),username,"","")
        receiveNotifications(username,connection)

    elif len(sys.argv)==4:
        username=sys.argv[1]
        option=sys.argv[2]
        youtuber_name = sys.argv[3]
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        updateSubscription(channel,username,youtuber_name,option)
        receiveNotifications(username,connection)

    else:
        print("Usage:")
        print("python user.py <username>")
        print("python user.py <username> s/u <youtuber name>")
    

