import pika
import json
import sys

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

def receiveNotifications(self):
    def callback(ch, method, properties, body):
        print("New Notification:", body.decode('utf-8'))

    self.channel.basic_consume(queue=self.user_name, on_message_callback=callback, auto_ack=True)
    print("Waiting for notifications...")
    self.channel.start_consuming()

if __name__ == "__main__":
    if len(sys.argv)==2:
        username=sys.argv[1]
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        updateSubscription(channel,username,"","")
        receiveNotifications(username)
        print("typ1")

    elif len(sys.argv)==4:
        username=sys.argv[1]
        option=sys.argv[2]
        youtuber_name = sys.argv[3]
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        updateSubscription(channel,username,option,youtuber_name)
        print("typ2")
    else:
        print("Invalid option")
    updateSubscription(channel,username,"","")
    

