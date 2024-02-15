import sys
import pika
import json
import uuid

class Youtuber:
    def __init__(self, youtuber_name):
        self.youtuber_name = youtuber_name
        self.subscribers = set()
        self.videos = set()

    def add_subscriber(self, subscriber_name):
        self.subscribers.add(subscriber_name)

    def add_video(self, video_name):
        self.videos.add(video_name)

    def remove_subscriber(self, subscriber_name):
        self.subscribers.discard(subscriber_name)

    def get_subscribers(self):
        return self.subscribers

    def get_name(self):
        return self.youtuber_name

def publish_video(youtuber_name, video_name, channel):
    channel.queue_declare(queue='youtuber_requests')

    message = {
        "youtuber_uuid": str(uuid.uuid1()),
        "youtuber_name": youtuber_name,
        "video_name": video_name
    }
    message = json.dumps(message)
    channel.basic_publish(exchange='', routing_key='youtuber_requests', body=message)
    print("SUCCESS: Video published")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python Youtuber.py <YoutuberName> <VideoName>")
    else:
        youtuber_name = sys.argv[2]
        video_name=""
        # video_name = sys.argv[2]
        for i in range(3,len(sys.argv)):
            video_name+=sys.argv[i]+" "
        server=sys.argv[1]
        connection = pika.BlockingConnection(pika.ConnectionParameters(server, 5672, 'bot', pika.PlainCredentials('bot', 'bot')))
        channel = connection.channel()
        publish_video(youtuber_name, video_name, channel)
