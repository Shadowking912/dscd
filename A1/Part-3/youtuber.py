import sys
import pika
import json
import uuid


def publishVideo(youtuber_name,video_name,channel):
    channel.queue_declare(queue='youtuber_requests')
    # message = f"{self.youtuber_name} uploaded {self.video_name}"
    
    message={
        "youtuber_uuid":f"{uuid.uuid1()}",
        "youtuber_name":f"{youtuber_name}",
        "video_name":f"{video_name}"
    }
    message = json.dumps(message)
    channel.basic_publish(exchange='', routing_key='youtuber_requests', body=message)
    print("SUCCESS: Video published")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python Youtuber.py <YoutuberName> <VideoName>")
    else:
        youtuber_name = sys.argv[1]
        video_name = sys.argv[2]
        youtuber_name = youtuber_name
        video_name = video_name
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        publishVideo(youtuber_name,video_name,channel)
