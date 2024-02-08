import pika
import sys

class Youtuber:
    def __init__(self, youtuber_name, video_name):
        self.youtuber_name = youtuber_name
        self.video_name = video_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

    def publishVideo(self):
        self.channel.queue_declare(queue='youtuber_requests')
        message = f"{self.youtuber_name} uploaded {self.video_name}"
        self.channel.basic_publish(exchange='', routing_key='youtuber_requests', body=message)
        print("SUCCESS: Video published")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python Youtuber.py <YoutuberName> <VideoName>")
    else:
        youtuber_name = sys.argv[1]
        video_name = sys.argv[2]
        youtuber = Youtuber(youtuber_name, video_name)
        youtuber.publishVideo()
