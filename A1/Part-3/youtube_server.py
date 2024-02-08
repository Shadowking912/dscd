import pika

class YoutubeServer:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='user_requests')
        self.channel.queue_declare(queue='youtuber_requests')
        self.channel.queue_declare(queue='notifications')

    def consume_user_requests(self, ch, method, properties, body):
        print(body.decode('utf-8'))

    def consume_youtuber_requests(self, ch, method, properties, body):
        print(body.decode('utf-8'))

    def notify_users(self):
        def callback(ch, method, properties, body):
            print("New Notification:", body.decode('utf-8'))

        self.channel.basic_consume(queue='notifications', on_message_callback=callback, auto_ack=True)
        print("Waiting for notifications...")
        self.channel.start_consuming()

if __name__ == "__main__":
    server = YoutubeServer()
    server.consume_user_requests()
    server.consume_youtuber_requests()
    server.notify_users()
