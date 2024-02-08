import pika
import json

class User:
    def __init__(self, user_name):
        self.user_name = user_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

    def updateSubscription(self, youtuber_name, subscribe):
        self.channel.queue_declare(queue='user_requests')
        message = {
            "user": self.user_name,
            "youtuber": youtuber_name,
            "subscribe": subscribe
        }
        self.channel.basic_publish(exchange='', routing_key='user_requests', body=json.dumps(message))
        print("SUCCESS: Subscription updated")

    def receiveNotifications(self):
        def callback(ch, method, properties, body):
            print("New Notification:", body.decode('utf-8'))

        self.channel.basic_consume(queue=self.user_name, on_message_callback=callback, auto_ack=True)
        print("Waiting for notifications...")
        self.channel.start_consuming()

if __name__ == "__main__":
    user_name = input("Enter your username: ")
    user = User(user_name)

    while True:
        print("\nChoose an action:")
        print("1. Subscribe to a YouTuber")
        print("2. Unsubscribe from a YouTuber")
        print("3. Receive notifications")
        print("4. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            youtuber_name = input("Enter the name of the YouTuber to subscribe to: ")
            user.updateSubscription(youtuber_name, True)
        elif choice == '2':
            youtuber_name = input("Enter the name of the YouTuber to unsubscribe from: ")
            user.updateSubscription(youtuber_name, False)
        elif choice == '3':
            user.receiveNotifications()
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")
