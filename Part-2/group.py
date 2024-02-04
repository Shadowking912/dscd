import zmq
import uuid
import sys

class Group:
    def __init__(self, id, name):
        self.id = id
        self.name = name
        self.users = []
        self.messages = []
        self.registration_status = False
        self.MessageServerSocket = ""
        self.GroupAddress = "tcp://"

    def print_group_details(self):
        print(f"Group Details are:\n ID: {self.id} \n Name: {self.name}\n UserList:{self.users}\n Registration Status: {self.registration_status}\n Group Server Address: {self.GroupAddress}\n") 

    def add_user(self, user_uuid):
        self.users.append(user_uuid)
        print(f" New User ID: {user_uuid} \n Added to group ID: {self.id}")

    def remove_user(self, user_uuid):
        if user_uuid in self.users:
            self.users.remove(user_uuid)
            print(f"User ID: {user_uuid} removed from group ID: {self.id}")
        else:
            print(f"User ID: {user_uuid} not found in group ID: {self.id}")

    def register_group(self, server_address):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://"+server_address)
        socket.send_json({'action': 'register_group', 'group_id': self.id, 'group_name': self.name,
                           'ip_address': '127.0.0.1', 'port': 5556})  # group Server Address
        self.registration_status = True
        self.MessageServerSocket = socket
        self.GroupAddress=self.GroupAddress+"127.0.0.1:5556"
        response = self.MessageServerSocket.recv_string()
        print(response)
        self.print_group_details()

    def listen_for_user(self):
        print("Listening for User Commands\n")
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://127.0.0.1:5556")

        while True:
            message = socket.recv_json()
            # print("Got Request")

            if message['action'] == 'add_user':
                user_uuid = message['user_uuid']
                self.add_user(user_uuid)
                socket.send_string("SUCCESS")

            elif message['action'] == 'remove_user':
                user_uuid = message['user_uuid']
                self.remove_user(user_uuid)
                socket.send_string("SUCCESS")

            elif message['action'] == 'get_messages':
                timestamp = message.get('timestamp', None)
                messages = self.get_messages(timestamp)
                socket.send_json(messages)

            elif message['action'] == 'add_message':
                new_message = message['message']
                self.add_message(new_message)
                print(f"MESSAGE SEND FROM {message['user_id']}")
                socket.send_string("SUCCESS")

            elif message['action'] == 'print_group_details':
                socket.send_string(self.print_group_details())
            else:
                print("Invalid Request")

    def get_messages(self, timestamp=None):
        if timestamp:
            return [msg for msg in self.messages if msg['timestamp'] >= timestamp]
        else:
            return self.messages

    def add_message(self, message):
        self.messages.append(message)
        

# Main function to run the group interface
def main():
    if len(sys.argv) != 2:
        print("Usage: python group.py <server_address>")
        sys.exit(1)

    server_address = sys.argv[1]

    unique_id = str(uuid.uuid1())
    group_name = input("Enter group name: ")
    group = Group(unique_id, group_name)

    while True:
        print("\nCommands:")
        print("1. Register group")
        print("2. Listen for User Interactions")
        print("3. Print group details")
        print("4. Exit")

        choice = input("Enter choice: ")

        if choice == '1':
            group.register_group(server_address)
        elif choice == '2':
            group.listen_for_user()
        elif choice == '3':
            group.print_group_details()
        elif choice == '4':
            print("Exiting...")
            break
        else:
            print("Invalid choice")

if __name__ == "__main__":
    main()
