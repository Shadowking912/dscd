import zmq
import sys
import uuid
import datetime

class User:
    def __init__(self, user_id, server_address):
        self.user_id = user_id
        self.MessageServerAddress = server_address
        self.context = zmq.Context()
        self.MessageServerSocket = self.context.socket(zmq.REQ)
        self.MessageServerSocket.connect("tcp://"+(self.MessageServerAddress))
        self.GroupList = []
        self.JoinedGroups= []
        self.GroupSockets = []

    def get_group_list(self):
        self.MessageServerSocket.send_json({'action': 'get_group_list', 'user_uuid':self.user_id})
        group_list = self.MessageServerSocket.recv_json()
        self.GroupList = group_list
        print("List of available groups:")
        i=1
        for group_name, group_address in group_list.items():
            print(f"{i}. {group_name} - {group_address}")
            i+=1

    def join_group(self, group_id):
        group_address = self.GroupList[group_id]
        # print(f"DEB:{group_id},{group_address},{self.GroupList}")
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect("tcp://"+(group_address))
        # socket.connect("tcp://127.0.0.1:5556")
        # print("DEB: Sending JSON")
        socket.send_json({'action': 'add_user', 'user_uuid': self.user_id})
        # print("DEB: Sent Json")
        response = socket.recv_string()
        print(response)

    def leave_group(self, group_name):
        self.socket.send_json({'action': 'leave_group', 'group_name': group_name, 'user_id': self.user_id})
        response = self.socket.recv_string()
        print(response)

    def get_messages(self, group_name, timestamp=None):
        request = {'action': 'get_messages', 'group_name': group_name}
        if timestamp:
            request['timestamp'] = timestamp
        self.socket.send_json(request)
        messages = self.socket.recv_json()
        print(f"Messages from group {group_name}:")
        for message in messages:
            print(message)

    def send_message(self, group_name, message):
        timestamp = datetime.datetime.now().timestamp()
        self.socket.send_json({'action': 'send_message', 'group_name': group_name, 'user_id': self.user_id, 'message': message, 'timestamp':timestamp})
        # response = self.socket.recv_string()
        # print(response)
        try:
            response = self.socket.recv_string()
            print(response)
        except zmq.Again:
            print("FAIL")

    def display_help(self):
        print("Available commands:")
        print("1. list_groups - Get the list of available groups")
        print("2. join_group <group_name> - Join a group")
        print("3. leave_group <group_name> - Leave a group")
        print("4. get_messages <group_name> [<timestamp>] - Get messages from a group")
        print("5. send_message <group_name> <message> - Send a message to a group")
        print("6. help - Display available commands")
        print("7. exit - Exit the application")

def main():
    if len(sys.argv) != 2:
        print("Usage: python user.py <server_address>")
        sys.exit(1)

    user_id = str(uuid.uuid1())
    server_address = sys.argv[2]

    user = User(user_id, server_address)
    user.display_help()

    while True:
        command = input("Enter command: ").strip().split(' ')
        action = command[0]

        if action == '1' or action == 'list_groups':
            user.get_group_list()

        elif action == '2' or action == 'join_group':
            if len(command) != 2:
                print("Usage: join_group <group_name>")
            else:
                group_name = command[1]
                user.join_group(group_name)

        elif action == '3' or action == 'leave_group':
            if len(command) != 2:
                print("Usage: leave_group <group_name>")
            else:
                group_name = command[1]
                user.leave_group(group_name)

        elif action == '4' or action == 'get_messages':
            if len(command) < 2 or len(command) > 3:
                print("Usage: get_messages <group_name> [<timestamp>]")
            else:
                group_name = command[1]
                timestamp = command[2] if len(command) == 3 else None
                user.get_messages(group_name, timestamp)

        elif action == '5' or action == 'send_message':
            if len(command) < 3:
                print("Usage: send_message <group_name> <message>")
            else:
                group_name = command[1]
                message = ' '.join(command[2:])
                user.send_message(group_name, message)

        elif action == '6' or action == 'help':
            user.display_help()

        elif action == '7' or action == 'exit':
            print("Exiting...")
            break

        else:
            print("Invalid command. Type 'help' for available commands.")

if __name__ == "__main__":
    main()
