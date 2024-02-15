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
        self.GroupSockets = {}

    def show_groups(self):
        i=1
        for j in self.JoinedGroups:
            print(f"{i}. {j}")
            i+=1

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
        self.GroupSockets[group_id] = socket
        self.JoinedGroups.append(group_id)
        response = socket.recv_string()
        print(response)

    def leave_group(self, group_name):
        try:
            group_socket = self.GroupSockets[group_name]
        except KeyError:
            print(f"FAIL: Group '{group_name}' not found.")
            return 
        group_socket.send_json({'action': 'leave_group', 'user_id': self.user_id})
        response = group_socket.recv_string()
        self.JoinedGroups.remove(group_name)
        self.GroupSockets.pop(group_name)
        print(response)

    def get_messages(self, group_name, timestamp=None):
        try:
            group_socket = self.GroupSockets[group_name]
        except KeyError:
            print(f"FAIL: Group '{group_name}' not found.")
            return 
        request = {'action': 'get_messages', 'group_name': group_name, 'user_uuid':self.user_id}
        if timestamp:
            request['timestamp'] = timestamp
        group_socket.send_json(request)
        messages = group_socket.recv_json()
        print(f"Messages from group {group_name}:")
        for message in messages:
            print(message)
        print("----------------------------------")
    

    def send_message(self, group_name, message):
        try:
            group_socket = self.GroupSockets[group_name]
        except KeyError:
            print(f"FAIL: Group '{group_name}' not found.")
            return 
        timestamp = datetime.datetime.now().strftime('%H:%M:%S')
        group_socket.send_json({'action': 'send_message', 'group_name': group_name, 'user_id': self.user_id, 'message': message, 'timestamp':timestamp})
        # response = group_socket.recv_string()
        # print(response)
        try:
            response = group_socket.recv_string()
            print(response)
        except zmq.Again:
            print("FAIL")

    def display_help(self):
        print("Available commands:")
        print("1. list_groups - Get the list of available groups")
        print("2. join_group <group_name> - Join a group")
        print("3. show_groups - Show Joined Groups")
        print("4. leave_group <group_name> - Leave a group")
        print("5. get_messages <group_name> [<timestamp>] - Get messages from a group")
        print("6. send_message <group_name> <message> - Send a message to a group")
        print("7. help - Display available commands")
        print("8. exit - Exit the application")

def main():
    if len(sys.argv) != 2:
        print("Usage: python user.py <server_address>")
        sys.exit(1)

    user_id = str(uuid.uuid1())
    server_address = sys.argv[1]

    user = User(user_id, server_address)
   

    while True:
        user.display_help()
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

        elif action == '3' or action == 'show_groups':
            if len(command) != 1:
                print("Usage: show_groups")
            else:
                user.show_groups()
        elif action == '4' or action == 'leave_group':
            if len(command) != 2:
                print("Usage: leave_group <group_name>")
            else:
                group_name = command[1]
                user.leave_group(group_name)

        elif action == '5' or action == 'get_messages':
            if len(command) < 2 or len(command) > 3:
                print("Usage: get_messages <group_name> [<timestamp>]")
            else:
                group_name = command[1]
                timestamp = command[2] if len(command) == 3 else None
                user.get_messages(group_name, timestamp)

        elif action == '6' or action == 'send_message':
            if len(command) < 3:
                print("Usage: send_message <group_name> <message>")
            else:
                group_name = command[1]
                message = ' '.join(command[2:])
                user.send_message(group_name, message)

        elif action == '7' or action == 'help':
            user.display_help()

        elif action == '8' or action == 'exit':
            print("Exiting...")
            break

        else:
            print("Invalid command. Type 'help' for available commands.")

if __name__ == "__main__":
    main()
