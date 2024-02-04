import zmq
server_address="tcp://127.0.0.1:5555"

class MessageServer:
    def __init__(self):
        self.groups = {}

    def register_group(self, group_id, ip_address, port):
        self.groups[group_id] = str(ip_address)+":"+str(port)
        print(f"JOIN REQUEST FROM {ip_address}:{port}")

    def get_group_list(self,user_uuid):
        print(f"GROUP LIST REQUEST FROM {user_uuid}")
        return self.groups


def main():
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(server_address) 

    message_server = MessageServer()
    print(f"Message Server Started at {server_address}")

    while True:
        message = socket.recv_json()

        if message['action'] == 'register_group':
            group_id = message['group_id']
            ip_address = message['ip_address']
            port = message['port']
            message_server.register_group(group_id, ip_address, port)
            socket.send_string("SUCCESS")

        elif message['action'] == 'get_group_list':
            user_uuid = message['user_uuid']
            group_list = message_server.get_group_list(user_uuid)
            socket.send_json(group_list)

if __name__ == "__main__":
    main()
    
