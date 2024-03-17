import zmq
import signal



signal.signal(signal.SIGINT, signal.SIG_DFL)
class RaftClient:
    def __init__(self, client_id, server_address):
        self.client_id = client_id
        self.server_address = server_address

        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.server_address)


    def change_socket_connection(self):
        self.close_connection() 
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(self.server_address)

    def send_request(self, request_type, key=None, value=None):
        request = {
            'client_id':self.client_id,
            'type': "client_request",
            'sub-type':request_type,
            'key': key,
            'value': value
        }

        try:
            self.socket.send_json(request)
            response = self.socket.recv_json()

            status = response['status']
            while status!='success':
                # Debugging statement
                print("In the while loop")
                leader = response['leaderId']
                if leader==-1:
                    print("No leader found")
                    break
                self.server_address = f"tcp://127.0.0.1:555{leader}"
                self.change_socket_connection()
                self.socket.send_json(request)
                print(f"Response from server : {response}")
                response  = self.socket.recv_json()
                status = response['status']
            
            print("Message added successfully")
        except zmq.ZMQError as e:
            print(f"Error occurred: {str(e)}")

    def close_connection(self):
        self.socket.close()
        self.context.term()

if __name__ == "__main__":
    client_id = int(input("Enter Client ID: "))
    # server_address = input("Enter Server Address (e.g., tcp://127.0.0.1:5555): ")
    server_address = "tcp://127.0.0.1:5552"
    client = RaftClient(client_id, server_address)

    # client.run()


    while True:
        print("\nOptions:")
        print("1. SET")
        print("2. GET")
        print("3. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            key = input("Enter key: ")
            value = input("Enter value: ")
            client.send_request('SET', key, value)
        elif choice == '2':
            key = input("Enter key: ")
            client.send_request('GET', key)
        elif choice == '3':
            client.close_connection()
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

        # message = self.socket.recv_json()