
---

# Group Messaging Application using ZeroMQ

This project implements a low-level group messaging application using ZeroMQ. The application comprises a central messaging app server, multiple groups, and users interacting with each other in real-time.

## Components

### 1. Message Server (`message_server.py`)

#### MessageServer Class

- **`register_group(group_id, ip_address, port)`**: Registers a group with the message server.
  
- **`get_group_list(user_uuid)`**: Retrieves the list of available groups for a user.

#### Main Functionality

- Binds to a ZeroMQ socket and listens for incoming messages.
- Handles registration and group list requests from groups and users respectively.
- Manages the list of groups and their IP addresses.
- Handles user requests to join or leave groups.
- Facilitates group communication.


**Usage:**
```bash
python message_server.py
```

### 2. Group Server (`group.py`)

#### Group Class

- **`__init__(id, name)`**: Initializes a group with its ID and name.
  
- **`print_group_details()`**: Prints details of the group, including ID, name, user list, registration status, and group server address.
  
- **`add_user(user_uuid)`**: Adds a user to the group.
  
- **`remove_user(user_uuid)`**: Removes a user from the group.
  
- **`register_group(server_address)`**: Registers the group with the message server.
  
- **`listen_for_user()`**: Listens for user interactions such as joining, leaving, sending, and receiving messages.

- **`get_messages(timestamp=None)`**: Retrieves messages from the group based on the provided timestamp. If no timestamp is provided, all messages are returned.

- **`add_message(message, user_uuid, timestamp)`**: Adds a message to the group with the user ID and timestamp.

#### Main Functionality

- Allows the group to register with the message server, listen for user interactions, and handle various commands.
- Registers with the message server.
- Manages group-level operations such as user management and message handling.
- Stores messages sent by users within the group.


**Usage:**
```bash
python group.py <group_address>
```

### 3. Users (`user.py`)


#### User Class

- **`__init__(user_id, server_address)`**: Initializes a user with a unique ID and the address of the message server.

- **`show_groups()`**: Displays the list of groups that the user has joined.

- **`get_group_list()`**: Retrieves the list of available groups from the message server.

- **`join_group(group_id)`**: Allows the user to join a specific group.

- **`leave_group(group_name)`**: Enables the user to leave a group.

- **`get_messages(group_name, timestamp=None)`**: Retrieves messages from a group based on the provided timestamp.

- **`send_message(group_name, message)`**: Sends a message to a group.

- **`display_help()`**: Displays available commands to the user.

#### Main Functionality

- Provides a command-line interface for users to interact with groups and the message server.
- Interact with the message server and groups.
- Join or leave groups, send messages, and fetch messages from groups.


**Usage:**
```bash
python user.py <server_address>
```

## Running the Application

1. Start the Message Server:
```bash
python message_server.py
```

2. Start the Group Server(s):
```bash
python group.py <group_address>
```

3. Start Users:
```bash
python user.py <server_address>
```

## Commands for Users

- `list_groups`: Get the list of available groups.
- `join_group <group_name>`: Join a group.
- `show_groups `: Show Joined Groups.	
- `leave_group <group_name>`: Leave a group.
- `get_messages <group_name> [<timestamp>]`: Get messages from a group.
- `send_message <group_name> <message>`: Send a message to a group.
- `help`: Display available commands.
- `exit`: Exit the application.

