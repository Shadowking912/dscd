# YouTube Server with RabbitMQ

This Python script implements a simplified version of a YouTube server using RabbitMQ for message passing. It allows users to subscribe to YouTubers and receive notifications when new videos are uploaded.

## Dependencies

- Python 3.x
- RabbitMQ

## Installation

1. Install Python 3.x from [python.org](https://www.python.org/downloads/)

2. RabbitMq:

- Windows: Install RabbitMQ from [rabbitmq.com](https://www.rabbitmq.com/download.html)

- Linux: run "sh `rabbitsetup.sh`"
## Usage

1. Clone this repository:

    ```bash
    git clone https://github.com/your_username/YouTubeServer.git
    ```

2. Navigate to the project directory:

    ```bash
    cd YouTubeServer
    ```

3. Install the required Python packages:

    ```bash
    pip install pika
    ```

## Components

### Structure

- `youtube_server.py`: Main Python script containing the `YoutubeServer` class and script execution logic.
- `user.py`: Defines the `User` class representing a user.
- `youtuber.py`: Defines the `Youtuber` class representing a YouTuber.

### 1. Youtube Server (`youtube_server.py`)

**Usage:**
```bash
python youtube_server.py <internal_ip>
```

### Functionality

- **Initialization**: The `YoutubeServer` class initializes with two RabbitMQ channels. It declares queues for handling user and YouTuber requests, and an exchange for sending notifications to users.

- **Consuming Requests**:
  - `consume_user_requests()`: Listens for user subscription requests. Processes subscriptions and unsubscriptions.
  - `consume_youtuber_requests()`: Listens for new video upload requests from YouTubers. Processes video uploads and notifies subscribers.

- **Notification**: The `notify_users()` method sends notifications to subscribers when a new video is uploaded by a YouTuber.

- **Main Script**: 
  - Reads the RabbitMQ host from the command line.
  - Establishes connections and channels to RabbitMQ.
  - Creates an instance of the `YoutubeServer` class and starts consuming user and YouTuber requests.

### 2. User (`youtuber.py`)

**Usage:**
```bash
python youtuber.py <server_external_ip> <youtuber_name> <video_name>
```

### Functionality


- **Initialization**: Creates a Youtuber object with a name.
- **Subscriber Management**:
    - `add_subscriber(subscriber_name)`: Adds a subscriber to the Youtuber's list.
    - `remove_subscriber(subscriber_name)`: Removes a subscriber from the Youtuber's list.
    - `get_subscribers()`: Retrieves the list of subscribers.
- **Video Management**:
    - `add_video(video_name)`: Adds a video to the Youtuber's list of videos.
- **Utility Methods**:
    - `get_name()`: Retrieves the name of the Youtuber.


### 3. User (`user.py`)

**Usage:**
```bash
python user.py <server_external_ip> <user_name> <s/u> <youtuber_name>
python user.py <server_external_ip> <user_name>
```

### Functionality

- **Subscribe/Unsubscribe Youtuber**:
  - use the first usage

- **Recieve Notifications**:
  - use the second usage

- **User Class**: 
  - Represents a user of the subscription system.
  - Provides methods to add, delete, and retrieve subscriptions.

- **Updating Subscriptions** (`updateSubscription()`):
  - Updates user subscriptions by sending a message to the RabbitMQ server.
  - It either subscribes or unsubscribes to a youtuber.

- **Receiving Notifications** (`receiveNotifications()`):
  - Listens for notifications about new video uploads.
  - Prints received notifications to the console.
