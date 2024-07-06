# Google RAFT Consensus Algorithm Implementation in Python

This repository contains a custom implementation of the RAFT consensus algorithm in Python using gRPC for communication between nodes. RAFT is a consensus algorithm designed to manage a replicated log in a distributed system, providing high availability and fault tolerance.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
- [Architecture](#architecture)
  - [Components](#components)
  - [Protocol Buffers](#protocol-buffers)
  - [State Machine](#state-machine)
- [Configuration](#configuration)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

## Introduction

This project is a Python-based implementation of the RAFT consensus algorithm using gRPC for communication between nodes. RAFT is designed to be a simple yet efficient consensus algorithm for managing replicated logs and ensuring consistency in distributed systems. The implementation covers key RAFT components such as leader election, log replication, and safety mechanisms.

## Features

- **Leader Election:** Automated leader election to maintain a single leader in the cluster.
- **Log Replication:** Ensures consistent log replication across multiple nodes.
- **Fault Tolerance:** Capable of handling node failures and network partitions.
- **gRPC Communication:** Efficient and scalable node-to-node communication using gRPC.
- **Extensible Configuration:** Customizable settings for various deployment environments.

## Getting Started

### Prerequisites

- Python 3.8 or later
- `pip` (Python package installer)
- `virtualenv` for creating isolated Python environments

### Installation

1. **Clone the repository:**

    ```bash
    git clone https://github.com/your-username/google-raft-python.git
    cd google-raft-python
    ```

    <button onclick="copyToClipboard('git clone https://github.com/your-username/google-raft-python.git\ncd google-raft-python')">Copy</button>

2. **Create and activate a virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```

    <button onclick="copyToClipboard('python -m venv venv\nsource venv/bin/activate')">Copy</button>

3. **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

    <button onclick="copyToClipboard('pip install -r requirements.txt')">Copy</button>

4. **Generate gRPC code from proto files:**

    ```bash
    ./generate_grpc_code.sh
    ```

    > Ensure you have `protoc` installed and available in your `PATH`.

    <button onclick="copyToClipboard('./generate_grpc_code.sh')">Copy</button>

### Usage

1. **Start the RAFT node:**

    ```bash
    python raft_node.py --config config.yaml
    ```

    <button onclick="copyToClipboard('python raft_node.py --config config.yaml')">Copy</button>

2. **Run multiple nodes for a full cluster:**

    ```bash
    python raft_node.py --config config1.yaml
    python raft_node.py --config config2.yaml
    python raft_node.py --config config3.yaml
    ```

    > Ensure each configuration file specifies unique node IDs and port numbers.

    <button onclick="copyToClipboard('python raft_node.py --config config1.yaml\npython raft_node.py --config config2.yaml\npython raft_node.py --config config3.yaml')">Copy</button>

3. **Interact with the cluster using the provided client tool:**

    ```bash
    python raft_client.py --action add --key key1 --value value1
    python raft_client.py --action get --key key1
    ```

    <button onclick="copyToClipboard('python raft_client.py --action add --key key1 --value value1\npython raft_client.py --action get --key key1')">Copy</button>

## Architecture

### Components

- **RaftNode:** Core class representing a RAFT node, handling state transitions and message processing.
- **StateMachine:** Abstract class for state machine implementations, ensuring correct log application.
- **gRPC Service:** Defines RAFT RPCs for inter-node communication.

### Protocol Buffers

Protocol buffers are used to define the structure of the messages exchanged between nodes. The `.proto` files in the `protos/` directory define the RPC services and message types.

### State Machine

The state machine interface should be implemented by any application requiring RAFT-based consensus. This ensures that logs are applied consistently across nodes.

```python
class StateMachine:
    def apply_log(self, log_entry):
        pass
