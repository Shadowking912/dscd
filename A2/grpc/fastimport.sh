#!/usr/bin/bash
sudo apt update -y
sudo apt install python3-pip -y
sudo apt install python3-venv -y
sudo apt install git -y
python3 -m venv venv
source venv/bin/activate&&pip install grpcio grpcio-tools
pat="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"
git clone https://username:$pat@github.com/Shadowking912/dscd.git
source venv/bin/activate&&cd dscd/A2/&&chmod -R u+w+x grpc&&cd grpc&&./removeproto.sh 