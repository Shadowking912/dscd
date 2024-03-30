sudo apt update -y
sudo apt install python3-pip -y
sudo apt install python3-venv -y
python3 -m venv venv
source venv/bin/activate&&pip install grpcio grpcio-tools