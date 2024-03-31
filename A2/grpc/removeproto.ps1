directory=$(pwd)

# Check if the directory exists
if [ ! -d "$directory" ]; then
    echo "Directory '$directory' does not exist."
    exit 1
fi

# Find and delete folders starting with "logs_node"
find "$directory" -type d -name "logs_node*" -exec rm -rf {} +