
# Get the current working directory
$directory = Get-Location

# Check if the directory exists
if (-not (Test-Path -Path $directory -PathType Container)) {
    Write-Host "Directory '$directory' does not exist."
    exit 1
}

# Find and delete folders starting with "logs_node"
Get-ChildItem -Path $directory -Directory -Filter "logs_node*" | Remove-Item -Recurse -Force
