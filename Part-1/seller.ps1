$inputs= "1`n2`nA`n0`n10`nThis is productA`n20.0`n2`nB`n1`n20`nThis is productB`n30.0`n2`nC`n2`n10`nThis is productC`n30.0`n6"
# Execute the Python script with input redirection
Write-Output $inputs | python client_seller.py