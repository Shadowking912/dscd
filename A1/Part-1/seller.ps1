$inputs= "1`n2`nA`n0`n10`nThis is productA`n20.0`n2`nB`n1`n20`nThis is productB`n30.0`n2`nC`n2`n10`nThis is productC`n30.0`n5`n3`n1`n40`n50`n5`n6"
# Execute the Python script with input redirection
Write-Output $inputs | python client_seller.py