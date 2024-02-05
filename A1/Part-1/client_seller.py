from concurrent import futures

import grpc
import market_seller_pb2_grpc
import market_seller_pb2
import uuid
import sys

class SellerNotificationServer(market_seller_pb2_grpc.SellerNotificationServerServicer):
    def ReceiveNotification(self,request,context):
        product=request.notification
        print(f"Item ID:{product.id},Price:{product.price},Name:{product.name},Category:{product.productCategory}")
        print(f"Description:{product.description}")
        print(f"Quantity Reamining:{product.quantityRemaining}")
        print(f"Rating : {product.rating}/5 | Seller:{product.Address}")
        print()
        status_response = market_seller_pb2.StatusResponse()
        status_response.status = "SUCCESS"
        return status_response

def register(stub,unique_id,addr):
    # address = "0.0.0.0:" + str(4000)
    register_request = market_seller_pb2.RegisterSellerRequest(address=addr,uuid=unique_id)
    registration_response = stub.RegisterSeller(register_request)
    print(registration_response.status)

def sell(stub,unique_id):
    print("Enter the product name : ")
    product_name = input()
    category = -1
    while(category<0 or category>2):
        print("\nEnter the Category")
        print("Possible Choices for the Product Category : ")
        print("0) ELECTRONICS")
        print("1) FASHION")
        print("2) OTHERS")
        print("\nPlease enter your category choice : ")
        category = int(input())
    print("\nEnter the Quantity of the product : ")
    quantity = int(input())
    print("\nPlease add the description of the product : ")
    description = input()
    print("\nPlease enter the price per unit :")
    price_per_unit = float(input())

    sell_request = market_seller_pb2.SellItemRequest(productName=product_name,productCategory=category,quantity=quantity,pricePerUnit=price_per_unit,description=description,uuid=unique_id)
    sell_response = stub.SellItem(sell_request)
    print(sell_response.status)

def displayItems(stub,unique_id):
    displayrequest = market_seller_pb2.ProductDisplayRequest(uuid=unique_id)
    displayresponses = stub.DisplaySellerItems(displayrequest)
    for displayresponse in displayresponses:
        print(f"Item ID: {displayresponse.id},Price: {displayresponse.price},Name: {displayresponse.name},Category: {displayresponse.productCategory}")
        print(f"Description: {displayresponse.description}")
        print(f"Quantity Reamining: {displayresponse.quantityRemaining}")
        print(f"Rating : {displayresponse.rating}/5 | Seller: {displayresponse.Address}")
        print()

def update(stub,unique_id):
    print("Here is a list of all the available items that you registered :-")
    displayItems(stub,unique_id)
    print("Please enter the id of the product you wish to update : ")
    id = int(input())
    print("Please enter the updated quantity")
    quantity = int(input())
    print("Please enter the updated price")
    price = float(input())
    updateitemrequest = market_seller_pb2.UpdateItemRequest(uuid=unique_id,id=id,newPrice=price,newQuantity=quantity)
    updateitemresponse = stub.UpdateItem(updateitemrequest)
    print(updateitemresponse)

def delete(stub,unique_id):
    print("Here is a list of all the available items that you registered :-")
    displayItems(stub,unique_id)
    print("Please enter the id of the product you wish to delete : ")
    id = int(input())
    deleteitemrequest = market_seller_pb2.DeleteItemRequest(uuid=unique_id,id=id)
    deleteitemresponse = stub.DeleteItem(deleteitemrequest)
    print(deleteitemresponse)



def run(unique_id,addr="localhost:50053"):

    # Notification Server
    seller_notification_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notification_server = SellerNotificationServer()
    notification_server_addr=addr
    market_seller_pb2_grpc.add_SellerNotificationServerServicer_to_server(notification_server,seller_notification_server)
    seller_notification_server.add_insecure_port(notification_server_addr)
    seller_notification_server.start()


    # Client
    channel= grpc.insecure_channel('localhost:50050')
    stub = market_seller_pb2_grpc.MarketPlaceStub(channel)
    while(1):
        print("Welcome to the Shop Seller :-")
        print("Here are your possible options : -")
        print("1) Register yourself as a seller")
        print("2) Sell item")
        print("3) Update item record")
        print("4) Delete item")
        print("5) Display your listed items")
        print("6) Exit")
        print("Please select which service you would like to avail ?")
        choice = int(input())

        if choice==1:
            register(stub,unique_id,notification_server_addr)
        elif choice==2:
            sell(stub,unique_id)
        elif choice==3:
            update(stub,unique_id)
        elif choice==4:
            delete(stub,unique_id)
        elif choice==5:
            displayItems(stub,unique_id)
        elif choice==6:
            break
            
if __name__=="__main__":
    unique_id=str(uuid.uuid1())
    run(unique_id,sys.argv[1][0])