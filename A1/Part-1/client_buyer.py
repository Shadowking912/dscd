from concurrent import futures

import grpc
import market_buyer_pb2_grpc
import market_buyer_pb2
import uuid
import sys

class BuyerNotificationServer(market_buyer_pb2_grpc.BuyerNotificationServerServicer):
    def ReceiveNotification(self,request,context):
        print("yes")
        product=request.notification
        print(f"Item ID:{product.id},Price:{product.price},Name:{product.name},Category:{product.productCategory}")
        print(f"Description:{product.description}")
        print(f"Quantity Reamining:{product.quantityRemaining}")
        print(f"Rating : {product.rating}/5 | Seller:{product.Address}")
        print()
        status_response = market_buyer_pb2.StatusResponse()
        status_response.status = "SUCCESS"
        return status_response


# class BuyerNotif(market_buyer_pb2_grpc.BuyerNotificationServer)
def SearchItem(stub,unique_id):
    category = -1
    while(category<0 or category>3):
        print("\nEnter the Category")
        print("Possible Choices for the Product Category : ")
        print("0) ELECTRONICS")
        print("1) FASHION")
        print("2) OTHERS")
        print("3) ANY")
        print("\nPlease enter your category choice : ")
        category = int(input())
    item=input("Item Name")
    searchrequest=market_buyer_pb2.SearchRequest(item_name=item,category=category)
    searchresponses=stub.SearchItem(searchrequest)
    # print(searchresponses)
    for searchresponse in searchresponses:
        # print(searchresponse)
        print("The following item has been updated : ")
        print(f"Item ID:{searchresponse.id},Price:{searchresponse.price},Name:{searchresponse.name},Category:{searchresponse.productCategory}")
        print(f"Description:{searchresponse.description}")
        print(f"Quantity Reamining:{searchresponse.quantityRemaining}")
        print(f"Rating : {searchresponse.rating}/5 | Seller:{searchresponse.Address}")
        print()

def BuyItem(stub,notification_server_addr):
    itemid=int(input("Item id: "))
    qty=int(input("Quantity: "))
    buyrequest=market_buyer_pb2.BuyRequest(id=itemid,quantity=qty,Address=notification_server_addr)
    buyresponse=stub.BuyItem(buyrequest)
    print(buyresponse)

def AddTOWishList(stub,unique_id,addr):
    itemid=int(input("Item id: "))
    wishreq=market_buyer_pb2.WishRequest(uuid=unique_id,id=itemid,Address=addr)
    wishresponse=stub.AddWish(wishreq)
    print(wishresponse)

def RateItem(stub):
    print("Available Ratings : 1-5(integers)")
    itemid=int(input("Item id: "))
    ratings=int(input("Rating: "))
    raterequest=market_buyer_pb2.RateRequest(id=itemid,rating=ratings)
    rateresponse=stub.RateItem(raterequest)
    print(rateresponse)

def run(unique_id,addr="localhost:50052"):
    # Notification Server

    buyer_notification_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    notification_server = BuyerNotificationServer()
    notification_server_addr=addr
    market_buyer_pb2_grpc.add_BuyerNotificationServerServicer_to_server(notification_server,buyer_notification_server)
    buyer_notification_server.add_insecure_port(notification_server_addr)
    buyer_notification_server.start()

    # Client
    channel= grpc.insecure_channel('localhost:50050')
    stub= market_buyer_pb2_grpc.MarketPlaceStub(channel)
    while(1):
        print("Welcome to the Shop Buyer :-")
        print("Here are your possible options : -")
        print("1) Search Item")
        print("2) Buy Item")
        print("3) Add to Wishlist")
        print("4) Rate Item")
        print("5) Logout")

        print("Please select which service you would like to avail ?")
        choice = int(input())

        if choice==1:
            SearchItem(stub,unique_id)
        elif choice==2:
            BuyItem(stub,notification_server_addr)
        elif choice==3:
            AddTOWishList(stub,unique_id,notification_server_addr)
        elif choice==4:
            RateItem(stub)
        elif choice==5:
            break
            

if __name__=="__main__":
    unique_id=str(uuid.uuid1())
    run(unique_id,sys.argv[1][0])