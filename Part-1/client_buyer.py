import grpc
import market_buyer_pb2_grpc
import market_buyer_pb2
import uuid

def SearchItem(stub,unique_id):
    print("Available categories:\n ELECTRONICS-0\n FASHION-1\n OTHERS-2\n ANY-3")
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
        print(f"Item ID:{searchresponse.id},Price:${searchresponse.price},Name:${searchresponse.name},Category:${searchresponse.productCategory}")
        print(f"Description:{searchresponse.description}")
        print(f"Quantity Reamining:{searchresponse.quantityRemaining}")
        print(f"Rating : {searchresponse.rating}/5 | Seller:${searchresponse.sellerAddress}")
        print()

def BuyItem(stub,unique_id):
    itemid=int(input("Item id: "))
    qty=int(input("Quantity: "))
    usrname=input("Username: ")
    buyrequest=market_buyer_pb2.BuyRequest(username=usrname,id=itemid,quantity=qty)
    buyresponse=stub.BuyItem(buyrequest)
    print(buyresponse)

def AddTOWishList(stub):
    itemid=int(input("Item id: "))
    usrname=input("Username: ")
    wishreq=market_buyer_pb2.WishRequest(username=usrname,id=itemid)
    wishresponse=stub.AddWish(wishreq)
    print(wishresponse)

def RateItem(stub):
    print("Available Ratings : 1-5(integers)")
    itemid=int(input("Item id: "))
    usrname=input("Username: ")
    ratings=int(input("Rating: "))
    raterequest=market_buyer_pb2.RateRequest(id=itemid,username=usrname,rating=ratings)
    rateresponse=stub.RateItem(raterequest)

def run(unique_id):
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = market_buyer_pb2_grpc.MarketPlaceStub(channel)
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
                BuyItem(stub,unique_id)
        # elif choice==3:
        #     u
            
        # elif choice==4:
            
        # elif choice==5:
            

if __name__=="__main__":
    unique_id=str(uuid.uuid1())
    run(unique_id)