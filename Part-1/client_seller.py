import grpc
import market_seller_pb2_grpc
import market_seller_pb2
import uuid
def register(stub,unique_id,ip_address=None,port_number=None):
    register_request = market_seller_pb2.RegisterSellerRequest(uuid=unique_id)
    registration_response = stub.RegisterSeller(register_request)
    print(registration_response.status)

def sell(stub,unique_id,ip_address=None,port_number=None):
    print("Enter the product name : ")
    product_name = input()
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

    sell_request = market_seller_pb2.SellItemRequest(productName=product_name,productCategory=category,quantity=quantity,description=description,pricePerUnit=price_per_unit,uuid=unique_id)
    sell_response = stub.RegisterSeller(sell_request)
    print(sell_response.status)

def run(unique_id):
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = market_seller_pb2_grpc.MarketPlaceStub(channel)
        print("Welcome to the Shop Seller :-")
        print("Here are your possible options : -")
        print("1) Register yourself as a seller")
        print("2) Sell item")
        print("3) Update item record")
        print("4) Delete item")
        print("5) Display your listed items")

        print("Please select which service you would like to avail ?")
        choice = int(input())

        if choice==1:
            register(stub,unique_id)
        elif choice==2:
            sell(stub,unique_id)
        # elif choice==3:
        #     u
            
        # elif choice==4:
            
        # elif choice==5:
            

if __name__=="__main__":
    unique_id=str(uuid.uuid1())
    while(1):
        run(unique_id)