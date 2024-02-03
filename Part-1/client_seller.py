import grpc
import market_seller_pb2_grpc
import market_seller_pb2

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = market_seller_pb2_grpc.MarketPlaceStub(channel)
        print("Welcome to the Shop Seller :-")
        print("Here are your possible options : -")
        print("1) Register yourself as a seller")
        print("2) Sell item")
        print("3) Update item record")
        print("4) Delete item")
        print("5) Display your listed items")


