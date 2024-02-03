from concurrent import futures

import grpc
import market_seller_pb2
import market_seller_pb2_grpc
import Helper_Classes.Product


class MarketPlaceService(market_seller_pb2_grpc.MarketPlaceServicer):
    def __init__(self):
        self.sellers={}
        self.products=[]
    
    def RegisterSeller(self, request, context):
        seller_uuid = request.uuid
        print(f"Seller join request from  {context.peer()}[ip:port], uuid={seller_uuid}")
        status_response = market_seller_pb2.StatusResponse()
        if seller_uuid not in self.sellers:
            status_response.status = "SUCCESS"
            self.sellers[seller_uuid] = (context.peer(),[])
        else:
            status_response.status="FAILURE"
        return status_response
    
    def SellItem(self, request, context):
        return super().SellItem(request, context)
    
    def UpdateItem(self, request, context):
        return super().UpdateItem(request, context)
    
    def DeleteItem(self, request, context):
        return super().DeleteItem(request, context)
    
    def DisplaySellerItems(self, request, context):
        return super().DisplaySellerItems(request, context)
    



def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    market_seller_pb2_grpc.add_MarketPlaceServicer_to_server(MarketPlaceService(),server)
    server.add_insecure_port("localhost:50051")
    server.start()
    server.wait_for_termination()

if __name__=="__main__":
    serve()