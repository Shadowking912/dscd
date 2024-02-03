import grpc
import market_seller_pb2
import market_seller_pb2_grpc

class MarketPlaceService(market_seller_pb2_grpc.MarketPlaceServicer):
    def RegisterSeller(self, request, context):
        return super().RegisterSeller(request, context)

    def SellItem(self, request, context):
        return super().SellItem(request, context)
    
    def UpdateItem(self, request, context):
        return super().UpdateItem(request, context)
    
    def DeleteItem(self, request, context):
        return super().DeleteItem(request, context)
    
    def DisplaySellerItems(self, request, context):
        return super().DisplaySellerItems(request, context)