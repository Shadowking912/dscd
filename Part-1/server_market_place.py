from concurrent import futures

import grpc
import market_seller_pb2
import market_seller_pb2_grpc
import market_buyer_pb2
import market_buyer_pb2_grpc

from Helper_Classes.Product import Product
from Helper_Classes.Seller  import Seller

class MarketPlaceService(market_seller_pb2_grpc.MarketPlaceServicer,market_buyer_pb2_grpc.MarketPlaceServicer):
    
    def __init__(self):
        self.sellers={}
        self.currentProducts=0
        self.buyers={}
        self.products = []

    #seller    
    def RegisterSeller(self, request, context):
        seller_notification_server_address = request.address
        seller_uuid = request.uuid
        new_seller = Seller(seller_uuid,seller_notification_server_address)
        print(f"Seller join request from  {context.peer()}[ip:port], uuid={seller_uuid}")
        status_response = market_seller_pb2.StatusResponse()
        if seller_uuid not in self.sellers:
            status_response.status = "SUCCESS"
            self.sellers[seller_uuid] = new_seller
            # print("Sellers = ",self.sellers)
            # print(self.sellers[seller_uuid].uuid)
        else:
            status_response.status="FAILURE"
        return status_response
    
    def SellItem(self, request, context):
        seller_uuid = request.uuid
        status_response = market_seller_pb2.StatusResponse()
        if seller_uuid not in self.sellers:
            status_response.status = "FAILURE"
        else:
            product_id = self.currentProducts+1
            self.currentProducts+=1
            product_name  = request.productName
            product_category = request.productCategory
            product_quantity = request.quantity
            product_description = request.description
            producrprice_per_unit = request.pricePerUnit
            new_product = Product(product_id,product_name,product_category,product_quantity,product_description,producrprice_per_unit,0,context.peer())
            # print(self.sellers[seller_uuid].uuid)
            self.sellers[seller_uuid].add_product(new_product)
            self.products.append((new_product,seller_uuid))
            status_response.status="SUCCESS"
       
        return status_response

    def UpdateItem(self, request, context):
        
        return super().UpdateItem(request, context)
    
    def DeleteItem(self, request, context):
        return super().DeleteItem(request, context)
    
    def DisplaySellerItems(self, request, context):
        return super().DisplaySellerItems(request, context)
    
    #buyers
    def SearchItem(self,request,context):
        print("yes")
        item_name=request.item_name
        category=request.category
        info=[]
        if(item_name==""):
            if(category!="ANY"):
                for i in self.products:
                    i=i[0]
                    if(i.category==category):
                        info.append(i)
            else:
                for i in self.products:
                    i=i[0]
                    info.append(i)
        else:
            if(category!="ANY"):
                for i in self.products:
                    i=i[0]
                    if(i.category==category and i.name==item_name):
                        info.append(i)
            else:
                for i in self.products:
                    i=i[0]
                    if(i.name==item_name):
                        info.append(i)
        print(info)
        if(len(info)==0):
            print("Here")
            market_product_reply=market_buyer_pb2.ProductDisplayResponse()
            yield market_product_reply
        else:
            for product_info in info:
                market_product_reply = market_buyer_pb2.ProductDisplayResponse()
                market_product_reply.id=product_info.id
                market_product_reply.price=product_info.price
                market_product_reply.name=product_info.name
                market_product_reply.quantityRemaining=product_info.quantity
                market_product_reply.rating=product_info.ratings
                market_product_reply.sellerAddress = product_info.seller_address
                market_product_reply.productCategory = product_info.category
                yield market_product_reply
    def BuyItem(self, request, context):
        print("Hello")
        return super().BuyItem(request, context)

    def AddWish(self,request,context):
        return super().AddWish(request,context)
    
    def RateItem(self, request, context):
        return super().RateItem(request, context)
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    market=MarketPlaceService()
    market_seller_pb2_grpc.add_MarketPlaceServicer_to_server(market,server)
    market_buyer_pb2_grpc.add_MarketPlaceServicer_to_server(market,server)
    server.add_insecure_port("localhost:50051")
    server.start()
    server.wait_for_termination()

if __name__=="__main__":
    serve()