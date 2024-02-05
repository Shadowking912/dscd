from concurrent import futures

import grpc
import market_seller_pb2
import market_seller_pb2_grpc
import market_buyer_pb2
import market_buyer_pb2_grpc

from Helper_Classes.Product import Product
from Helper_Classes.Seller  import Seller
from Helper_Classes.Buyer import Buyer

class MarketPlaceService(market_seller_pb2_grpc.MarketPlaceServicer,market_buyer_pb2_grpc.MarketPlaceServicer):
    
    def __init__(self):
        self.sellers={}
        self.currentProducts=0
        self.buyers={}
        self.products = {}


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
        print(f"Sell Item request from {context.peer()}[ip:port]")
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
            print(product_description)
            new_product = Product(product_id,product_name,product_category,product_quantity,product_description,producrprice_per_unit,0,self.sellers[seller_uuid].address)
            # print(self.sellers[seller_uuid].uuid)
            self.sellers[seller_uuid].add_product(new_product)
            self.products[new_product.id] = new_product
            status_response.status="SUCCESS"
        return status_response
    
    def notification_info(self,desired_product,client,address):
        file=None
        stub=None
        if client=="Buyer":
            channel= grpc.insecure_channel(address)
            stub = market_buyer_pb2_grpc.BuyerNotificationServerStub(channel)
            file=market_buyer_pb2
        else:
            channel= grpc.insecure_channel(address)
            stub= market_seller_pb2_grpc.SellerNotificationServerStub(channel)
            file=market_seller_pb2

        product=desired_product
        market_product_request =file.ProductDisplayResponse()
        market_product_request.id=product.id
        market_product_request.price=product.price
        market_product_request.name=product.name
        market_product_request.quantityRemaining=product.quantity
        market_product_request.rating=product.ratings
        market_product_request.Address = product.seller_address
        market_product_request.productCategory = product.category
        market_product_request.description=product.description
        market_product_request =file.Notification(notification=market_product_request)
        notification_response = stub.ReceiveNotification(market_product_request)
        print(notification_response)
    
    def UpdateItem(self, request, context):
        print("updating")
        product_id = request.id
        print(f"Update Item {product_id}[id] request from {context.peer()}[ip:port]")
        seller_uuid = request.uuid
        updated_quantity = request.newQuantity
        updated_price = request.newPrice
        status_response = market_seller_pb2.StatusResponse()
        if seller_uuid not in self.sellers.keys():
            status_response.status = "FAILURE"
            return status_response
        product_list = self.sellers[seller_uuid].get_product_list()
        print("Product List = ",product_list)
        if product_id not in product_list.keys():
            status_response.status="FAILURE"
            return status_response
        desired_product = product_list[product_id]
        desired_product.quantity = updated_quantity
        desired_product.price = updated_price
        status_response.status="SUCCESS"

        #send notification
        for i in self.buyers:
            if desired_product in self.buyers[i].wishlist:
                self.notification_info(desired_product,"Buyer",self.buyers[i].address)
        
        return status_response

    def DeleteItem(self, request, context):
        product_id = request.id
        seller_uuid = request.uuid
        print(f"Delete Item {product_id}[id] request from {context.peer()}")
        status_response = market_seller_pb2.StatusResponse()
        if seller_uuid not in self.sellers.keys():
            status_response.status="FAILURE"
            return status_response
        
        product_list = self.sellers[seller_uuid].get_product_list()
        if product_id not in product_list.keys():
            status_response.status="FAILURE"
            return status_response
        
        del product_list[product_id]
        del self.products[product_id]

        for i in self.buyers:
            if (product_id in self.buyers[i].wishlist):
                del self.buyers[i].wishlist[product_id]
        status_response.status = "SUCCESS"

        
        return status_response
    
    def DisplaySellerItems(self, request, context):
        print(f"Display Items request from {context.peer()}")
        seller_display_reply = market_seller_pb2.ProductDisplayResponse()
        seller_uuid = request.uuid
        if seller_uuid not in self.sellers:
            return seller_display_reply    
        seller = self.sellers[seller_uuid]
        product_list = seller.get_product_list()
        print("Product List = ",product_list)
        if len(product_list)==0:
            return seller_display_reply
        else:
            for product in product_list.values():
                market_product_reply = market_seller_pb2.ProductDisplayResponse()
                market_product_reply.id=product.id
                market_product_reply.price=product.price
                market_product_reply.name=product.name
                market_product_reply.quantityRemaining=product.quantity
                market_product_reply.rating=product.ratings
                market_product_reply.Address = product.seller_address
                market_product_reply.productCategory = product.category
                market_product_reply.description=product.description
                yield market_product_reply
    #buyers
    def SearchItem(self,request,context):
        item_name=request.item_name
        category=request.category
        info=[]
        if(item_name==""):
            if(category!=3):
                for i in self.products:
                    if(self.products[i].category==category):
                        info.append(self.products[i])
            else:
                for i in self.products:
                    info.append(self.products[i])
        else:
            if(category!=3):
                for i in self.products:
                    if(self.products[i].category==category and self.products[i].name==item_name):
                        info.append(self.products[i])
            else:
                for i in self.products:
                    if(self.products[i].name==item_name):
                        info.append(self.products[i])
        if(len(info)==0):
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
                market_product_reply.Address = product_info.seller_address
                market_product_reply.productCategory = product_info.category
                market_product_reply.description=product_info.description
                yield market_product_reply
    
    def BuyItem(self, request, context):
        itemid=request.id
        qty=request.quantity
        buyer_addr=request.Address
        print(f"Buy request {qty}[quantity] of item {itemid}[item id] from {context.peer()}[buyer address]")
        status_response = market_buyer_pb2.StatusResponse()
        if itemid in self.products:
            if(self.products[itemid].quantity>=qty):
                self.products[itemid].quantity-=qty
                status_response.status="SUCCESS"

                #send notification
                self.notification_info(self.products[itemid],"Seller",self.products[itemid].seller_address)
        else:           
            status_response.status="FAILURE"
        return status_response

    def AddWish(self,request,context):
        itemid=request.id
        buyeruuid=request.uuid
        addr=request.Address
        status_response = market_buyer_pb2.StatusResponse()
        if itemid not in self.products.keys():
            status_response.status="FAILURE"
            return status_response
        if(buyeruuid in self.buyers):
            self.buyers[buyeruuid].wishlist[itemid]=self.products[itemid]
        else:
            new_buyer=Buyer(buyeruuid)
            new_buyer.wishlist[itemid]=self.products[itemid]
            self.buyers[buyeruuid]=new_buyer 
        self.buyers[buyeruuid].address=addr
        status_response.status="SUCCESS"    
        return status_response
    
    def RateItem(self, request, context):
        itemid=request.id
        rating=request.rating
        status_response = market_buyer_pb2.StatusResponse()
        if itemid not in self.products.keys():
            status_response.status="FAILURE"
        else:
            self.products[itemid].rating_list.append(rating)
            self.products[itemid].ratings=sum(self.products[itemid].rating_list)/(len(self.products[itemid].rating_list))
            status_response.status="SUCCESS"
        return status_response

def serve():

  
    # Adding the market place server 
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    market=MarketPlaceService()
    market_seller_pb2_grpc.add_MarketPlaceServicer_to_server(market,server)
    market_buyer_pb2_grpc.add_MarketPlaceServicer_to_server(market,server)
    server.add_insecure_port("localhost:50050")
    server.start()
    server.wait_for_termination()


if __name__=="__main__":
    serve()