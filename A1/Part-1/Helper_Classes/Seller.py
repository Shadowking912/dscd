from Helper_Classes.Product import Product
class Seller:
    def __init__(self,uuid,seller_notification_server_address):
        self.uuid = uuid
        self.address = seller_notification_server_address
        self.__product_list={}
    
    def add_product(self,product):
        self.__product_list[product.id] = product
    
    def update_product(self):
        pass

    def get_product_list(self):
        return self.__product_list