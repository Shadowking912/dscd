class Product:
    def __init__(self,id,name,category,quantity,description,price_per_unit,ratings,seller_address):
        self.id = id
        self.name = name
        self.category=category
        self.quantity = quantity
        self.description = description
        self.price = price_per_unit
        self.ratings = ratings
        self.rating_list=[]
        self.seller_address = seller_address