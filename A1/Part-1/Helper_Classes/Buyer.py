class Buyer:
    def __init__(self,uuid,buyer_notification_server_address):
        self.wishlist={}
        self.id=uuid
        self.address = buyer_notification_server_address