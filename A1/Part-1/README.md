

This Python script implements a marketplace service using gRPC, allowing sellers to register, add products, and update product information, and buyers to search for items, add them to their wishlist, and make purchases.

## Dependencies

- Python 3.x
- gRPC

## Installation

1. Install Python 3.x from [python.org](https://www.python.org/downloads/)
2. Install gRPC using pip:

    ```bash
    pip install grpcio grpcio-tools
    ```

## Usage


1. Navigate to the project directory:

    ```bash
    cd Part-1
    ```

2. Generating stub code from the proto files

    ```bash
    python -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/market_seller.proto

    python -m grpc_tools.protoc -I protos --python_out=. --grpc_python_out=. protos/market_buyer.proto
    ```


# Proto-files
## 1. market_seller.proto
-  Includes the declaration of the functionality for handling the seller and marketplace interaction. 
- Contains the message formats along with two other functionalities:-
    - The functionality of the marketplace handles the interaction between the seller and the marketplace.
    - The notification server functionality of the server.

## 2. market_buyer.proto
- Includes the declaration of the functionality for handling the buyer and marketplace interaction. 
- Contains the message formats along with two other functionalities:-
    - The functionality of the marketplace handles the interaction between the buyer and the marketplace.
    - The notification server functionality of the buyer.


# Components

### Structure
- `market.py` : Main python script containing the `MarketPlaceService` class and main execution logic.
- `client.py` : Python Script for the buyer(client) and containing the `BuyerNotificationServer` class for buyers.
- `seller.py` : Python Script for the seller and containing the `SellerNotificationServer` class for sellers.

## 1.Marketplace

## Usage

Run the script to start the marketplace 
service:

```bash
python market.py <market_address_ip:port>
```

- `<market_external_ip_address:port>`: IP address and port where the marketplace service will be hosted.


## Assosciated Classes

- **Product Class**
    - Represents a product sold on the market
    - Every instance of the product class has an assosciated id,category,name,category,description,price_per_unit,,seller_address and rating_list storing rating given by different buyers to the product

- **Seller Class**
    - Represents a seller registered on the marketplace.
    - Every instance of the seller class has an assosciated unqiue id (uuid) an address (notification server adress and port of the seller) and a dictionary storing (product_id:product_object_list) for storing the products sold by that seller. 
    - It also has functionalities implemented for adding a product to the dicitonary and getting the product list.

- **Buyer Class**
    - Represents a buyer registered on the marketplace.
    - Every instance of buyer class has an assosciated unique id (uuid) and a dictionary storing the (buyer_uuid : product_list) storing the list of products added to the wishlist by the given buyer.

## Functionality

### 1. MarketPlaceService

- **Seller Interaction**:
    - `RegisterSeller(request)`: Registers a seller with the marketplace.
    - `SellItem(request)`: Adds a new product to the marketplace.
    - `UpdateItem(request)`: Updates product information in the marketplace.
    - `DeleteItem(request)`: Deletes a product from the marketplace.
- **Product Display**:
    - `DisplaySellerItems(request)`: Displays products listed by a seller.
    - `SearchItem(request)`: Searches for products based on name and category.
- **Buyer Interaction**:
    - `BuyItem(request)`: Allows a buyer to purchase an item.
    - `AddWish(request)`: Adds an item to the buyer's wishlist.
    - `RateItem(request)`: Allows a buyer to rate a product.
    - `DisplayWishlist(request)`: Displays the buyer's wishlist.

- **Sending Notifications**:
    - `notification_info()`: Sends notificaiton to the server or client notificaiton server:-
        - Sends notification to the client notification server if the seller updates a product in the wishlisht of the client
        - Sends notification to the seller notificaiton server if the client buys a product listed by that particular seller.

- `serve()`
    - Creates server object using grpc.server()
    - Calls the constructor of the MarketPlaceService() class for creating an instance of the MarketPlaceService class.
    - Starts the server and listens for incoming connections

## 2. Seller

### Usage

Run the script to start the seller 
service:

```bash
python seller.py <notification_server_external_ip:port> <market_address_ip:port>
```
- `<notification_server_external_ip:port>`: External IP address and port where the notification server of seller will be hosted.

- `<market_external_ip_address:port>`: IP address and port where the marketplace server is hosted.

### Functionality

**Seller Functionalities**

1. **Register as a Seller**:
    - `register()`: Allows sellers to register themselves with the marketplace.

2. **Sell Item**:
    - `sell()`: Allows sellers to list a new item for sale.

3. **Update Item Record**:
    - `update()`: Enables sellers to update the quantity and price of their listed items.

4. **Delete Item**:
    - `delete()`: Allows sellers to remove a listed item from the marketplace.

5. **Display Listed Items**:
    - `displayItems()`: Displays all the items listed by the seller.

6. **Exit**:
    - Quits the client application.

7. **Receive Notificaiton**
    - `ReceiveNotification()`:Receives the notification from the server and prints it on the seller side when the seller buys a product list by that particular seller which includes details of items they are interested in, such as price, name, category, description, quantity remaining, rating, and seller address.

## 3. Client

### Usage

Run the script to start the buyer 
service:

```bash
python client.py <notification_server_external_ip:port> <market_address_ip:port>
```
- `<notification_server_external_ip:port>`: External IP address and port where the notification server of buyer will be hosted.

- `<market_external_ip_address:port>`: IP address and port where the marketplace server will be hosted.

## Functionality

1. **Search Item**:
    - `SearchItem()`: Allows buyers to search for items based on item name and category.

2. **Buy Item**:
    - `BuyItem()` : Allows buyers to purchase items by specifying the item ID and quantity.

3. **Add to Wishlist**:
    - `AddTOWishList()` : Enables buyers to add items to their wishlist by specifying the item ID.

4. **Rate Item**:
    - `RateItem()` : Allows buyers to rate items by specifying the item ID and rating.

5. **Display Wishlist**:
    - `DsplayWishlist()` : Displays the wishlist containing items added by the buyer.

6. **Logout**:
    - Quits the client application.

7. **Receive Notification**:
    - Provides buyers with notifications containing details of items they are interested in, such as price, name, category, description, quantity remaining, rating, and seller address when the seller updates a product that the seller has in its wishlist.