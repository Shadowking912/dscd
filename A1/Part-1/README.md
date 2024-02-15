

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

1. Clone this repository:

    ```bash
    git clone https://github.com/your_username/grpc-marketplace.git
    ```

2. Navigate to the project directory:

    ```bash
    cd grpc-marketplace
    ```
# Components
## 1.Marketplace

### Usage

Run the script to start the marketplace 
service:

```bash
python market.py <market_address_ip:port>
```

- `<market_external_ip_address:port>`: IP address and port where the marketplace service will be hosted.

### Functionality

### Product Class (`Product`)

- **Initialization**: Creates a product object with various attributes like ID, name, category, quantity, price, etc.
- **Rating Management**:
    - `rate_product(rating)`: Adds a rating to the product.

### Seller Class (`Seller`)

- **Initialization**: Creates a seller object with a UUID and notification server address.
- **Product Management**:
    - `add_product(product)`: Adds a product to the seller's list of products.
    - `update_product()`: Updates product information.

### Buyer Class (`Buyer`)

- **Initialization**: Creates a buyer object with a UUID and notification server address.
- **Wishlist Management**:
    - `add_to_wishlist(product_id)`: Adds a product to the buyer's wishlist.



### MarketPlaceService

- **Registration**:
    - `RegisterSeller(request)`: Registers a seller with the marketplace.
- **Product Management**:
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

## 2.Seller

### Usage

Run the script to start the seller 
service:

```bash
python seller.py <notification_server_external_ip:port> <market_address_ip:port>
```
- `<notification_server_external_ip:port>`: External IP address and port where the notification server of seller will be hosted.

- `<market_external_ip_address:port>`: IP address and port where the marketplace server will be hosted.

### Functionality

1. **Register as a Seller**:
    - `register()`: Allows sellers to register themselves with the marketplace.

2. **Sell Item**:
    - `sell()`: Allows sellers to list a new item for sale.

3. **Update Item Record**:
    - `update()`: Enables sellers to update the quantity and price of their listed items.

4. **Delete Item**:
    - Allows sellers to remove a listed item from the marketplace.

5. **Display Listed Items**:
    - `displayItems()`: Displays all the items listed by the seller.

6. **Exit**:
    - Quits the client application.



## 3.Buyer Client

This Python script serves as a client for interacting with the gRPC-based marketplace service as a buyer. Buyers can search for items, buy items, add items to their wishlist, rate items, and display their wishlist using this client.

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
    - Allows buyers to search for items based on item name and category.

2. **Buy Item**:
    - Allows buyers to purchase items by specifying the item ID and quantity.

3. **Add to Wishlist**:
    - Enables buyers to add items to their wishlist by specifying the item ID.

4. **Rate Item**:
    - Allows buyers to rate items by specifying the item ID and rating.

5. **Display Wishlist**:
    - Displays the wishlist containing items added by the buyer.

6. **Logout**:
    - Quits the client application.

7. **Receive Notification**:
    - Provides buyers with notifications containing details of items they are interested in, such as price, name, category, description, quantity remaining, rating, and seller address.