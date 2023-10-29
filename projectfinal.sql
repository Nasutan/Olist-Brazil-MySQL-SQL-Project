CREATE DATABASE myproject;
USE myproject;

CREATE TABLE Sellers (
	seller_id VARCHAR(50) PRIMARY KEY,
	seller_city VARCHAR(50),
	seller_state VARCHAR(50)
);

LOAD DATA LOCAL INFILE
'/Users/nasutan/Downloads/Cleaned-Dataset/Sellers.csv'
INTO TABLE myproject.Sellers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 lines
(seller_id,seller_city, seller_state);

create table Products (
	product_id VARCHAR(50) PRIMARY KEY,
	product_category_name VARCHAR(255),
	product_name_length INT,
	product_descrpition_length INT,
	product_photos_qty INT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT
);

LOAD DATA LOCAL INFILE
'/Users/nasutan/Downloads/Cleaned-Dataset/Products.csv'
INTO TABLE myproject.Products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 lines
(product_id,product_category_name,product_name_length,product_descrpition_length,product_photos_qty,
product_weight_g,product_length_cm,product_height_cm,product_width_cm);

create table Customers (
	customer_id VARCHAR(50) PRIMARY KEY,
        customer_unique_id VARCHAR(50),
	customer_city VARCHAR(255),
	customer_state VARCHAR(10)
);

LOAD DATA LOCAL INFILE
'/Users/nasutan/Downloads/Cleaned-Dataset/Customers.csv'
INTO TABLE myproject.Customers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 lines
(customer_id, customer_unique_id, customer_city, customer_state);

create table Orders (
	order_id VARCHAR(50) PRIMARY KEY,
	customer_id VARCHAR(50),
	order_status VARCHAR(100),
   	order_purchase_timestamp datetime,
	order_approved_at DATETIME,
	order_delivered_carrier_date DATETIME,
	order_delivered_customer_date DATETIME,
	order_estimated_delivery_date DATETIME,
	FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

LOAD DATA LOCAL INFILE
'/Users/nasutan/Downloads/Cleaned-Dataset/Orders.csv'
INTO TABLE myproject.Orders
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 lines
(order_id, customer_id, order_status,order_purchase_timestamp,order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date);

create table OrderDetails (
	order_item_id INT,
	shipping_limit_date datetime,
	price FLOAT,
	freight_value FLOAT,
	order_id VARCHAR(50),
	product_id VARCHAR(50),
	seller_id VARCHAR(50),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (seller_id) REFERENCES Sellers(seller_id)
);

set global local_infile=true;
LOAD DATA LOCAL INFILE
'/Users/nasutan/Downloads/Cleaned-Dataset/OrderDetails.csv'
INTO TABLE myproject.OrderDetails
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 lines
(order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value);


CREATE TABLE Payments (
	order_id VARCHAR(50),
    	payment_squential INT,
    	payment_type VARCHAR(50),
    	payment_installments INT,
    	paymnet_value FLOAT,
	FOREIGN KEY (order_id) REFERENCES Orders (order_id)
);

LOAD DATA LOCAL INFILE
'/Users/nasutan/Downloads/Cleaned-Dataset/Payments.csv'
INTO TABLE myproject.Payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 lines
(order_id, payment_squential, payment_type, payment_installments, paymnet_value);


CREATE TABLE Reviews (
	review_id VARCHAR(50),
    	order_id VARCHAR(50),
    	review_score INT,
    	review_creation_date DATETIME,
    	review_answer_timestamp DATETIME,
    	PRIMARY KEY (review_id,order_id),
    	FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

LOAD DATA LOCAL INFILE
'/Users/nasutan/Downloads/Cleaned-Dataset/Reviews.csv'
INTO TABLE myproject.Reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 lines
(review_id, order_id,review_score, review_creation_date, review_answer_timestamp);


#1. List of products with product photo of less than 2 and total number of purchase
SELECT i.order_id, COUNT(*) AS Total_Purchase
FROM orderdetails as i
INNER JOIN products as p
ON p.product_id = i.product_id
WHERE p.product_photos_qty < 2
GROUP BY i.order_id;


#2. List products with review score of 5 and total number of purchase
SELECT product_category_name, p.product_id, COUNT(*) AS Total_Purchase
FROM orderdetails as i
INNER JOIN products as p
ON p.product_id = i.product_id
INNER JOIN reviews as r
ON r.order_id = i.order_id
WHERE r.review_score = 5
GROUP BY p.product_id;


#3. Products freight value in descending order
SELECT p.product_id, i.freight_value 
FROM products as p
INNER JOIN orderdetails as i
ON i.product_id = p.product_id
ORDER BY i.freight_value DESC;


#4. Number of orders per year
SELECT YEAR(order_purchase_timestamp) AS year,
		COUNT(DISTINCT order_id) AS num_orders             
FROM customers c JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY year;

#5. Average sale of item in certain month, year
select round(avg(od.price),2) avg_sale, year(o.order_purchase_timestamp) year_sale, month(o.order_purchase_timestamp) month_sale
from orderdetails od
inner join orders o
on od.order_id = o.order_id
group by year(o.order_purchase_timestamp), month(o.order_purchase_timestamp)
order by year_sale, month_sale;


#6. freight value based on states in ascending order
select customer_state, avg(i.freight_value) mean_fright
from myproject.orders o
left join myproject.customers c on o.customer_id = c.customer_id
left join myproject.orderdetails i on i.order_id = o.order_id
group by customer_state
order by mean_fright asc;

#7. Cities and states of customers that ordered
SELECT customer_state, customer_city 
FROM customers as c
inner join orders as o on c.customer_id = o.customer_id
left join orderdetails as i on o.order_id = i.order_id
group by customer_state, customer_city ;


#8. Products that has less sales in decresing order.
select product_category_name, count(distinct o.order_id) total_order
from orders as o
inner join customers as c on c.customer_id=o.customer_id
left join orderdetails as i on i.order_id = o.order_id
left join products as p on i.product_id = p.product_id
group by product_category_name
order by total_order desc;


#9. The slow-moving item, Order_delivered_carrier_date - order_delivered_customer_date
select round(avg(datediff(o.order_delivered_customer_date,o.Order_delivered_carrier_date)),2) delivery_time, c.customer_state
from orders o 
inner join customers c on o.customer_id = c.customer_id
group by c.customer_state
order by delivery_time;


#10. Slow packaging
select o.customer_id, datediff(o.Order_approved_at,o.order_delivered_carrier_date) preparation_time, c.customer_city, 
(product_length_cm*product_height_cm*product_width_cm) package_volume_cm_cubic, r.review_score
from orders o 
inner join customers c on o.customer_id = c.customer_id
inner join orderdetails od on o.order_id = od.order_id
inner join products p on od.product_id = p.product_id
inner join reviews r on r.order_id = o.order_id
order by preparation_time desc
limit 5;


#11. Most payment methods
select count(payment_type), payment_type 
 from payments 
 group by payment_type;
 

#12. Top product categories based on the number of reviews
SELECT COUNT(r.review_score) AS review_count, p.product_category_name
FROM reviews r
INNER JOIN orderdetails od ON r.order_id = od.order_id
INNER JOIN (
    SELECT product_id, product_category_name
    FROM products 
) p ON p.product_id = od.product_id
GROUP BY p.product_category_name
ORDER BY review_count DESC
LIMIT 10;


#13. Determine sellers’ revenue and show the top five best sellers. 
select  sum(od.price) total_sale, od.seller_id
from orderdetails od
group by od.seller_id
order by total_sale desc
limit 5;

# MORE QUERIES CAN BE OBTAIN BY CANTQCTING ME ;)









