create table fruit_orders (
    id int auto_increment primary key,
    fruit_id int references fruit(id),
    customer_name varchar2(30)
);
