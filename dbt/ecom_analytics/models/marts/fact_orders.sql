-- Grain: one row per order.
with orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select customer_key, customer_id from {{ ref('dim_customers') }}

),

products as (

    select product_key, product_id from {{ ref('dim_products') }}

)

select
    o.order_id,
    c.customer_key,
    p.product_key,
    to_number(to_char(o.ordered_at::date, 'YYYYMMDD')) as order_date_key,
    o.ordered_at,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.payment_method,
    o.order_status
from orders o
left join customers c on o.customer_id = c.customer_id
left join products  p on o.product_id  = p.product_id
