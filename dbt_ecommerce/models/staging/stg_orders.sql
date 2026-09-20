-- Flattens raw VARIANT purchase events into a typed, deduplicated staging table.
-- Grain: one row per (order_id, product_id) — an order line item.

with source as (

    select raw_data from {{ source('raw', 'purchase_events') }}

),

flattened as (

    select
        raw_data:event_id::string        as event_id,
        raw_data:order_id::string        as order_id,
        raw_data:customer_id::string     as customer_id,
        raw_data:product_id::string      as product_id,
        raw_data:product_name::string    as product_name,
        raw_data:category::string        as category,
        raw_data:unit_price::float       as unit_price,
        raw_data:quantity::int           as quantity,
        raw_data:total_amount::float     as total_amount,
        raw_data:payment_method::string  as payment_method,
        raw_data:shipping_country::string as shipping_country,
        raw_data:event_timestamp::timestamp_ntz as order_timestamp
    from source

),

deduped as (

    select *,
        row_number() over (
            partition by order_id, product_id
            order by order_timestamp desc
        ) as rn
    from flattened

)

select
    event_id,
    order_id,
    customer_id,
    product_id,
    product_name,
    category,
    unit_price,
    quantity,
    total_amount,
    payment_method,
    shipping_country,
    order_timestamp
from deduped
where rn = 1
  and order_id is not null
  and customer_id is not null
