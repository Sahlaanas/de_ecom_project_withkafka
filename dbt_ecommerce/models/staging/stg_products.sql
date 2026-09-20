with purchases as (

    select
        raw_data:product_id::string   as product_id,
        raw_data:product_name::string as product_name,
        raw_data:category::string     as category,
        raw_data:unit_price::float    as unit_price,
        raw_data:event_timestamp::timestamp_ntz as seen_at
    from {{ source('raw', 'purchase_events') }}

),

inventory as (

    select
        raw_data:product_id::string   as product_id,
        raw_data:product_name::string as product_name,
        cast(null as string)          as category,
        cast(null as float)           as unit_price,
        raw_data:event_timestamp::timestamp_ntz as seen_at
    from {{ source('raw', 'inventory_events') }}

),

unioned as (

    select * from purchases
    union all
    select * from inventory

),

ranked as (

    select *,
        row_number() over (
            partition by product_id
            order by (category is not null) desc, seen_at desc
        ) as rn
    from unioned
    where product_id is not null

)

select
    product_id,
    product_name,
    category,
    unit_price
from ranked
where rn = 1
