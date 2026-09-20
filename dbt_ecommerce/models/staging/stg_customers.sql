-- In this simulated pipeline, customer attributes only appear on purchase events
-- (email, shipping_country). In a real system you'd more likely source customers
-- from a CDC feed off the app's OLTP database rather than inferring from events —
-- this staging model shows the pattern for when events are your only source.

with purchases as (

    select raw_data from {{ source('raw', 'purchase_events') }}

),

page_views as (

    select raw_data from {{ source('raw', 'page_view_events') }}

),

customer_from_purchases as (

    select
        raw_data:customer_id::string  as customer_id,
        raw_data:customer_email::string as email,
        raw_data:shipping_country::string as country,
        raw_data:event_timestamp::timestamp_ntz as seen_at
    from purchases

),

customer_from_views as (

    select
        raw_data:customer_id::string  as customer_id,
        cast(null as string)          as email,
        cast(null as string)          as country,
        raw_data:event_timestamp::timestamp_ntz as seen_at
    from page_views

),

unioned as (

    select * from customer_from_purchases
    union all
    select * from customer_from_views

),

ranked as (

    select *,
        row_number() over (
            partition by customer_id
            order by (email is not null) desc, seen_at desc
        ) as rn
    from unioned
    where customer_id is not null

)

select
    customer_id,
    email,
    country,
    min(seen_at) over (partition by customer_id) as first_seen_at
from ranked
where rn = 1
