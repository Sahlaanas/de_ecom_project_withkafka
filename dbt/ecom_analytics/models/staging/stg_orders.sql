with source as (

    select * from {{ source('raw', 'raw_events') }}
    where event_type = 'order'

),

deduplicated as (

    -- Kafka delivers at-least-once, so the same event can appear twice.
    -- Keep exactly one row per event_id (the earliest ingested).
    select *
    from source
    qualify row_number() over (
        partition by event_id
        order by ingested_at, kafka_offset
    ) = 1

),

renamed as (

    select
        event_id,
        payload:order_id::string            as order_id,
        payload:customer_id::string         as customer_id,
        payload:product_id::string          as product_id,
        payload:quantity::int               as quantity,
        payload:unit_price::number(10,2)    as unit_price,
        payload:total_amount::number(10,2)  as total_amount,
        payload:payment_method::string      as payment_method,
        payload:order_status::string        as order_status,
        event_timestamp                     as ordered_at,
        ingested_at
    from deduplicated

)

select * from renamed