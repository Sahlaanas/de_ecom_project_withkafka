-- Grain: one row per order line item (order_id + product_id).
-- Built incremental + merge so re-runs don't duplicate rows and late-arriving
-- events for a given order_id/product_id overwrite the previous version.
-- (For the portfolio-build phase, `dbt run` with no --full-refresh is fine;
-- this is the pattern to point to when an interviewer asks "how do you handle
-- late-arriving / duplicate events?")

{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id'],
        incremental_strategy='merge'
    )
}}

select
    o.event_id,
    o.order_id,
    o.customer_id,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.total_amount,
    o.payment_method,
    o.shipping_country,
    o.order_timestamp,
    date_trunc('day', o.order_timestamp) as order_date
from {{ ref('stg_orders') }} o

{% if is_incremental() %}
where o.order_timestamp > (select coalesce(max(order_timestamp), '1900-01-01') from {{ this }})
{% endif %}
