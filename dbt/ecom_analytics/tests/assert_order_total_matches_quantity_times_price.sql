-- Returns rows that VIOLATE the rule. Zero rows = test passes.
select
    order_id,
    quantity,
    unit_price,
    total_amount
from {{ ref('stg_orders') }}
where abs(total_amount - (quantity * unit_price)) > 0.01