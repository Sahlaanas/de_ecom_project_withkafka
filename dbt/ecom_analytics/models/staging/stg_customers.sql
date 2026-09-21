select
    customer_id,
    full_name,
    email,
    city,
    country,
    signup_date,
    snapshot_date
from {{ source('raw', 'raw_customers') }}
qualify row_number() over (
    partition by customer_id
    order by snapshot_date desc
) = 1