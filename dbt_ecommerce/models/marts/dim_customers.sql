select
    customer_id,
    email,
    country,
    first_seen_at
from {{ ref('stg_customers') }}
