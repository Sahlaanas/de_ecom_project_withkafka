select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    full_name,
    email,
    city,
    country,
    signup_date
from {{ ref('stg_customers') }}