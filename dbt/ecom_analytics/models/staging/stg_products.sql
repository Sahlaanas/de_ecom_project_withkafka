-- Catalogs arrive as dated snapshots; keep the latest version of each product.
select
    product_id,
    product_name,
    category,
    unit_price,
    snapshot_date
from {{ source('raw', 'raw_products') }}
qualify row_number() over (
    partition by product_id
    order by snapshot_date desc
) = 1