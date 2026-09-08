-- Silver: clean customers bronze table.

with source as (
    select * from {{ source('bronze', 'customers') }}
)

select
    customer_id,
    cast(email as string) as email,
    cast(signup_date as date) as signup_date,
    cast(region as string) as region,
    cast(updated_at as timestamp) as updated_at
from source
qualify row_number() over (
    partition by customer_id order by updated_at desc
) = 1
