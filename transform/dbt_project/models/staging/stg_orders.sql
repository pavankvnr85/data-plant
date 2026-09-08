-- Silver: clean, dedupe, type-cast the raw orders bronze table.

with source as (
    select * from {{ source('bronze', 'orders') }}
),

deduped as (
    select
        order_id,
        customer_id,
        cast(order_amount as decimal(10, 2)) as order_amount,
        cast(order_status as string) as order_status,
        cast(created_at as timestamp) as created_at,
        cast(updated_at as timestamp) as updated_at,
        row_number() over (
            partition by order_id order by updated_at desc
        ) as rn
    from source
)

select
    order_id,
    customer_id,
    order_amount,
    order_status,
    created_at,
    updated_at
from deduped
where rn = 1
