-- Gold: daily revenue by region. This is the table Phase 5's BI dashboard
-- and Phase 7's wastage checks will reference by name.

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        o.order_id,
        o.order_amount,
        o.created_at,
        c.region
    from orders o
    left join customers c on o.customer_id = c.customer_id
    where o.order_status = 'completed'
)

select
    date_trunc('day', created_at) as order_date,
    region,
    count(order_id) as order_count,
    sum(order_amount) as total_revenue
from joined
group by 1, 2
