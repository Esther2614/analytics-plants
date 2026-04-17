-- orders left join returns
with orders as (
    select * from {{ ref('orders') }}
),

returns as (
    select * from {{ ref('returns') }}
),

final as (
    select
        o.order_id,
        o.session_id,
        o.client_name,
        o.phone,
        o.shipping_address,
        o.state,
        o.ordered_at,
        o.payment_method,
        o.shipping_cost,
        o.tax_rate,
        o.is_deleted,
        r.returned_date,
        r.is_refunded
    from orders o
    left join returns r
        on o.order_id = r.order_id
)

select * from final