-- orders left join returns
with orders as (
    select * from {{ ref('orders') }}
),

returns as (
    select * from {{ ref('returns') }}
),
item_views as (
    select * from {{ ref('views') }}
),
session_revenue as (
    select
        session_id,
        sum(
            price_per_unit *
            (add_to_cart_quantity - remove_from_cart_quantity)
        )                                                           as gross_revenue
    from item_views
    group by session_id
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
        r.is_refunded,
        coalesce(sr.gross_revenue, 0) as gross_revenue
    from orders o
    left join returns r
        on o.order_id = r.order_id
    left join session_revenue sr                                                       
        on o.session_id = sr.session_id
)

select * from final