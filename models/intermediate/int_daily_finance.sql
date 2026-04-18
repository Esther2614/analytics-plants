--Fact: 1 order_id-->1 session_id, 1 session_id--> 1+ item_view

-- Assumptions:
    -- Tax is excluded from cost calculation as TAX_RATE represents US Sales Tax, which is collected from customers
    -- shipping costs is not covered in the expense table.
    -- Full refunds are provided for all cases.

-- Revenue Calculation:
    --   gross_revenue = price_per_unit * (add_to_cart_quantity - remove_from_cart_quantity), aggregated by session
    --   refunded_amount = gross_revenue of orders that have been refunded
    --   net_revenue = gross_revenue - refunded_amount

-- Cost Calculation:
    --   expense_cost = operational costs aggregated by day from expenses table (hr, tech_tool, warehouse, other)
    --   shipping_cost = shipping costs aggregated by day from orders table
    --   total_cost = expense_cost + shipping_cost + tax_cost

-- Table Joins:
-- 1. item_views(already grouped by session_id) INNER JOIN orders (on session_id): 
    -- to confirm which sessions resulted in actual orders.
-- 2. LEFT JOIN returns (on order_id): 
    -- to flag which orders have been refunded
-- 3. FULL OUTER JOIN daily_costs(including expenses and shipping costs) (on finance_date): 
    -- to merge revenue and costs, preserving all dates



with orders as (
    select * from {{ ref('orders') }}
    where is_deleted = false
),

item_views as (
    select * from {{ ref('views') }}
    where is_deleted = false
),

order_returns as (
    select * from {{ ref('returns') }}
    where is_refunded = true
),

expenses as (
    select * from {{ ref('expenses') }}
),

-- aggregate item_views by session first 
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

-- join to orders to confirm actual purchases, one row per order
order_revenue as (
    select
        o.order_id,
        o.ordered_at::date                                          as finance_date,
        o.shipping_cost,
        sr.gross_revenue
    from orders o
    inner join session_revenue sr
        on o.session_id = sr.session_id
),

-- flag refunded orders
revenue_with_returns as (
    select
        or_.finance_date,
        or_.gross_revenue,
        or_.shipping_cost,
        case
            when r.order_id is not null then or_.gross_revenue
            else 0
        end                                                         as refunded_amount
    from order_revenue or_
    left join order_returns r
        on or_.order_id = r.order_id
),

-- aggregate revenue and shipping costs by day
daily_revenue as (
    select
        finance_date,
        sum(gross_revenue)                                          as gross_revenue,
        sum(refunded_amount)                                        as refunded_amount,
        sum(gross_revenue) - sum(refunded_amount)                   as net_revenue,
        sum(shipping_cost)                                          as shipping_cost
    from revenue_with_returns
    group by 1
),

-- aggregate operational costs by day
daily_expense_costs as (
    select
        expense_date                                                as finance_date,
        sum(expense_amount)                                         as expense_cost
    from expenses
    group by 1
),

final as (
    select
        coalesce(r.finance_date, e.finance_date)                    as finance_date,
        coalesce(r.gross_revenue, 0)                                as gross_revenue,
        coalesce(r.refunded_amount, 0)                              as refunded_amount,
        coalesce(r.net_revenue, 0)                                  as net_revenue,
        coalesce(r.shipping_cost, 0)                                as shipping_cost,
        coalesce(e.expense_cost, 0)                                 as expense_cost,
        coalesce(r.shipping_cost, 0) +
        coalesce(e.expense_cost, 0)                                 as total_cost
    from daily_revenue r
    full outer join daily_expense_costs e
        on r.finance_date = e.finance_date
)

select * from final