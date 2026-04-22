-- add 3 columns for subsequent analytics:
--is_returned and total_shipping_cost,
--Number of days from order to return
with orders as (
    select * from {{ ref('int_fact_orderds') }}
),

final as (
    select
        order_id,
        session_id,
        client_name,
        phone,
        shipping_address,
        state,
        ordered_at,
        payment_method,
        shipping_cost,
        tax_rate,
        --is_deleted,

        -- refund-related columns
        returned_date,
        is_refunded,
        case
            when returned_date is not null then true
            else false
        end                                                         as is_returned,

        -- computing fees 
        round(shipping_cost, 2)                   as total_shipping_cost,

        -- days from making the order to return
        datediff('day', ordered_at::date, returned_date)            as days_to_return,
        gross_revenue

    from orders
    where is_deleted = false    
)

select * from final