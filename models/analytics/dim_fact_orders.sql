-- add 3 columns for subsequent analytics:
--is_returned
--total_shipping_cost,
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

        -- 退货相关
        returned_date,
        is_refunded,
        case
            when returned_date is not null then true
            else false
        end                                                         as is_returned,

        -- 费用计算
        round(shipping_cost * (1 + tax_rate), 2)                   as total_shipping_cost,

        -- 从下单到退货的天数
        datediff('day', ordered_at::date, returned_date)            as days_to_return

    from orders
    where is_deleted = false    -- 过滤软删除订单
)

select * from final