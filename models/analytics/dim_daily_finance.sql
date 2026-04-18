-- Aggregates daily financial data from int_daily_finances
-- Final metric: net_profit = net_revenue - total_cost

with int_daily_finances as (
    select * from {{ ref('int_daily_finance') }}
),

final as (
    select
        finance_date,

        -- revenue
        sum(net_revenue)                            as total_net_revenue,

        -- cost 
        sum(total_cost)                             as total_cost,

        -- net profit
        sum(net_revenue) - sum(total_cost)          as net_profit

    from int_daily_finances
    group by 1
)

select * from final
order by finance_date