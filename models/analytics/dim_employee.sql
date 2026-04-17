-- compute whether the employee is still active and tenure days for each employee
with employees as (
    select * from {{ ref('int_employee') }}
),

final as (
    select
        *,
        -- 业务逻辑放这里
        case
            when quit_date is not null then false
            else true
        end                                                      as is_active,

        datediff('day', hire_date, coalesce(quit_date, current_date()))
                                                                 as tenure_days
    from employees
)

select * from final