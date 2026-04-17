--left join quit_date
with joins as (
    select * from {{ ref('hr_joins') }}
),

quits as (
    select
        employee_id,
        max(quit_date) as quit_date
    from {{ ref('hr_quits') }}
    group by employee_id
),

final as (
    select
        j.employee_id,
        j.employee_name,
        j.city,
        j.address,
        j.job_title,
        j.annual_salary,
        j.hire_date,
        q.quit_date        -- 只是把两张表拼在一起，不做任何判断
    from joins j
    left join quits q
        on j.employee_id = q.employee_id
)

select * from final