-- models/base_model/base_google_drive__hr_joins.sql

with source as (

    select * from {{ source('load_drive', 'hr_joins') }}

),

cleaned as (

    select

        employee_id,


        try_to_date(trim(substr(hire_date, 4)), 'YYYY-MM-DD')       as hire_date,
        trim(name)                                                  as employee_name,
        lower(trim(city))                                           as city,
        trim(address)                                               as address,
        lower(trim(title))                                          as job_title,
        annual_salary,


        _fivetran_synced

    from source

)

select * from cleaned