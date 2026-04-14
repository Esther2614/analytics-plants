-- models/base_model/base_google_drive__hr_quits.sql

with source as (

    select * from {{ source('load_drive', 'hr_quits') }}

),

cleaned as (

    select

        employee_id,

        quit_date,


        _fivetran_synced

    from source

)

select * from cleaned