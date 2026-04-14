-- models/base_model/base_google_drive__expenses.sql

with source as (

    select * from {{ source('load_drive', 'expenses') }}

),

cleaned as (

    select

        date                                                        as expense_date,
        lower(trim(expense_type))                                   as expense_type,
        try_to_number(
            replace(replace(expense_amount, '$', ''), ' ', ''),
            10, 2
        )                                                           as expense_amount,


        _fivetran_synced

    from source

)

select * from cleaned