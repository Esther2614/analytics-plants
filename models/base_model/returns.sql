-- models/base_model/base_google_drive__returns.sql

with source as (

    select * from {{ source('load_drive', 'returns') }}

),

cleaned as (

    select
        trim(order_id)                                              as order_id,

        returned_at                                                 as returned_date,
        case lower(trim(is_refunded))
            when 'yes' then true
            when 'no' then false
            else null
        end                                                         as is_refunded,

        _fivetran_synced

    from source

)

select * from cleaned