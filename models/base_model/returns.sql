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
-- ,deduplicated as (

--     select *,
--         row_number() over (
--             partition by order_id
--             order by returned_date asc
--         ) as rn
--     from cleaned

-- )

-- select
--     order_id,
--     returned_date,
--     is_refunded,
--     _fivetran_synced
-- from deduplicated
-- where rn = 1

select * from cleaned

