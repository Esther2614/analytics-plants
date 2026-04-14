-- models/base_model/base_web_schema__orders.sql

with source as (

    select * from {{ source('load_web', 'orders') }}

),

cleaned as (

    select

        trim(order_id)                                              as order_id,

        trim(session_id)                                            as session_id,
        trim(client_name)                                           as client_name,
        trim(phone)                                                 as phone,
        trim(shipping_address)                                      as shipping_address,
        trim(state)                                                 as state,
        order_at                                                    as ordered_at,
        trim(payment_info)                                          as payment_info,
        lower(trim(payment_method))                                 as payment_method,
        try_to_number(
            replace(replace(shipping_cost, 'USD', ''), ' ', ''),
            10, 2
        )                                                           as shipping_cost,
        tax_rate,

        "_fivetran_deleted"                                         as is_deleted,
        "_fivetran_synced"                                          as synced_at

    from source

)

select * from cleaned