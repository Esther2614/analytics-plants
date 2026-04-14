with source as (

    select * from {{ source('load_web', 'item_views') }}

),

cleaned as (

    select

        "_fivetran_id"                                              as item_view_id,


        trim(session_id)                                            as session_id,
        trim(item_name)                                             as item_name,
        item_view_at                                                as viewed_at,
        price_per_unit,
        add_to_cart_quantity,
        remove_from_cart_quantity,


        "_fivetran_deleted"                                         as is_deleted,
        "_fivetran_synced"                                          as synced_at

    from source

)

select * from cleaned