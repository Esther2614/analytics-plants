with source as (

    select * from {{ source('load_web', 'page_views') }}

),

cleaned as (

    select

        "_fivetran_id"                                              as page_view_id,


        trim(session_id)                                            as session_id,
        lower(trim(page_name))                                      as page_name,
        view_at                                                     as viewed_at,

        "_fivetran_deleted"                                         as is_deleted,
        "_fivetran_synced"                                          as synced_at

    from source

)

select * from cleaned