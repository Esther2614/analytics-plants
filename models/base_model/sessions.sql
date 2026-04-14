with source as (

    select * from {{ source('load_web', 'sessions') }}

),

cleaned as (

    select

        trim(session_id)                                            as session_id,


        client_id,
        try_to_timestamp_ntz(session_at)                            as session_started_at,
        lower(trim(os))                                             as os,
        trim(ip)                                                    as ip_address,


        "_fivetran_deleted"                                         as is_deleted,
        "_fivetran_synced"                                          as synced_at

    from source

)

select * from cleaned