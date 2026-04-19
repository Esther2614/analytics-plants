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

),
-- fix duplicate error
t1 AS
(
    SELECT
        session_id,
        client_id,
        session_started_at,
        os,
        ip_address,
        is_deleted,
        synced_at,
        row_number() OVER (PARTITION BY session_id ORDER BY session_started_at DESC) as rn
    FROM cleaned
)
SELECT
    session_id,
    client_id,
    session_started_at,
    os,
    ip_address,
    is_deleted,
    synced_at
FROM t1 
WHERE rn=1