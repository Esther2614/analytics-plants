-- only contain the latest info of the clients who have ordered
-- 1. sessions info
WITH tot_clients_1 AS 
(
    SELECT
        client_id,
        session_id,
        session_started_at,
        OS,
        IP_ADDRESS
    FROM {{ ref('sessions') }}
),
-- 2. orders info
tot_orders_1 AS
(
    SELECT
        ORDER_ID,
        session_id,
        client_name,
        phone,
        shipping_address,
        state,
        payment_method,
        ordered_at
    FROM {{ ref('orders') }}
),
-- raw join (sessions&orders)
t1 AS
(
    SELECT
        a.client_id,
        a.session_id,
        a.session_started_at,
        a.OS,
        a.IP_ADDRESS,
        b.ORDER_ID,
        b.client_name,
        b.phone,
        b.shipping_address,
        b.state,
        b.payment_method,
        b.ordered_at
    FROM tot_clients_1 a
    INNER JOIN tot_orders_1 b
    ON a.session_id = b.session_id   
),
t2 AS
(
    SELECT 
        client_id,
        session_id,
        session_started_at,
        OS,
        IP_ADDRESS,
        ORDER_ID,
        client_name,
        phone,
        shipping_address,
        state,
        payment_method,
        ordered_at,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ordered_at DESC) AS rn_2,
        COUNT(ORDER_ID) OVER (PARTITION BY client_id) AS total_orders_count
    FROM t1
)
SELECT
    client_id,
    client_name,
    phone,
    shipping_address,
    state,
    payment_method,
    OS AS last_OS,
    IP_ADDRESS AS last_IP_ADDRESS,
    session_id AS last_session_id,
    ORDER_ID AS last_ORDER_ID,
    ordered_at AS last_order_at,
    total_orders_count
FROM t2
WHERE rn_2=1


