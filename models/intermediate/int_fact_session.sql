-- page views
WITH page_view_stats as (
    select 
        session_id,
        count(page_view_id) as cnt_page_views,
        min(viewed_at) as first_view_time,
        max(viewed_at) as last_view_time
    from {{ ref('page_views') }}
    group by 1
),

-- funnel
funnel_data AS
(
    SELECT
        session_id,
        MAX(CASE WHEN page_name = 'landing_page' THEN 1 ELSE 0 END) AS has_viewed_landing_page,
        MAX(CASE WHEN page_name = 'shop_plants' THEN 1 ELSE 0 END) AS has_viewed_shop_plants,
        MAX(CASE WHEN page_name = 'cart' THEN 1 ELSE 0 END) AS has_viewed_cart,
        MAX(CASE WHEN page_name = 'plant_care' THEN 1 ELSE 0 END) AS has_viewed_plant_care,
        MAX(CASE WHEN page_name = 'faq' THEN 1 ELSE 0 END) AS has_viewed_faq
    FROM
    (
        select 
                session_id,
                PAGE_NAME,
                count(page_view_id) as cnt_page_views 
        from {{ ref('page_views') }}
        group by session_id ,PAGE_NAME
    ) t
    GROUP BY session_id
),

-- items viewed
item_view_stats as (
    select 
        session_id,
        count(item_view_id) as cnt_item_views,
        sum(add_to_cart_quantity) as total_add_to_cart,
        sum(remove_from_cart_quantity) AS total_remove_from_cart
    from {{ ref('views') }}
    group by 1
),

-- CVR
order_conversion as (
    select 
        session_id,
        count(order_id) as cnt_orders
    from {{ ref('orders') }}
    group by 1
)

select
    s.session_id,
    s.client_id,
    s.session_started_at,
    p.first_view_time,
    p.last_view_time,
    f.has_viewed_landing_page,
    f.has_viewed_shop_plants,
    f.has_viewed_cart,
    f.has_viewed_plant_care,
    f.has_viewed_faq,
    coalesce(p.cnt_page_views, 0) as page_view_count,
    coalesce(i.cnt_item_views, 0) as item_view_count,
    coalesce(i.total_add_to_cart, 0) as total_add_to_cart_quantity,
    coalesce(i.total_remove_from_cart, 0) AS total_remove_from_cart,
    case when o.cnt_orders > 0 then true else false end as is_order_converted
from {{ ref('sessions') }} s
left join page_view_stats p on s.session_id = p.session_id
left join funnel_data f on s.session_id = f.session_id
left join item_view_stats i on s.session_id = i.session_id
left join order_conversion o on s.session_id = o.session_id










