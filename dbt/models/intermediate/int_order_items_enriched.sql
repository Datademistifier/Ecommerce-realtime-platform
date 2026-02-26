-- models/intermediate/int_order_items_enriched.sql
-- ─────────────────────────────────────────────────────────────
-- Intermediate: enrich orders with session context.
-- Joins orders to the session that likely generated them,
-- enabling revenue attribution by referrer/device/channel.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized='table',
        tags=['intermediate', 'orders']
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

sessions as (

    select * from {{ ref('int_user_sessions') }}

),

-- Match each order to the session closest in time
-- for the same customer (within 30 minutes)
order_session_match as (

    select
        o.*,
        s.session_id,
        s.device_type,
        s.browser,
        s.referrer          as attribution_channel,
        s.session_outcome,
        s.engagement_score,
        row_number() over (
            partition by o.order_id
            order by abs(extract(epoch from (
                o.order_placed_at - s.session_start
            )))
        ) as match_rank

    from orders o
    left join sessions s
        on  o.customer_id = s.customer_id
        and s.session_date = date(o.order_placed_at)
        and o.order_placed_at between s.session_start
            and (s.session_end + interval '30 minutes')

),

best_match as (

    select * from order_session_match
    where match_rank = 1

)

select
    order_id,
    customer_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    order_total,
    order_status,
    shipping_state,
    payment_method,
    is_cancelled,
    order_value_tier,
    order_placed_at,
    pipeline_processed_at,

    -- Session attribution (null if no matching session found)
    session_id,
    coalesce(device_type, 'unknown')         as device_type,
    coalesce(browser, 'unknown')             as browser,
    coalesce(attribution_channel, 'direct')  as attribution_channel,
    session_outcome,
    engagement_score

from best_match
