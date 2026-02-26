-- models/intermediate/int_user_sessions.sql
-- ─────────────────────────────────────────────────────────────
-- Intermediate: reconstruct user sessions from clickstream.
-- Each row = one session with all funnel stage flags.
-- Used by mart_user_behavior.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized='table',
        tags=['intermediate', 'sessions']
    )
}}

with clicks as (

    select * from {{ ref('stg_clickstream') }}

),

sessions as (

    select
        session_id,
        customer_id,
        device_type,
        browser,
        referrer,

        -- Session timing
        min(event_occurred_at)  as session_start,
        max(event_occurred_at)  as session_end,
        extract(epoch from (max(event_occurred_at) - min(event_occurred_at)))
                                as session_duration_seconds,

        -- Event counts
        count(*)                                    as total_events,
        count(distinct page)                        as unique_pages_viewed,
        count(case when event_type = 'PAGE_VIEW'       then 1 end) as page_views,
        count(case when event_type = 'PRODUCT_VIEW'    then 1 end) as product_views,
        count(case when event_type = 'SEARCH'          then 1 end) as searches,
        count(case when event_type = 'ADD_TO_CART'     then 1 end) as add_to_cart_events,
        count(case when event_type = 'CHECKOUT_START'  then 1 end) as checkout_starts,
        count(case when event_type = 'CHECKOUT_COMPLETE' then 1 end) as conversions,

        -- Products browsed
        count(distinct product_id)                  as unique_products_viewed,
        max(cart_value)                             as max_cart_value,

        -- Funnel reached
        bool_or(event_type = 'PRODUCT_VIEW')        as reached_consideration,
        bool_or(event_type = 'ADD_TO_CART')         as reached_intent,
        bool_or(event_type = 'CHECKOUT_START')      as reached_purchase_intent,
        bool_or(event_type = 'CHECKOUT_COMPLETE')   as converted,

        -- Session date
        date(min(event_occurred_at))                as session_date

    from clicks
    group by 1, 2, 3, 4, 5

),

enriched as (

    select
        *,
        -- Session quality classification
        case
            when converted = true                      then 'CONVERTED'
            when reached_purchase_intent = true        then 'NEAR_CONVERSION'
            when reached_intent = true                 then 'HIGH_INTENT'
            when reached_consideration = true          then 'BROWSING'
            else 'BOUNCE'
        end as session_outcome,

        -- Engagement score (simple weighted formula)
        (
            page_views * 1
            + product_views * 3
            + searches * 2
            + add_to_cart_events * 5
            + checkout_starts * 8
            + conversions * 10
        ) as engagement_score

    from sessions

)

select * from enriched
