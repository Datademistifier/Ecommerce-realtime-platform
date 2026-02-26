-- models/marts/mart_user_behavior.sql
-- ─────────────────────────────────────────────────────────────
-- Mart: user-level behavioral and revenue summary.
-- Grain: one row per customer_id (lifetime aggregate)
-- Powers customer segmentation and lifetime value analysis.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized='table',
        tags=['marts', 'users']
    )
}}

with orders as (

    select * from {{ ref('int_order_items_enriched') }}
    where is_cancelled = false

),

sessions as (

    select * from {{ ref('int_user_sessions') }}

),

order_summary as (

    select
        customer_id,
        count(distinct order_id)        as total_orders,
        sum(order_total)                as lifetime_value,
        avg(order_total)                as avg_order_value,
        min(order_placed_at)            as first_order_at,
        max(order_placed_at)            as last_order_at,
        count(distinct category)        as categories_purchased,
        count(distinct product_id)      as unique_products_bought,
        -- Most common payment method
        mode() within group (
            order by payment_method
        )                               as preferred_payment_method,
        -- Most common channel
        mode() within group (
            order by attribution_channel
        )                               as primary_attribution_channel

    from orders
    group by 1

),

session_summary as (

    select
        customer_id,
        count(distinct session_id)      as total_sessions,
        avg(session_duration_seconds)   as avg_session_duration_secs,
        avg(engagement_score)           as avg_engagement_score,
        sum(case when converted then 1 else 0 end) as converting_sessions,
        mode() within group (
            order by device_type
        )                               as primary_device,
        mode() within group (
            order by referrer
        )                               as primary_referrer

    from sessions
    where customer_id is not null
    group by 1

),

combined as (

    select
        o.customer_id,
        o.total_orders,
        o.lifetime_value,
        o.avg_order_value,
        o.first_order_at,
        o.last_order_at,
        o.categories_purchased,
        o.unique_products_bought,
        o.preferred_payment_method,
        o.primary_attribution_channel,

        coalesce(s.total_sessions, 0)           as total_sessions,
        coalesce(s.avg_session_duration_secs, 0) as avg_session_duration_secs,
        coalesce(s.avg_engagement_score, 0)      as avg_engagement_score,
        coalesce(s.converting_sessions, 0)       as converting_sessions,
        coalesce(s.primary_device, 'unknown')    as primary_device,
        coalesce(s.primary_referrer, 'unknown')  as primary_referrer,

        -- Days between first and last order
        extract(day from (o.last_order_at - o.first_order_at))
                                                as customer_tenure_days,

        -- Purchase frequency: orders per 30 days of tenure
        case
            when extract(day from (o.last_order_at - o.first_order_at)) > 0
            then round(
                o.total_orders::numeric
                / (extract(day from (o.last_order_at - o.first_order_at)) / 30.0),
                2
            )
            else o.total_orders
        end                                     as orders_per_month,

        -- Customer segment
        case
            when o.lifetime_value >= 500            then 'VIP'
            when o.lifetime_value >= 200
                and o.total_orders >= 3             then 'LOYAL'
            when o.total_orders >= 2                then 'RETURNING'
            else 'NEW'
        end                                     as customer_segment,

        current_timestamp                       as dbt_updated_at

    from order_summary o
    left join session_summary s using (customer_id)

)

select * from combined
