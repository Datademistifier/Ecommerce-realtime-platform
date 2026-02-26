-- models/marts/mart_daily_revenue.sql
-- ─────────────────────────────────────────────────────────────
-- Mart: daily revenue summary — the primary business dashboard table.
-- Grain: one row per (order_date, category, shipping_state)
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized='table',
        tags=['marts', 'revenue'],
        post_hook="ANALYZE {{ this }}"
    )
}}

with enriched_orders as (

    select * from {{ ref('int_order_items_enriched') }}
    where is_cancelled = false

),

daily as (

    select
        date(order_placed_at)       as order_date,
        category,
        shipping_state,

        -- Volume
        count(distinct order_id)    as total_orders,
        count(distinct customer_id) as unique_customers,
        sum(quantity)               as total_items_sold,

        -- Revenue
        sum(order_total)            as gross_revenue,
        avg(order_total)            as avg_order_value,
        min(order_total)            as min_order_value,
        max(order_total)            as max_order_value,
        percentile_cont(0.5) within group (
            order by order_total
        )                           as median_order_value,

        -- Value tier breakdown
        count(case when order_value_tier = 'HIGH_VALUE' then 1 end) as high_value_orders,
        count(case when order_value_tier = 'MID_VALUE'  then 1 end) as mid_value_orders,
        count(case when order_value_tier = 'LOW_VALUE'  then 1 end) as low_value_orders,

        -- Attribution
        count(case when attribution_channel = 'google'         then 1 end) as orders_from_google,
        count(case when attribution_channel = 'email_campaign' then 1 end) as orders_from_email,
        count(case when attribution_channel = 'direct'         then 1 end) as orders_from_direct,

        -- Device split
        count(case when device_type = 'mobile'  then 1 end) as mobile_orders,
        count(case when device_type = 'desktop' then 1 end) as desktop_orders,
        count(case when device_type = 'tablet'  then 1 end) as tablet_orders,

        -- Metadata
        current_timestamp as dbt_updated_at

    from enriched_orders
    group by 1, 2, 3

),

with_wow as (

    select
        d.*,

        -- Week-over-week revenue comparison using LAG
        lag(gross_revenue, 7) over (
            partition by category, shipping_state
            order by order_date
        ) as revenue_wow_prior,

        round(
            100.0 * (gross_revenue - lag(gross_revenue, 7) over (
                partition by category, shipping_state
                order by order_date
            )) / nullif(lag(gross_revenue, 7) over (
                partition by category, shipping_state
                order by order_date
            ), 0),
            2
        ) as revenue_wow_pct_change

    from daily d

)

select * from with_wow
