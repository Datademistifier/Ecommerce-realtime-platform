-- models/marts/mart_product_performance.sql
-- ─────────────────────────────────────────────────────────────
-- Mart: product-level performance metrics.
-- Grain: one row per (product_id, order_date)
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized='table',
        tags=['marts', 'products']
    )
}}

with orders as (

    select * from {{ ref('int_order_items_enriched') }}
    where is_cancelled = false

),

product_daily as (

    select
        product_id,
        product_name,
        category,
        date(order_placed_at)       as order_date,

        count(distinct order_id)    as times_ordered,
        count(distinct customer_id) as unique_buyers,
        sum(quantity)               as units_sold,
        sum(order_total)            as total_revenue,
        avg(unit_price)             as avg_selling_price,

        -- Attribution: which channel drives product sales?
        count(case when attribution_channel = 'google'         then 1 end) as google_orders,
        count(case when attribution_channel = 'email_campaign' then 1 end) as email_orders,

        current_timestamp           as dbt_updated_at

    from orders
    group by 1, 2, 3, 4

),

with_ranking as (

    select
        *,
        -- Rank within category by revenue on each day
        rank() over (
            partition by category, order_date
            order by total_revenue desc
        ) as revenue_rank_in_category,

        -- Running total revenue per product
        sum(total_revenue) over (
            partition by product_id
            order by order_date
            rows between unbounded preceding and current row
        ) as cumulative_revenue,

        -- 7-day rolling average units sold
        avg(units_sold) over (
            partition by product_id
            order by order_date
            rows between 6 preceding and current row
        ) as rolling_7d_avg_units

    from product_daily

)

select * from with_ranking
