-- models/staging/stg_orders.sql
-- ─────────────────────────────────────────────────────────────
-- Staging layer: clean and standardise raw orders.
-- No joins, no business logic — just shape the data.
-- Source: raw_events.orders (written by PySpark Structured Streaming)
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized='view',
        tags=['staging', 'orders']
    )
}}

with source as (

    select * from {{ source('raw_events', 'orders') }}

),

cleaned as (

    select
        -- Keys
        order_id,
        customer_id,
        product_id,

        -- Normalise strings
        trim(upper(product_name))   as product_name,
        trim(upper(category))       as category,
        trim(upper(order_status))   as order_status,
        trim(upper(shipping_state)) as shipping_state,
        trim(lower(payment_method)) as payment_method,

        -- Numerics — guard against nulls
        coalesce(quantity, 0)    as quantity,
        coalesce(unit_price, 0)  as unit_price,
        coalesce(order_total, 0) as order_total,

        -- Timestamps
        event_timestamp::timestamp  as order_placed_at,
        processed_at::timestamp     as pipeline_processed_at,

        -- Derived flags
        case
            when order_status = 'CANCELLED' then true
            else false
        end as is_cancelled,

        case
            when order_total >= 100 then 'HIGH_VALUE'
            when order_total >= 50  then 'MID_VALUE'
            else 'LOW_VALUE'
        end as order_value_tier

    from source

    where
        -- Basic quality gates at staging layer
        order_id    is not null
        and customer_id is not null
        and order_total > 0
        and quantity    > 0

)

select * from cleaned
