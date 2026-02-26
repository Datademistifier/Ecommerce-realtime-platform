-- models/staging/stg_clickstream.sql
-- ─────────────────────────────────────────────────────────────
-- Staging: clean raw clickstream events from PySpark.
-- ─────────────────────────────────────────────────────────────

{{
    config(
        materialized='view',
        tags=['staging', 'clickstream']
    )
}}

with source as (

    select * from {{ source('raw_events', 'clickstream') }}

),

cleaned as (

    select
        event_id,
        session_id,
        customer_id,
        trim(upper(event_type))   as event_type,
        trim(lower(page))         as page,
        trim(lower(device_type))  as device_type,
        trim(lower(browser))      as browser,
        trim(lower(referrer))     as referrer,
        product_id,
        trim(lower(search_term))  as search_term,
        coalesce(results_count, 0) as results_count,
        coalesce(cart_value, 0)    as cart_value,

        event_timestamp::timestamp  as event_occurred_at,
        processed_at::timestamp     as pipeline_processed_at,

        -- Derived: is this a conversion event?
        case
            when event_type = 'CHECKOUT_COMPLETE' then true
            else false
        end as is_conversion,

        -- Derived: funnel stage
        case
            when event_type in ('PAGE_VIEW', 'SEARCH')        then 'AWARENESS'
            when event_type = 'PRODUCT_VIEW'                  then 'CONSIDERATION'
            when event_type in ('ADD_TO_CART','REMOVE_FROM_CART') then 'INTENT'
            when event_type = 'CHECKOUT_START'                then 'PURCHASE_INTENT'
            when event_type = 'CHECKOUT_COMPLETE'             then 'CONVERSION'
            else 'OTHER'
        end as funnel_stage

    from source

    where
        event_id   is not null
        and session_id is not null
        and event_timestamp is not null

)

select * from cleaned
