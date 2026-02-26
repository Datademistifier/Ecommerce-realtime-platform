-- tests/assert_order_total_positive.sql
-- Custom singular test: every order in the mart must have positive revenue.
-- Fails if any row returns (dbt treats any returned row as a failure).

select
    order_id,
    customer_id,
    order_total
from {{ ref('stg_orders') }}
where order_total <= 0
