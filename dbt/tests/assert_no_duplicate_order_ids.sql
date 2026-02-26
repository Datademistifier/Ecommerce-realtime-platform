-- tests/assert_no_duplicate_order_ids.sql
-- Custom test: order_id must be unique in the mart layer.
-- Any duplicates indicate a fan-out join bug in intermediate models.

select
    order_id,
    count(*) as occurrences
from {{ ref('int_order_items_enriched') }}
group by order_id
having count(*) > 1
