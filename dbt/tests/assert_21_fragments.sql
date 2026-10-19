SELECT
    COUNT(*) AS fragment_count,
    MIN(order_no) AS min_order_no,
    MAX(order_no) AS max_order_no
FROM {{ ref('stg_fragments') }}
HAVING
    COUNT(*) <> {{ var('expected_fragments', 21) }}
    OR MAX(order_no) - MIN(order_no) + 1 <> {{ var('expected_fragments') }}
