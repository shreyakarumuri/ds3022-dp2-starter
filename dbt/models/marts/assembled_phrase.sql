SELECT
    count(*)                                AS fragment_count,
    string_agg(word, ' ' order by order_no) AS phrase
FROM {{ ref('stg_fragments') }}
