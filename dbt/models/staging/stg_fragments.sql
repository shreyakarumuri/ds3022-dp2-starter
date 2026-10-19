SELECT DISTINCT
    CAST(order_no AS INT) as order_no,
    TRIM(word) AS word
FROM {{ source('raw', 'fragments') }}