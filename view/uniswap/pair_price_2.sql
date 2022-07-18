WITH
    sub_token0 AS (SELECT
        pb.block_number,
        pb.token0 as token,
        LAST_VALUE(p.price)
            OVER (PARTITION BY pb.block_number, pb.token0 ORDER BY pb.block_number ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) * pb.price1 AS price
        FROM `xed-project-237404.footprint_test.uni_price_base` pb
        left join `xed-project-237404.footprint_test.uni_price_1` p
        ON pb.token1 = p.token and pb.block_number <= p.block_number
        where
        p.price is not null),
    sub_token1 AS (SELECT
        pb.block_number,
        pb.token1 as token,
        LAST_VALUE(p.price)
            OVER (PARTITION BY pb.block_number, pb.token1 ORDER BY pb.block_number ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) * pb.price0 AS price
        FROM `xed-project-237404.footprint_test.uni_price_base` pb
        left join `xed-project-237404.footprint_test.uni_price_1` p
        ON pb.token0 = p.token and pb.block_number <= p.block_number
        where
        p.price is not null)

SELECT
block_number,token,
avg(price) as price
FROM sub_token0 GROUP BY block_number,token
UNION ALL
SELECT
block_number,token,
avg(price) as price
FROM sub_token1 GROUP BY block_number,token