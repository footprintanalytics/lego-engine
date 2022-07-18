WITH daily AS (
    SELECT
    -- day,
    day, chain, name, protocol,
    CONCAT(chain, "_",name, "_",protocol) AS id,
    -- chain AS id,
    SUM(volume) AS volume
    FROM `xed-project-237404.footprint_etl.dex_pair_daily_stats`
    WHERE day > '2021-01-01'
    GROUP BY day, chain,  name, protocol
    -- GROUP BY day, chain
),
avgfor3d AS (
    SELECT
    day,id,volume ,
    (
        SELECT
        AVG(volume)
        FROM daily d2
        WHERE d2.day < d1.day AND d2.day > DATE_SUB(d1.day, INTERVAL 3 DAY)
        AND d2.id = d1.id
    ) AS volume_past_3d
    FROM daily d1
)
SELECT * FROM avgfor3d d
WHERE d.volume > 100000 AND d.volume_past_3d > 100000
AND (d.volume > d.volume_past_3d * 50 OR d.volume  < d.volume_past_3d * (1/50))
ORDER BY d.day