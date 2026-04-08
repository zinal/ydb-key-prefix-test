SELECT MAX(ballast1) AS mb, COUNT(*) AS cnt FROM `key_prefix_demo/main`
WHERE tv BETWEEN Timestamp("2021-10-20T00:00:00Z") AND Timestamp("2021-10-22T00:00:00Z")
UNION ALL
SELECT MAX(ballast2) AS mb, COUNT(*) AS cnt FROM `key_prefix_demo/sub`
WHERE tv BETWEEN Timestamp("2021-10-20T00:00:00Z") AND Timestamp("2021-10-22T00:00:00Z");