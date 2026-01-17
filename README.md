# YDB Key Prefix Demo and Test

## Building

Maven, Java SDK 21

```bash
mvn clean package -DskipTests=true
```

## Queries

```sql
SELECT dt, COUNT(*) AS cnt FROM
(SELECT CAST(tv AS Date) AS dt FROM `key_prefix_demo/main` VIEW ix_coll)
GROUP BY dt
ORDER BY cnt ASC
LIMIT 10;
```
