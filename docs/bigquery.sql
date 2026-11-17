-- Batch analytics over delivery attempts in BigQuery (martinet.attempts).
-- Each row's `data` column is one DeliveryAttempt as JSON (protojson names).

-- 1. Delivery success rate per tenant, last 24h
SELECT
  JSON_VALUE(data, '$.tenantId') AS tenant,
  COUNTIF(JSON_VALUE(data, '$.outcome') = 'ATTEMPT_OUTCOME_SUCCESS') / COUNT(*) AS success_rate,
  COUNT(*) AS attempts
FROM martinet.attempts
WHERE publish_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY tenant
ORDER BY attempts DESC;

-- 2. Worst endpoints: failure rate and p99 latency
SELECT
  JSON_VALUE(data, '$.endpoint') AS endpoint,
  COUNT(*) AS attempts,
  COUNTIF(JSON_VALUE(data, '$.outcome') != 'ATTEMPT_OUTCOME_SUCCESS') / COUNT(*) AS failure_rate,
  APPROX_QUANTILES(CAST(JSON_VALUE(data, '$.latencyMs') AS INT64), 100)[OFFSET(99)] AS p99_ms
FROM martinet.attempts
GROUP BY endpoint
HAVING attempts > 50
ORDER BY failure_rate DESC
LIMIT 20;

-- 3. How many attempts does a delivery need? (retry ladder effectiveness)
SELECT attempts_needed, COUNT(*) AS deliveries
FROM (
  SELECT JSON_VALUE(data, '$.deliveryId') AS delivery,
         MAX(CAST(JSON_VALUE(data, '$.attempt') AS INT64)) AS attempts_needed
  FROM martinet.attempts
  GROUP BY delivery
)
GROUP BY attempts_needed
ORDER BY attempts_needed;

-- 4. Failures per minute (compare with what pouls computed in real time)
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP_MILLIS(CAST(JSON_VALUE(data, '$.attemptedAtMs') AS INT64)), MINUTE) AS minute,
  COUNTIF(JSON_VALUE(data, '$.outcome') != 'ATTEMPT_OUTCOME_SUCCESS') AS failures
FROM martinet.attempts
GROUP BY minute
ORDER BY minute DESC
LIMIT 60;
