SELECT
  *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cpn, TIMESTAMP) row_number,
    ABS(CAST(DATE(CAST(subscriptions.contractStartDate AS TIMESTAMP)) AS DATE)-CAST(DATE(timestamp) AS DATE)) AS duration_today,
    ABS(CAST(DATE(CAST(subscriptions.contractStartDate AS TIMESTAMP)) AS DATE)-CAST(DATE(CancellationRequestedDate) AS DATE)) AS duration_cancellation,
    CASE
      WHEN subscriptions.subscriptionStatusCode =='Active' THEN 1
      ELSE 0
    END AS status,
        CASE
      WHEN subscriptions.subscriptionStatusCode =='Active' THEN 0
      ELSE 1
    END AS status_cancellation
  FROM (
    SELECT
      *,MAX(subscriptions.CancellationRequestedDate ) over (partition by cpn) as CancellationRequestedDate
    FROM
      TABLE_DATE_RANGE ([newsuk-datatech-prod:athena.accounts_],TIMESTAMP(DATE_ADD('2016-11-05',-1,"DAY")),TIMESTAMP(DATE_ADD(CURRENT_DATE(),-1,"DAY")))
    WHERE
    timestamp <= TIMESTAMP(DATE_ADD(subscriptions.contractStartDate,90,"DAY"))
    AND
      subscriptions.mpc = 'MP370'
      AND subscriptions.subscriptionStatusCode IN ('Active',
        'Cancelled',
        'Terminated',
        'Cancellation Requested')
--       AND ABS(CAST(DATE(CAST(subscriptions.contractStartDate AS TIMESTAMP)) AS DATE)-CAST(DATE(timestamp) AS DATE)) <= 90
    ORDER BY
      cpn,
      timestamp))
WHERE
  row_number = 1