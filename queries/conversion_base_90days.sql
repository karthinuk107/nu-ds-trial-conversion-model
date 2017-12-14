SELECT
  *,
  CASE
    WHEN current_status <> 'Active' THEN ABS(CAST(DATE(CAST(subscriptions.contractStartDate1 AS TIMESTAMP)) AS DATE)-CAST(DATE(COALESCE(CancellationRequestedDate,contractEndDate)) AS DATE))
    ELSE integer ('0')
  END AS duration_cancellation
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY cpn, TIMESTAMP) ROW_NUMBER,
    ABS(CAST(DATE(CAST(subscriptions.contractStartDate AS TIMESTAMP)) AS DATE)-CAST(DATE(TIMESTAMP) AS DATE)) AS duration_today,
    CASE
      WHEN subscriptions.subscriptionStatusCode =='Active' THEN 1
      ELSE 0
    END AS status,
    CASE
      WHEN subscriptions.subscriptionStatusCode =='Active' THEN 0
      ELSE 1
    END AS status_cancellation,
    MAX(CASE
        WHEN subscriptions_CancellationRequestedDate_1 IS NULL THEN subscriptions.CancellationRequestedDate
        ELSE NULL
      END ) OVER (PARTITION BY cpn) AS CancellationRequestedDate
  FROM (
    SELECT
      *,
      MIN(CASE
          WHEN subscriptions.subscriptionStatusCode = 'Cancellation Requested' AND DATE(timestamp) <= '2017-04-18' THEN DATE(timestamp)
          ELSE NULL END) OVER (PARTITION BY cpn) AS subscriptions_CancellationRequestedDate_1,
      MAX(CASE
          WHEN DATE(subscriptions.contractEndDate) = '9999-12-31' THEN NULL
          ELSE subscriptions.contractEndDate
        END ) OVER (PARTITION BY cpn) AS contractEndDate,
      FIRST_VALUE(subscriptions.subscriptionStatusCode ) OVER (PARTITION BY cpn ORDER BY TIMESTAMP DESC) AS current_status,
      FIRST_VALUE(gender ) OVER (PARTITION BY cpn ORDER BY TIMESTAMP DESC) AS gender1,
      FIRST_VALUE( country ) OVER (PARTITION BY cpn ORDER BY TIMESTAMP DESC) AS country1,
      FIRST_VALUE( subscriptions.contractStartDate ) OVER (PARTITION BY cpn ORDER BY TIMESTAMP DESC) AS subscriptions.contractStartDate1,
    FROM
      TABLE_DATE_RANGE ([newsuk-datatech-prod:athena.accounts_],TIMESTAMP(DATE_ADD('2016-09-01',-1,"DAY")),TIMESTAMP(DATE_ADD(CURRENT_DATE(),-1,"DAY")))
    WHERE
      TIMESTAMP <= TIMESTAMP(DATE_ADD(subscriptions.contractStartDate,90,"DAY"))
      AND subscriptions.mpc = 'MP370'
      AND subscriptions.subscriptionStatusCode IN ( 'Cancelled',
        'Terminated',
        'Cancellation Requested') ) )
WHERE
  ROW_NUMBER = 1