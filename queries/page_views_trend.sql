SELECT
  month,
  balance,
  future_balance
FROM (
  SELECT
    STRFTIME_UTC_USEC(end_time, '%Y%m') AS month,
    SUM(cost) + SUM(credits.amount) AS balance,
    NULL AS future_balance
  FROM
    [newsuk-datatech-prod:gcp_billing.gcp_billing_export_003160_C5A0B8_305DC7]
  WHERE
    STRFTIME_UTC_USEC(end_time, '%Y%m') >= STRFTIME_UTC_USEC(DATE_ADD(CURRENT_TIMESTAMP(), -3, "MONTH"), '%Y%m')
    AND STRFTIME_UTC_USEC(end_time, '%Y%m') != STRFTIME_UTC_USEC(CURRENT_TIMESTAMP(), '%Y%m')
  GROUP BY
    month
  ORDER BY
    month),
  (
  SELECT
    STRFTIME_UTC_USEC(CURRENT_TIMESTAMP(), '%Y%m') AS month,
    NULL AS balance,
    SLOPE * (YEAR(CURRENT_TIMESTAMP()) * 12 + MONTH(CURRENT_TIMESTAMP())) + INTERCEPT AS future_balance
  FROM (
    SELECT
      SLOPE,
      (SUM_OF_Y - SLOPE * SUM_OF_X) / N AS INTERCEPT,
      CORRELATION
    FROM (
      SELECT
        N,
        SUM_OF_X,
        SUM_OF_Y,
        CORRELATION * STDDEV_OF_Y / STDDEV_OF_X AS SLOPE,
        CORRELATION
      FROM (
        SELECT
          COUNT(*) AS N,
          SUM(X) AS SUM_OF_X,
          SUM(Y) AS SUM_OF_Y,
          STDDEV_POP(X) AS STDDEV_OF_X,
          STDDEV_POP(Y) AS STDDEV_OF_Y,
          CORR(X,Y) AS CORRELATION
        FROM (
          SELECT
            YEAR(end_time) * 12 + MONTH(end_time) AS X,
            SUM(cost) + SUM(credits.amount) AS Y
          FROM
            [newsuk-datatech-prod:gcp_billing.gcp_billing_export_003160_C5A0B8_305DC7]
          WHERE
            STRFTIME_UTC_USEC(end_time, '%Y%m') >= STRFTIME_UTC_USEC(DATE_ADD(CURRENT_TIMESTAMP(), -3, "MONTH"), '%Y%m')
          GROUP BY
            X
          ORDER BY
            X )
        WHERE
          X IS NOT NULL
          AND Y IS NOT NULL)) ) ),
  (
  SELECT
    STRFTIME_UTC_USEC(DATE_ADD(CURRENT_TIMESTAMP(), 1, "MONTH"), '%Y%m') AS month,
    NULL AS balance,
    SLOPE * (YEAR(DATE_ADD(CURRENT_TIMESTAMP(), 1, "MONTH")) * 12 + MONTH(DATE_ADD(CURRENT_TIMESTAMP(), 1, "MONTH"))) + INTERCEPT AS future_balance,
  FROM (
    SELECT
      SLOPE,
      (SUM_OF_Y - SLOPE * SUM_OF_X) / N AS INTERCEPT,
      CORRELATION
    FROM (
      SELECT
        N,
        SUM_OF_X,
        SUM_OF_Y,
        CORRELATION * STDDEV_OF_Y / STDDEV_OF_X AS SLOPE,
        CORRELATION
      FROM (
        SELECT
          COUNT(*) AS N,
          SUM(X) AS SUM_OF_X,
          SUM(Y) AS SUM_OF_Y,
          STDDEV_POP(X) AS STDDEV_OF_X,
          STDDEV_POP(Y) AS STDDEV_OF_Y,
          CORR(X,Y) AS CORRELATION
        FROM (
          SELECT
            YEAR(end_time) * 12 + MONTH(end_time) AS X,
            SUM(cost) + SUM(credits.amount) AS Y
          FROM
            [newsuk-datatech-prod:gcp_billing.gcp_billing_export_003160_C5A0B8_305DC7]
          WHERE
            STRFTIME_UTC_USEC(end_time, '%Y%m') >= STRFTIME_UTC_USEC(DATE_ADD(CURRENT_TIMESTAMP(), -3, "MONTH"), '%Y%m')
          GROUP BY
            X
          ORDER BY
            X )
        WHERE
          X IS NOT NULL
          AND Y IS NOT NULL)) ) ),
ORDER BY
  month;