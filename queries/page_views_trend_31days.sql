SELECT
  cpn,
  CORRELATION * STDDEV_OF_Y / STDDEV_OF_X AS Page_Views_Trend
FROM (
  SELECT
    cpn,
    COUNT(*) AS N,
    SUM(X) AS SUM_OF_X,
    SUM(Y) AS SUM_OF_Y,
    STDDEV_POP(X) AS STDDEV_OF_X,
    STDDEV_POP(Y) AS STDDEV_OF_Y,
    CORR(X,Y) AS CORRELATION
  FROM (
    SELECT
      cpn,
      DATEDIFF(TIMESTAMP(activity_date),TIMESTAMP(subscriptions_contractStartDate)) AS X,
      total_page_views AS Y
  FROM
    [newsuk-datatech-dev-1251:tnl_trial_conversion.omniture_agg]
  WHERE
    TIMESTAMP(activity_date)  <= TIMESTAMP(DATE_ADD(subscriptions_contractStartDate1,31,"DAY"))
    and TIMESTAMP(activity_date) > timestamp(subscriptions_contractStartDate1)
    ORDER BY
      cpn,
      X)
  WHERE
    X IS NOT NULL
    AND Y IS NOT NULL
  GROUP BY
    cpn)