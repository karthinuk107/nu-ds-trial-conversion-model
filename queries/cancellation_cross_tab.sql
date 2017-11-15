SELECT
  duration_cancellation,
  COUNT(*) AS count
FROM (
  SELECT
    duration_cancellation,
    COUNT(*) AS Count
  FROM
    [newsuk-datatech-dev-1251:tnl_trial_conversion.conversion_90days]
  GROUP BY
    duration_cancellation,
    cpn
  ORDER BY
    duration_cancellation)
WHERE
  duration_cancellation IS NOT NULL
GROUP BY
  duration_cancellation
ORDER BY
  duration_cancellation