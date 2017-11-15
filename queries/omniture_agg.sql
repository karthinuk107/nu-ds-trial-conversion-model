SELECT
  a.*,
  b.* EXCEPT(cpn)
FROM
  `newsuk-datatech-dev-1251.tnl_trial_conversion.conversion_base_90days` a
LEFT JOIN
  `newsuk-datatech-dev-1251.tnl_trial_conversion.omniture_history` b
ON
  a.cpn = b.cpn
  AND DATE(a.timestamp) = DATE(TIMESTAMP (b.activity_date))