SELECT
  a.*,
  b.* EXCEPT(cpn)
FROM
  `newsuk-datatech-dev-1251.tnl_trial_conversion_mid_term.trial_agg_45days` a
LEFT JOIN
  `newsuk-datatech-dev-1251.tnl_trial_conversion_mid_term.page_views_trend_45days` b
ON
  a.cpn = b.cpn