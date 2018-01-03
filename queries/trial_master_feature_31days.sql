SELECT
  a.*,
  b.* EXCEPT(cpn)
FROM
  `newsuk-datatech-dev-1251.tnl_trial_conversion_mid_term.trial_agg_page_views_31days` a
LEFT JOIN
  `newsuk-datatech-dev-1251.tnl_trial_conversion.athena_axiom_features` b
ON
  a.cpn = b.cpn