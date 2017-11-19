SELECT
  a.*,
  b.* EXCEPT(cpn)
FROM
  `newsuk-datatech-dev-1251.tnl_trial_conversion.trial_digi_agg` a
LEFT JOIN
  `newsuk-datatech-dev-1251.tnl_trial_conversion.athena_axiom_features` b
ON
  a.cpn = b.cpn
