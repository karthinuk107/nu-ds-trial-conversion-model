SELECT
  athena.cpn_ AS cpn,
  a.scv_id AS scv_id,
  a.times_product_count AS times_product_count,
  a.total_trans AS total_trans,
  a.email_consent_flag AS email_consent_flag,
  a.times_PROM AS times_PROM,
  a.times_Adhoc AS times_Adhoc,
  a.times_Adhoc_L12M_trx_count AS times_Adhoc_L12M_trx_count,
  a.times_Adhoc_tenure AS times_Adhoc_tenure,
  a.times_Adhoc_engaged_recency AS times_Adhoc_engaged_recency,
  a.times_COMP AS times_COMP,
  a.times_COMP_L12M_trx_count AS times_COMP_L12M_trx_count,
  a.times_COMP_tenure AS times_COMP_tenure,
  a.times_COMP_engaged_recency AS times_COMP_engaged_recency,
  a.times_RT AS times_RT,
  a.times_RT_L12M_trx_count AS times_RT_L12M_trx_count,
  a.times_RT_tenure AS times_RT_tenure,
  a.times_RT_engaged_recency AS times_RT_engaged_recency,
  a.times_ST_WINE AS times_ST_WINE,
  a.times_ST_WINE_L12M_trx_count AS times_ST_WINE_L12M_trx_count,
  a.times_ST_WINE_tenure AS times_ST_WINE_tenure,
  a.times_ED AS times_ED,
  a.times_ED_L12M_trx_count AS times_ED_L12M_trx_count,
  a.times_ED_tenure AS times_ED_tenure,
  a.times_ST AS times_ST,
  a.times_ST_L12M_trx_count AS times_ST_L12M_trx_count,
  a.times_ST_tenure AS times_ST_tenure,
  a.times_ST_engaged_recency AS times_ST_engaged_recency,
  a.times_CUR_SERVICES AS times_CUR_SERVICES,
  a.times_CUR_SERVICES_L12M_trx_count AS times_CUR_SERVICES_L12M_trx_count,
  a.times_CUR_SERVICES_tenure AS times_CUR_SERVICES_tenure,
  a.times_SUPBREAK AS times_SUPBREAK,
  a.times_SUPBREAK_L12M_trx_count AS times_SUPBREAK_L12M_trx_count,
  axc.affluence_desc as affluence_desc,
  axc.affluence3_desc as affluence3_desc,
  axc.affordability_rank_desc as affordability_rank_desc,
  axc.affordability_segment_desc as affordability_segment_desc,
  axc.age_band_desc as age_band_desc,
  axc.cultural_pursuit_desc as cultural_pursuit_desc,
  axc.equiv_income_desc as equiv_income_desc,
  axc.income_bracket_desc as income_bracket_desc,
  axc.income_outgoing_desc as income_outgoing_desc,
  axc.kids_desc as kids_desc,
  axc.lifestage_desc as lifestage_desc,
  axc.mail_freq_desc as mail_freq_desc,
  axc.main_daily_news_desc as main_daily_news_desc,
  axc.main_sunday_news_desc as main_sunday_news_desc,
  axc.marital_status_desc as marital_status_desc,
  axc.occupation_desc as occupation_desc,
  axc.onliner_desc as onliner_desc,
  axc.property_type_desc as property_type_desc,
  axc.ownership_status_desc as ownership_status_desc,
  axc.self_employed_desc as self_employed_desc,
  axc.socio_econ_desc as socio_econ_desc,
  axc.stability_rank_desc as stability_rank_desc,
  axc.sun_news_freq_desc as sun_news_freq_desc,
  axc.techno_rank_desc as techno_rank_desc,
  axc.theatre_interest_desc as theatre_interest_desc
FROM (
  SELECT
    cpn AS cpn_
  FROM (TABLE_DATE_RANGE([newsuk-datatech-prod:athena.accounts_],DATE_ADD(CURRENT_TIMESTAMP(),-1,'DAY'),CURRENT_TIMESTAMP()))
  WHERE
    subscriptions.mpc = 'MP370' ) athena
LEFT JOIN (
  SELECT
    *
  FROM (
    SELECT
      scv_id,
      cpn,
      COUNT(product_family ) AS times_product_count,
      SUM(INTEGER(trx_count_L12M) ) AS total_trans,
      MAX(email_consent_flag) AS email_consent_flag,
      SUM(CASE
          WHEN product_family ='Times Promotions' THEN 1
          ELSE 0 END) AS Times_PROM,
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN 1
          ELSE 0 END) AS Times_Adhoc,
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_Adhoc_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN DATEDIFF(CURRENT_DATE(), First_registered_or_subscribed)
          ELSE 0 END) AS Times_Adhoc_tenure,
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN DATEDIFF(CURRENT_DATE(), last_engaged)
          ELSE 0 END) AS Times_Adhoc_engaged_recency,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN 1
          ELSE 0 END) AS Times_COMP,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_COMP_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN DATEDIFF(CURRENT_DATE(), First_registered_or_subscribed)
          ELSE 0 END) AS Times_COMP_tenure,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN DATEDIFF(CURRENT_DATE(), last_engaged)
          ELSE 0 END) AS Times_COMP_engaged_recency,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN 1
          ELSE 0 END) AS Times_RT,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_RT_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN DATEDIFF(CURRENT_DATE(), First_registered_or_subscribed)
          ELSE 0 END) AS Times_RT_tenure,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN DATEDIFF(CURRENT_DATE(), last_engaged)
          ELSE 0 END) AS Times_RT_engaged_recency,
      SUM(CASE
          WHEN product_family ='ST Wine Club' THEN 1
          ELSE 0 END) AS Times_ST_WINE,
      SUM(CASE
          WHEN product_family ='ST Wine Club' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_ST_WINE_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='ST Wine Club' THEN DATEDIFF(CURRENT_DATE(), First_registered_or_subscribed)
          ELSE 0 END) AS Times_ST_WINE_tenure,
      SUM(CASE
          WHEN product_family ='Encounters Dating' THEN 1
          ELSE 0 END) AS Times_ED,
      SUM(CASE
          WHEN product_family ='Encounters Dating' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_ED_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Encounters Dating' THEN DATEDIFF(CURRENT_DATE(), First_registered_or_subscribed)
          ELSE 0 END) AS Times_ED_tenure,
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN 1
          ELSE 0 END) AS Times_ST,
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_ST_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN DATEDIFF(CURRENT_DATE(), First_registered_or_subscribed)
          ELSE 0 END) AS Times_ST_tenure,
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN DATEDIFF(CURRENT_DATE(), last_engaged)
          ELSE 0 END) AS Times_ST_engaged_recency,
      SUM(CASE
          WHEN product_family ='Times Currency Service' THEN 1
          ELSE 0 END) AS Times_CUR_SERVICES,
      SUM(CASE
          WHEN product_family ='Times Currency Service' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_CUR_SERVICES_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Times Currency Service' THEN DATEDIFF(CURRENT_DATE(), First_registered_or_subscribed)
          ELSE 0 END) AS Times_CUR_SERVICES_tenure,
      SUM(CASE
          WHEN product_family ='SuperBreak Travel' THEN 1
          ELSE 0 END) AS Times_SUPBREAK,
      SUM(CASE
          WHEN product_family ='SuperBreak Travel' THEN INTEGER(trx_count_L12M)
ELSE 0 END) AS Times_SUPBREAK_L12M_trx_count,
    FROM
      [newsuk-datatech-dev-1251:adhoc.tnl_scv_products]
    GROUP BY
      scv_id,
      cpn ) )a
ON
  athena.cpn_=a.cpn
LEFT JOIN (
  SELECT
affluence_desc,
affluence3_desc,
affordability_rank_desc,
affordability_segment_desc,
age_band_desc,
cultural_pursuit_desc,
equiv_income_desc,
income_bracket_desc,
income_outgoing_desc,
kids_desc,
lifestage_desc,
mail_freq_desc,
main_daily_news_desc,
main_sunday_news_desc,
marital_status_desc,
occupation_desc,
onliner_desc,
property_type_desc,
ownership_status_desc,
self_employed_desc,
socio_econ_desc,
stability_rank_desc,
sun_news_freq_desc,
techno_rank_desc,
theatre_interest_desc
  FROM
    [newsuk-datatech-prod:acxiom_ilu.acxiom_20170918] ) axc
ON
  athena.cpn_=axc.cpn