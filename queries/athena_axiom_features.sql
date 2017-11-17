SELECT
  athena.cpn_ AS cpn,
  a.scv_id AS scv_id,
  a.times_product_count AS times_product_count,
  a.total_trans AS total_trans,
  a.email_consent_flag AS email_consent_flag,
  a.times_RA AS times_RA,
  a.times_RA_L12M_trx_count AS times_RA_L12M_trx_count,
  a.times_RA_tenure AS times_RA_tenure,
  a.times_SUB AS times_SUB,
  a.times_SUB_L12M_trx_count AS times_SUB_L12M_trx_count,
  a.times_SUB_tenure AS times_SUB_tenure,
  a.times_SUB_engaged_recency AS times_SUB_engaged_recency,
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
  axc.affluence_desc AS affluence_desc,
  axc.affordability_rank_desc AS affordability_rank_desc,
  axc.affordability_segment_desc AS affordability_segment_desc,
  axc.cultural_pursuit_desc AS cultural_pursuit_desc,
  axc.equiv_income_desc AS equiv_income_desc,
  axc.income_bracket_desc AS income_bracket_desc,
  axc.income_outgoing_desc AS income_outgoing_desc,
  axc.lifestage_desc AS lifestage_desc,
  axc.mail_freq_desc AS mail_freq_desc,
  axc.marital_status_desc AS marital_status_desc,
  axc.occupation_desc AS occupation_desc,
  axc.onliner_desc AS onliner_desc,
  axc.property_type_desc AS property_type_desc,
  axc.socio_econ_desc AS socio_econ_desc,
FROM (
  SELECT
    cpn AS cpn_
  FROM
    [newsuk-datatech-prod:athena.accounts_20171113]
  WHERE
    subscriptions.mpc = 'MP370' ) athena LEFT JOIN (
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
          WHEN product_family ='Times - Registered Access' THEN 1
          ELSE 0 END) AS Times_RA,
      MAX(CASE
          WHEN product_family ='Times - Registered Access' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_RA_L12M_trx_count,
      MAX(CASE
          WHEN product_family ='Times - Registered Access' THEN DATEDIFF(CURRENT_DATE(),DATE(First_registered_or_subscribed))
          ELSE 0 END) AS Times_RA_tenure,
      --MAX(CASE WHEN product_family ='Times - Registered Access' THEN DATEDIFF(current_date(),last_engaged) ELSE 0 END) AS Times_RA_engaged_recency,
      -- as no last engaged date available for Times RA
      SUM(CASE
          WHEN product_family ='Times Subscription' THEN 1
          ELSE 0 END) AS Times_SUB,
      SUM(CASE
          WHEN product_family ='Times Subscription' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_SUB_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Times Subscription' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_SUB_tenure,
      SUM(CASE
          WHEN product_family ='Times Subscription' THEN DATEDIFF(CURRENT_DATE(),last_engaged)
          ELSE 0 END) AS Times_SUB_engaged_recency,
      SUM(CASE
          WHEN product_family ='Times Promotions' THEN 1
          ELSE 0 END) AS Times_PROM,
      --SUM(CASE WHEN product_family ='Times Promotions' THEN integer(trx_count_L12M) ELSE 0 END) AS Times_PROM_L12M_trx_count,
      --SUM(CASE WHEN product_family ='Times Promotions' THEN DATEDIFF(current_date(),First_registered_or_subscribed) ELSE 0 END) AS Times_PROM_tenure,
      --SUM(CASE WHEN product_family ='Times Promotions' THEN DATEDIFF(current_date(),last_engaged) ELSE 0 END) AS Times_PROM_engaged_recency,
      -- as TIMES Promo has been discontinued
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN 1
          ELSE 0 END) AS Times_Adhoc,
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_Adhoc_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_Adhoc_tenure,
      SUM(CASE
          WHEN product_family ='Times Ad hoc Prospects' THEN DATEDIFF(CURRENT_DATE(),last_engaged)
          ELSE 0 END) AS Times_Adhoc_engaged_recency,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN 1
          ELSE 0 END) AS Times_COMP,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_COMP_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_COMP_tenure,
      SUM(CASE
          WHEN product_family ='Times Competitions' THEN DATEDIFF(CURRENT_DATE(),last_engaged)
          ELSE 0 END) AS Times_COMP_engaged_recency,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN 1
          ELSE 0 END) AS Times_RT,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_RT_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_RT_tenure,
      SUM(CASE
          WHEN product_family ='Riviera Travel' THEN DATEDIFF(CURRENT_DATE(),last_engaged)
          ELSE 0 END) AS Times_RT_engaged_recency,
      SUM(CASE
          WHEN product_family ='ST Wine Club' THEN 1
          ELSE 0 END) AS Times_ST_WINE,
      SUM(CASE
          WHEN product_family ='ST Wine Club' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_ST_WINE_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='ST Wine Club' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_ST_WINE_tenure,
      --SUM(CASE WHEN product_family ='ST Wine Club' THEN DATEDIFF(current_date(),last_engaged) ELSE 0 END) AS Times_ST_WINE_engaged_recency,
      --as no last engaged date available for Times Wine club
      SUM(CASE
          WHEN product_family ='Encounters Dating' THEN 1
          ELSE 0 END) AS Times_ED,
      SUM(CASE
          WHEN product_family ='Encounters Dating' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_ED_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Encounters Dating' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_ED_tenure,
      --SUM(CASE WHEN product_family ='Encounters Dating' THEN DATEDIFF(current_date(),last_engaged) ELSE 0 END) AS Times_ED_engaged_recency,
      -- as no last engaged date available for Times ED
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN 1
          ELSE 0 END) AS Times_ST,
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_ST_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_ST_tenure,
      SUM(CASE
          WHEN product_family ='ST Travel Magazine' THEN DATEDIFF(CURRENT_DATE(),last_engaged)
          ELSE 0 END) AS Times_ST_engaged_recency,
      SUM(CASE
          WHEN product_family ='Times Currency Service' THEN 1
          ELSE 0 END) AS Times_CUR_SERVICES,
      SUM(CASE
          WHEN product_family ='Times Currency Service' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_CUR_SERVICES_L12M_trx_count,
      SUM(CASE
          WHEN product_family ='Times Currency Service' THEN DATEDIFF(CURRENT_DATE(),First_registered_or_subscribed)
          ELSE 0 END) AS Times_CUR_SERVICES_tenure,
      --SUM(CASE WHEN product_family ='Times Currency Service' THEN DATEDIFF(current_date(),last_engaged) ELSE 0 END) AS Times_CUR_SERVICES_engaged_recency,
      -- as no last engaged date available for Times Currency
      SUM(CASE
          WHEN product_family ='SuperBreak Travel' THEN 1
          ELSE 0 END) AS Times_SUPBREAK,
      SUM(CASE
          WHEN product_family ='SuperBreak Travel' THEN INTEGER(trx_count_L12M)
          ELSE 0 END) AS Times_SUPBREAK_L12M_trx_count,
      --SUM(CASE WHEN product_family ='SuperBreak Travel' THEN DATEDIFF(current_date(),First_registered_or_subscribed) ELSE 0 END) AS Times_SUPBREAK_tenure,
      --SUM(CASE WHEN product_family ='SuperBreak Travel' THEN DATEDIFF(current_date(),last_engaged) ELSE 0 END) AS Times_SUPBREAK_engaged_recency
      --  as no last engaged date,first registered data  available for Times SUP Break
    FROM
      [newsuk-datatech-dev-1251:adhoc.tnl_scv_products]
    GROUP BY
      scv_id,
      cpn ) )a
ON
  athena.cpn_=a.cpn
LEFT JOIN (
  SELECT
    cpn,
    affluence_desc,
    affordability_rank_desc,
    affordability_segment_desc,
    cultural_pursuit_desc,
    equiv_income_desc,
    income_bracket_desc,
    income_outgoing_desc,
    lifestage_desc,
    mail_freq_desc,
    marital_status_desc,
    occupation_desc,
    onliner_desc,
    property_type_desc,
    socio_econ_desc,
  FROM
    [newsuk-datatech-prod:acxiom_ilu.acxiom_20170918] ) axc
ON
  athena.cpn_=axc.cpn