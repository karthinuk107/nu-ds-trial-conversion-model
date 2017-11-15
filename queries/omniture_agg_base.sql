select* from (SELECT
  cpn,
  country,
  gender,
  CASE
    WHEN dob <> '' THEN YEAR(timestamp)-INTEGER(LEFT(dob,4))
  END AS age,
  subscriptions_contractStartDate,
  subscriptions_promoCode,
  subscriptions_paymentMethodSelection,
  subscriptions_CancellationRequestedDate,
  NVL(duration_cancellation ,0) as duration_cancellation ,
  status,
  SUM(active_days) AS active_days,
  SUM(recency) AS recency,
  SUM(events_weekdays) AS events_weekdays,
  SUM(events_weekends) AS events_weekends,
  SUM(active_weekdays) AS active_weekdays,
  SUM(active_weekends) AS active_weekends,
  SUM(active_weeks) AS active_weeks,
  SUM(visits_weekdays) AS visits_weekdays,
  SUM(visits_weekends) AS visits_weekends,
  SUM(views_weekdays) AS views_weekdays,
  SUM(views_weekends) AS views_weekends,
  SUM(morning_views) AS morning_views,
  SUM(afternoon_views) AS afternoon_views,
  SUM(evening_views) AS evening_views,
  SUM(late_night_views) AS late_night_views,
  SUM(total_page_views) AS total_page_views,
FROM
  [newsuk-datatech-dev-1251:tnl_trial_conversion.omniture_history]
WHERE
  duration_today <= 45
GROUP BY
  cpn,
  country,
  gender,
  age,
  subscriptions_contractStartDate,
  subscriptions_promoCode,
  subscriptions_paymentMethodSelection,
  subscriptions_CancellationRequestedDate,
  status,
  primary_device,
   duration_cancellation ,
  secondary_device)
  where cpn='AAAA013516076'