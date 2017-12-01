SELECT
  cpn,
  country1,
  gender1,
  MAX(CASE
      WHEN dob <> '' THEN YEAR(CURRENT_TIMESTAMP()) -INTEGER(LEFT(dob, 4)) END) AS age,  DATE(subscriptions_contractStartDate1) AS subscriptions_contractStartDate,  DATE(CancellationRequestedDate) AS CancellationRequestedDate,  NVL(duration_cancellation, 0) AS duration_cancellation,  MAX(primary_device) primary_device,  MAX(secondary_device) secondary_device,  EXACT_COUNT_DISTINCT(activity_date ) AS active_days,  MAX(CASE
      WHEN duration_today = 45 THEN recency END) AS recency,
  SUM(NVL(events_weekdays ,0)) AS  events_weekdays,
  SUM(NVL(events_weekends ,0)) AS  events_weekends,
  SUM(NVL(active_weekdays ,0)) AS  active_weekdays,
  SUM(NVL(active_weekends ,0)) AS  active_weekends,
  SUM(NVL(active_weeks ,0)) AS  active_weeks,
  SUM(NVL(visits_weekdays ,0)) AS  visits_weekdays,
  SUM(NVL(visits_weekends ,0)) AS  visits_weekends,
  SUM(NVL(views_weekdays ,0)) AS  views_weekdays,
  SUM(NVL(views_weekends ,0)) AS  views_weekends,
  SUM(NVL(morning_views ,0)) AS  morning_views,
  SUM(NVL(afternoon_views ,0)) AS  afternoon_views,
  SUM(NVL(evening_views ,0)) AS  evening_views,
  SUM(NVL(late_night_views ,0)) AS  late_night_views,
  SUM(NVL(total_page_views ,0)) AS  total_page_views,
  SUM(NVL(interactions,0)) AS  interactions,
  SUM(NVL(news_cnt,0)) AS  news_cnt,
  SUM(NVL(puzzles_cnt,0)) AS  puzzles_cnt,
  SUM(NVL(world_cnt,0)) AS  world_cnt,
  SUM(NVL(sport_cnt,0)) AS  sport_cnt,
  SUM(NVL(business_cnt,0)) AS  business_cnt,
  SUM(NVL(comment_cnt,0)) AS  comment_cnt,
  SUM(NVL(Front_cnt,0)) AS  Front_cnt,
  SUM(NVL(times2_cnt,0)) AS  times2_cnt,
  SUM(NVL(register_cnt,0)) AS  register_cnt,
  SUM(NVL(past6d_cnt,0)) AS  past6d_cnt,
  SUM(NVL(scotland_cnt,0)) AS  scotland_cnt,
  SUM(NVL(TAS_cnt,0)) AS  TAS_cnt,
  SUM(NVL(news_reviews_cnt,0)) AS  news_reviews_cnt,
  SUM(NVL(life_cnt,0)) AS  life_cnt,
  SUM(NVL(mindgames_cnt,0)) AS  mindgames_cnt,
  SUM(NVL(opinion_cnt,0)) AS  opinion_cnt,
  SUM(NVL(arts_cnt,0)) AS  arts_cnt,
  SUM(NVL(fashion_cnt,0)) AS  fashion_cnt,
  SUM(NVL(driving_cnt,0)) AS  driving_cnt,
  SUM(NVL(books_cnt,0)) AS  books_cnt,
  SUM(NVL(home_cnt,0)) AS  home_cnt,
  SUM(NVL(magazine_cnt,0)) AS  magazine_cnt,
  SUM(NVL(money_cnt,0)) AS  money_cnt,
  SUM(NVL(football_cnt,0)) AS  football_cnt,
  SUM(NVL(news_review_cnt,0)) AS  news_review_cnt,
  SUM(NVL(saturday_review_cnt,0)) AS  saturday_review_cnt,
  SUM(NVL(style_cnt,0)) AS  style_cnt,
  SUM(NVL(culture_cnt,0)) AS  culture_cnt,
  SUM(NVL(the_game_cnt,0)) AS  the_game_cnt,
  SUM(NVL(cricket_cnt,0)) AS  cricket_cnt,
  SUM(NVL(timesplus_cnt,0)) AS  timesplus_cnt,
  SUM(NVL(weekend_cnt,0)) AS  weekend_cnt,
  SUM(NVL(travel_cnt,0)) AS  travel_cnt,
  SUM(NVL(law_cnt,0)) AS  law_cnt,
  SUM(NVL(ireland_cnt,0)) AS  ireland_cnt
FROM
  [newsuk-datatech-dev-1251:tnl_trial_conversion.omniture_agg]
WHERE
  duration_today <= 35
  AND cpn NOT IN (
  SELECT
    cpn
  FROM (
    SELECT
      cpn,
      COUNT(* ) cnt
    FROM (TABLE_DATE_RANGE([newsuk-datatech-prod:athena.accounts_],CURRENT_TIMESTAMP(),CURRENT_TIMESTAMP()))
    WHERE
      subscriptions.mpc = 'MP370'
    GROUP BY
      cpn)
  WHERE
    cnt >1)
GROUP BY
  cpn,
  country1,
  gender1,
  subscriptions_contractStartDate,
  CancellationRequestedDate,
  duration_cancellation