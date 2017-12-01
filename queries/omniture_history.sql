SELECT
  a.cpn AS cpn,
  COUNT(DISTINCT(a.active_events_3_month)) active_days,
  MIN(a.date_diff) AS recency,
  activity_date,
  SUM(CASE
      WHEN a.WEEKEND =0 AND a.events =1 THEN 1
      ELSE 0 END) AS events_weekdays,
  SUM(CASE
      WHEN a.WEEKEND =1 AND a.events =1 THEN 1
      ELSE 0 END) AS events_weekends,
  COUNT(DISTINCT(CASE
        WHEN a.WEEKEND =0 THEN a.activity_date END)) AS active_weekdays,  COUNT(DISTINCT(CASE
        WHEN a.WEEKEND =1 THEN a.activity_date END)) AS active_weekends,
  COUNT(DISTINCT(a.week_no)) AS active_weeks,
  COUNT(DISTINCT(CASE
        WHEN a.WEEKEND =0 THEN a.visit_num END)) AS visits_weekdays,  COUNT(DISTINCT(CASE
        WHEN a.WEEKEND =1 THEN a.visit_num END)) AS visits_weekends,
  b.primary_device AS primary_device,
  b.secondary_device AS secondary_device,
  SUM(CASE
      WHEN a.page_views = 1 AND a.WEEKEND =0 THEN 1
      ELSE 0 END) AS views_weekdays,
  SUM(CASE
      WHEN a.page_views = 1 AND a.WEEKEND =1 THEN 1
      ELSE 0 END) AS views_weekends,
  SUM(CASE
      WHEN a.page_views = 1 AND a.activity_hour>4 AND a.activity_hour<=11 THEN 1
      ELSE 0 END) AS morning_views,
  SUM(CASE
      WHEN a.page_views = 1 AND a.activity_hour>11 AND a.activity_hour<=16 THEN 1
      ELSE 0 END) AS afternoon_views,
  SUM(CASE
      WHEN a.page_views = 1 AND a.activity_hour>16 AND a.activity_hour<=21 THEN 1
      ELSE 0 END) AS evening_views,
  SUM(CASE
      WHEN a.page_views = 1 AND (a.activity_hour>21 OR a.activity_hour <=4) THEN 1
      ELSE 0 END) AS late_night_views,
  SUM(a.page_views ) AS total_page_views,
  SUM(interactions) AS interactions,
  SUM(CASE
      WHEN a.section = 'news' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS news_cnt,
  SUM(CASE
      WHEN a.section = 'puzzles' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS puzzles_cnt,
  SUM(CASE
      WHEN a.section LIKE 'world%' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS world_cnt,
  SUM(CASE
      WHEN a.section LIKE '%Sport%' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS sport_cnt,
  SUM(CASE
      WHEN a.section LIKE '%Business%' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS business_cnt,
  SUM(CASE
      WHEN a.section = 'Comment' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS comment_cnt,
  SUM(CASE
      WHEN a.section ='Front' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS Front_cnt,
  SUM(CASE
      WHEN a.section = 'Times2' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS times2_cnt,
  SUM(CASE
      WHEN a.section = 'Register' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS register_cnt,
  SUM(CASE
      WHEN a.section = 'past 6 days' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS past6d_cnt,
  SUM(CASE
      WHEN a.section = 'Scotland' OR a.section = '%scottish%' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS scotland_cnt,
  SUM(CASE
      WHEN a.section = 'the times acquisition store' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS TAS_cnt,
  SUM(CASE
      WHEN a.section = 'news review' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS news_reviews_cnt,
  SUM(CASE
      WHEN a.section = 'Life' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS life_cnt,
  SUM(CASE
      WHEN a.section = 'Mindgames' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS mindgames_cnt,
  SUM(CASE
      WHEN a.section = 'Opinion' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS opinion_cnt,
  SUM(CASE
      WHEN a.section = 'Arts' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS arts_cnt,
  SUM(CASE
      WHEN a.section = 'fashion' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS fashion_cnt,
  SUM(CASE
      WHEN a.section = 'Driving' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS driving_cnt,
  SUM(CASE
      WHEN a.section = 'books' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS books_cnt,
  SUM(CASE
      WHEN a.section = 'Home' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS home_cnt,
  SUM(CASE
      WHEN a.section = 'Magazine' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS magazine_cnt,
  SUM(CASE
      WHEN a.section = 'Money' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS money_cnt,
  SUM(CASE
      WHEN a.section = 'football' OR a.section ='premier league' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS football_cnt,
  SUM(CASE
      WHEN a.section = 'News_review' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS news_review_cnt,
  SUM(CASE
      WHEN a.section = 'Saturday_review' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS saturday_review_cnt,
  SUM(CASE
      WHEN a.section = 'Style' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS style_cnt,
  SUM(CASE
      WHEN a.section = 'culture' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS culture_cnt,
  SUM(CASE
      WHEN a.section = 'The_game' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS the_game_cnt,
  SUM(CASE
      WHEN a.section = 'cricket' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS cricket_cnt,
  SUM(CASE
      WHEN a.section = 'Times+' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS timesplus_cnt,
  SUM(CASE
      WHEN a.section = 'weekend' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS weekend_cnt,
  SUM(CASE
      WHEN a.section = 'Travel' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS travel_cnt,
  SUM(CASE
      WHEN a.section = 'Law' THEN INTEGER('1')
      ELSE INTEGER('0') END) AS law_cnt,
  SUM(CASE
      WHEN a.section = 'Ireland' OR a.section = '%irish%'THEN INTEGER('1')
      ELSE INTEGER('0') END) AS ireland_cnt,
FROM (
  SELECT
    cpn,
    activity_hour,
    activity_date,
    visit_num,
    page_views,
    events,
    WEEK(DATE(activity_date) ) week_no,
    comments+ video_plays+ video_completes+ shares+ saves AS interactions,
    CASE
      WHEN DAYOFWEEK(DATE(activity_date) ) IN (1,  7) THEN 1
      ELSE 0
    END AS WEEKEND,
    CASE
      WHEN DATEDIFF(CURRENT_DATE(),DATE(activity_date)) <= 90 AND events =1 THEN activity_date
      ELSE NULL
    END AS active_events_3_month,
    DATEDIFF(CURRENT_DATE(),DATE(activity_date)) AS date_diff,
    section
  FROM (
    SELECT
      activity_date_time,
      activity_date,
      activity_hour,
      CASE
        WHEN cpn IS NULL THEN 'UNKNOWN'
        ELSE cpn
      END AS cpn,
      visitor_ids AS visitor_id,
      visit_num,
      visit_page_num,
      comment_events AS comments,
      video_plays_events AS video_plays,
      video_completes_events AS video_completes,
      share_events AS shares,
      saves_events AS saves,
      events AS events,
      page_views AS page_views,
      section
      # Referrer Type - need to be confirmed
    FROM (
      SELECT
        cpn,
        products,
        visitor_ids,
        visit_ids,
        visit_num,
        visit_page_num,
        --Added from phase2
        product_type,
        product_version,
        CASE
          WHEN next_date_time IS NULL THEN 1
          ELSE 0
        END AS last_page_visit,
        registration_events,
        comment_events,
        video_plays_events,
        video_completes_events,
        share_events,
        subscription_events,
        saves_events,
        events,
        page_views,
        activity_date,
        activity_hour,
        activity_date_time,
        next_date_time,
        section,
        CASE
          WHEN next_date_time IS NULL THEN 0
          ELSE (PARSE_UTC_USEC(next_date_time) - PARSE_UTC_USEC(activity_date_time))/1000000
        END AS dwell_secs
      FROM (
        SELECT
          *,
          LOWER(NVL(post_prop_filtered,evar2)) AS section,
          LEAD(activity_date_time, 1) OVER (PARTITION BY visit_ids ORDER BY visit_page_num) next_date_time
        FROM (
          SELECT
            CONCAT(post_visid_high,'_',post_visid_low) AS visitor_ids,
            CONCAT(post_visid_high,'_',post_visid_low,'_',visit_num) AS visit_ids,
            visit_num,
            INTEGER(visit_page_num) visit_page_num,
            DATE(date_time) AS activity_date,
            HOUR(date_time) AS activity_hour,
            date_time AS activity_date_time,
            CASE
              WHEN post_prop11 LIKE 'CPN%' THEN REGEXP_REPLACE(post_prop11, 'CPN:', '')
              WHEN post_prop11 LIKE 'cpn%' THEN REGEXP_REPLACE(post_prop11, 'cpn:', '')
              ELSE 'null'
            END AS cpn,
            COALESCE(post_prop1, post_evar1) AS products,
            CASE
              WHEN REGEXP_MATCH(post_event_list, r'^202,|,202,|,202$') = TRUE THEN 1
              ELSE 0
            END AS registration_events,
            CASE
              WHEN REGEXP_MATCH(post_event_list, r'^236,|,236,|,236$') = TRUE THEN 1
              ELSE 0
            END AS comment_events,
            CASE
              WHEN REGEXP_MATCH(post_event_list, r'^208,|,208,|,208$') = TRUE THEN 1
              ELSE 0
            END AS video_plays_events,
            CASE
              WHEN REGEXP_MATCH(post_event_list, r'^209,|,209,|,209$') = TRUE THEN 1
              ELSE 0
            END AS video_completes_events,
            CASE
              WHEN REGEXP_MATCH(post_event_list, r'^224,|,224,|,224$') = TRUE THEN 1
              ELSE 0
            END AS share_events,
            CASE
              WHEN REGEXP_MATCH(post_event_list, r'^294,|,294,|,294$') = TRUE THEN 1
              ELSE 0
            END AS subscription_events,
            CASE
              WHEN REGEXP_MATCH(post_event_list, r'^258,|,258,|,258$') = TRUE THEN 1
              ELSE 0
            END AS saves_events,
            1 AS events,
            CASE
              WHEN post_page_event = '0' THEN 1
              ELSE 0
            END AS page_views,
            visit_page_num AS page_num
            --Added from phase2
            ,
            post_page_event_var2,
            post_page_event AS event_type,
            post_page_event_var1 AS next_page,
            post_event_list AS event_list,
            CASE
              WHEN post_prop3 LIKE '%news%' THEN 'news'
              WHEN post_prop3 LIKE '%current edition%' THEN REGEXP_EXTRACT(post_prop3,r':([^,]*)')
              WHEN post_prop3 LIKE '%Main Book%' THEN REGEXP_EXTRACT(post_prop3,r':([^,]*)')
              WHEN post_prop3 LIKE '%Times2:%'THEN REGEXP_EXTRACT(post_prop3,r':([^,]*)')
              WHEN post_prop3 LIKE '%Sport%' THEN 'Sport'
              WHEN post_prop3 LIKE '%culture%' THEN 'culture'
              WHEN post_prop3 LIKE '%Travel%' THEN 'Travel'
              WHEN post_prop3 LIKE '%sport%' THEN 'sport'
              WHEN post_prop3 LIKE '%Magazine%' THEN 'Magazine'
              WHEN post_prop3 LIKE '%past 6 days%' THEN REGEXP_EXTRACT(post_prop3,r':([^,]*)')
            END AS post_prop_filtered,
            evar2,
            -- , case when prop1 = 'the times and sunday times' then post_prop71 else 'others' end as parent_site
          FROM (TABLE_DATE_RANGE([newsuk-datatech-prod:omniture_rawlogs_daily.newsinttimesnetworkprodv2_daily_], TIMESTAMP('2016-11-01'),CURRENT_TIMESTAMP())) )
        WHERE
          cpn IN (
          SELECT
            cpn
          FROM (TABLE_DATE_RANGE([newsuk-datatech-prod:athena.accounts_],DATE_ADD(CURRENT_TIMESTAMP(),-1,'DAY'),CURRENT_TIMESTAMP()))
          WHERE
            subscriptions.mpc = 'MP370')) )) )a
LEFT JOIN (
  SELECT
    cpn,
    MAX (pd) AS primary_device,
    MAX(sd) AS secondary_device
  FROM (
    SELECT
      cpn,
      CASE
        WHEN rank_=1 THEN device_type
      END AS pd,
      CASE
        WHEN rank_=2 THEN device_type
      END AS sd
    FROM (
      SELECT
        cpn,
        device_type,
        ROW_NUMBER(device_type ) OVER (PARTITION BY cpn ORDER BY cnt DESC) AS rank_,
      FROM (
        SELECT
          cpn,
          device_type,
          COUNT(*) AS cnt
        FROM (
          SELECT
            CASE
              WHEN M.cpn IS NULL THEN 'UNKNOWN'
              ELSE REGEXP_REPLACE(M.cpn, 'CPN:', '')
            END AS cpn,
            LK_DV.device_type AS device_type,
            M.post_mobiledevice
          FROM (
            SELECT
              post_mobiledevice,
              CASE
                WHEN post_prop11 LIKE 'CPN%' THEN REGEXP_REPLACE(post_prop11, 'CPN:', '')
                WHEN post_prop11 LIKE 'cpn%' THEN REGEXP_REPLACE(post_prop11, 'cpn:', '')
                ELSE 'null'
              END AS cpn
              -- , case when prop1 = 'the times and sunday times' then post_prop71 else 'others' end as parent_site
            FROM (TABLE_DATE_RANGE([newsuk-datatech-prod:omniture_rawlogs_daily.newsinttimesnetworkprodv2_daily_], TIMESTAMP('2016-11-01'),CURRENT_TIMESTAMP()))) M
          LEFT JOIN
            [newsuk-datatech-uat-144114:omniture_reporting.ref_device_type] LK_DV
          ON
            LK_DV.device = M.post_mobiledevice )
        WHERE
          device_type <> 'UNKNOWN'
        GROUP BY
          cpn,
          device_type )
      ORDER BY
        cnt DESC) )
  GROUP BY
    cpn) b
ON
  a.cpn =b.cpn
GROUP BY
  cpn,
  primary_device,
  secondary_device,
  activity_date IGNORE CASE