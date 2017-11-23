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
  SUM(a.page_views ) AS total_page_views,interactions
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
      WHEN DAYOFWEEK(DATE(activity_date) ) IN (1, 7) THEN 1
      ELSE 0
    END AS WEEKEND,
    CASE
      WHEN DATEDIFF(CURRENT_DATE(),DATE(activity_date)) <= 90 AND events =1 THEN activity_date
      ELSE NULL
    END AS active_events_3_month,
    DATEDIFF(CURRENT_DATE(),DATE(activity_date)) AS date_diff,
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
      visit_page_num
      ,
      comment_events AS comments,
      video_plays_events AS video_plays,
      video_completes_events AS video_completes,
      share_events AS shares,
      saves_events AS saves,
      events AS events,
      page_views AS page_views
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
        product_version
        ,
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
        CASE
          WHEN next_date_time IS NULL THEN 0
          ELSE (PARSE_UTC_USEC(next_date_time) - PARSE_UTC_USEC(activity_date_time))/1000000
        END AS dwell_secs
      FROM (
        SELECT
          M.cpn,
          M.products M.visit_num,
          M.visit_page_num
          --, MAX(M.visit_num) OVER (PARTITION by M.cpn, M.activity_date) as last_visit
          ,
          M.visitor_ids,
          M.visit_ids,
          M.registration_events,
          M.comment_events,
          M.video_plays_events,
          M.video_completes_events
          -- Added from phase2
          -- , M.article_publish_time
          ,
          M.event_type,
          M.next_page,
          M.share_events,
          M.subscription_events,
          M.saves_events,
          M.events,
          M.page_views,
          M.activity_date_time,
          M.activity_date,
          M.activity_hour,
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
              WHEN UPPER(LEFT(post_evar35,4)) ='CPN:' THEN REGEXP_REPLACE(post_evar35, 'CPN:', '')
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
            post_event_list AS event_list
            -- , case when prop1 = 'the times and sunday times' then post_prop71 else 'others' end as parent_site
          FROM (TABLE_DATE_RANGE([newsuk-datatech-prod:omniture_rawlogs_daily.newsinttimesnetworkprodv2_daily_], TIMESTAMP('2016-11-04'),CURRENT_TIMESTAMP())) ) M
        LEFT JOIN (
          SELECT
            session_id,
            referrer_type
          FROM
            [newsuk-datatech-uat-144114:reporting_views.view_times_ref_type_last3days]) LK_RF
        ON
          LK_RF.session_id = M.visit_ids
        LEFT JOIN (
          SELECT
            product,
            product_type,
            application_type,
            operating_system,
            product_version
          FROM
            [newsuk-datatech-uat-144114:omniture_reporting.ref_times_product]) LK_PRODUCT
        ON
          LK_PRODUCT.product = M.products
        WHERE
          M.cpn IN (
          SELECT
            cpn
          FROM
            [newsuk-datatech-prod:athena.accounts_20171113]
          WHERE
            subscriptions.mpc = 'MP370')) )) )a LEFT JOIN (
  SELECT
    cpn,
    MAX (pd) AS primary_device,
    MAX(sd) AS secondary_device FROM (
    SELECT
      cpn,
      CASE
        WHEN rank_=1 THEN device_type
      END AS pd,
      CASE
        WHEN rank_=2 THEN device_type
      END AS sd FROM (
      SELECT
        cpn,
        device_type,
        ROW_NUMBER(device_type ) OVER (PARTITION BY cpn ORDER BY cnt DESC) AS rank_,
      FROM (
        SELECT
          cpn,
          device_type,
          COUNT(*) AS cnt FROM (
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
                WHEN UPPER(LEFT(post_evar35,4)) ='CPN:' THEN post_evar35
                ELSE 'null'
              END AS cpn
              -- , case when prop1 = 'the times and sunday times' then post_prop71 else 'others' end as parent_site
            FROM (TABLE_DATE_RANGE([newsuk-datatech-prod:omniture_rawlogs_daily.newsinttimesnetworkprodv2_daily_],
            TIMESTAMP('2016-11-04'),CURRENT_TIMESTAMP()))) M
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
  activity_date IGNORE
  CASE