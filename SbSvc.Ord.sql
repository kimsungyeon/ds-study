SELECT a.order_no AS ord_no
     , '' AS ord_seq
     , a.shop_no AS shop_no
     , a.member_no AS mem_no
     , b.ord_price AS ord_price
     , a.order_status AS ord_prog_cd
     , h.mem_id AS ord_id
     , a.order_datetime AS ord_tm
     , a.modified_by AS mod_id
     , a.modified_date AS mod_date
     , d.delivery_memo AS ord_msg -- 가게 요청 사항
     , '1' AS ct_ty_cd  --배달음식
     , e.category AS ct_cd
     , COALESCE(f.amount, 0) AS cost_price
     , e.order_accept_channel AS ord_take_ty_cd
     , '1' as ord_purch_cd
     , a.order_datetime AS ord_date
     , a.service_type AS service_type
     , a.service_channel AS service_channel
     , CASE WHEN a.service_type = 'BAEMIN' AND a.service_channel = 'APP' THEN '7jWXRELC2e'
            WHEN a.service_type = 'BAERA' AND a.service_channel = 'APP' THEN 'KxLWKxZZgS'
            WHEN a.service_channel = 'WWW' THEN 'L79Uq0m4r3'
            ELSE NULL END AS site_no
     , g.pay_method AS purch_method_cd
     , SUBSTR(rgn3, 1, 2) AS rgn1_cd
     , SUBSTR(rgn3, 1, 5) AS rgn2_cd
     , rgn3 AS rgn3_cd
     , a.reason_code AS reason_code
     , k.device_id AS dvc_id
     , c.delivery_time_minutes AS dlvry_cost_tm_min
     , i.ad_campaign_id as ad_campaign_id
     , j.id as ad_kind_id
     , j.ad_inventory_id as ad_inventory_id
     , d.rider_memo AS rider_msg  -- 라이더 요청 사항
     , DATE(a.order_datetime) AS ord_dt
FROM sborder.orders a
    LEFT OUTER JOIN (
        SELECT part_date
             , order_uuid
             , SUM(price * quantity) AS ord_price
         FROM sborder.line_item
        WHERE part_date >= '#[startDate minusDays|1]#'
          AND part_date < '#[endDate]#'
        GROUP BY part_date, order_uuid
    ) b
    ON a.uuid = b.order_uuid
  LEFT OUTER JOIN sborder.delivery c
    ON c.part_date >= '#[startDate minusDays|1]#'
   AND c.part_date < '#[endDate]#'
   AND a.delivery_uuid = c.uuid
  LEFT OUTER JOIN (
      SELECT order_uuid
           , MAX(CASE WHEN memo_type = 'DELIVERY' THEN memo END) delivery_memo -- 가게 요청 메모 - 참조(https://jira.woowa.in/browse/CTODATASD-1802)
           , MAX(CASE WHEN memo_type = 'RIDER' THEN  memo END) rider_memo       -- 라이더 요청 메모
       FROM sborder.order_memo
      WHERE part_date >= '#[startDate minusDays|1]#'
        AND part_date < '#[endDate]#'
      GROUP BY order_uuid
   ) d
    ON a.uuid = d.order_uuid
  LEFT OUTER JOIN sborder.seller_summary e
    ON e.part_date >= '#[startDate minusDays|1]#'
   AND e.part_date < '#[endDate]#'
   AND a.seller_summary_uuid = e.uuid
  LEFT OUTER JOIN sborder.charge_line f
    ON f.part_date >= '#[startDate minusDays|1]#'
   AND f.part_date < '#[endDate]#'
   AND a.uuid = f.order_uuid
   AND f.charge_type = 'DELIVERY_TIP'
  LEFT OUTER JOIN (
        SELECT
            order_uuid,
            pay_method,
            row_number() over (partition by order_uuid order by sum(amount) desc) as row_numb
          FROM sborder.order_pay_method
         WHERE part_date >= '#[startDate minusDays|1]#'
           AND part_date < '#[endDate]#'
         GROUP BY
            order_uuid,
            pay_method
        ) g
    ON g.row_numb = 1
   AND a.uuid = g.order_uuid
  LEFT OUTER JOIN sbsvc.mem h
    ON a.member_no = h.mem_no
  INNER JOIN sborder.ad_campaign i
    ON i.part_date >= '#[startDate minusDays|1]#'
   AND i.part_date < '#[endDate]#'
   AND a.order_no = i.order_no
  LEFT OUTER JOIN sbadcenter.ad_kind j
    ON i.ad_kind_id = j.id
  LEFT OUTER JOIN sborder.device k
    ON k.part_date >= '#[startDate minusDays|1]#'
   AND k.part_date < '#[endDate]#'
   AND a.order_no = k.order_no
 WHERE a.part_date >= '#[startDate minusDays|1]#'
   AND a.part_date < '#[endDate]#'
   AND a.service_type = 'BAEMIN'
   AND a.order_datetime >= CAST('#[startDate]#' AS timestamp)
   AND a.order_datetime < CAST('#[endDate]#' AS timestamp)
