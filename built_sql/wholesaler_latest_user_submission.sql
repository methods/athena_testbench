WITH all_submissions AS (
  (
    SELECT
      'IVR' AS provenance,
      ivr_nhs_number AS nhs_number,
      CASE WHEN UPPER(ivr_delivery_supplies) = 'TRUE' THEN 'YES' ELSE 'NO' END AS has_access_to_essential_supplies,
      CASE WHEN UPPER(ivr_carry_supplies) = 'TRUE' THEN 'YES' ELSE 'NO' END AS is_able_to_carry_supplies,
      '' AS email_address,
      ivr_phone_number_calls AS phone_number_calls,
      '' AS phone_number_texts,
      COALESCE(
        TRY(DATE_PARSE(ivr_call_timestamp, '%Y-%m-%d %H:%i:%s')),
        TRY(DATE_PARSE(ivr_call_timestamp, '%Y-%m-%d %H:%i:%s.%f'))
      ) AS submission_time
    FROM "ivr_clean_staging"
    WHERE ivr_current_item_id = '17'
  )
  UNION ALL
  (
    SELECT
      'WEB' AS provenance,
      nhs_number,
      essential_supplies AS has_access_to_essential_supplies,
      carry_supplies AS is_able_to_carry_supplies,
      contact AS email_address,
      phone_number_calls,
      phone_number_texts,
      FROM_UNIXTIME(unixtimestamp) AS submission_time
    FROM "web_clean_staging"
    WHERE nhs_number <> ''
  )
)

, latest_submission AS (
  SELECT
    provenance,
    nhs_number,
    submission_time,
    has_access_to_essential_supplies,
    is_able_to_carry_supplies,
    email_address,
    phone_number_calls,
    phone_number_texts
  FROM (
    SELECT
      MAX(submission_time) AS st,
      nhs_number AS nn
    FROM all_submissions
    GROUP BY nhs_number
  )
  JOIN all_submissions
    ON nn = nhs_number
    AND st = submission_time
)


/* LA FEEDBACK */


, all_la_feedback AS (
  SELECT
      CAST("nhsnumber" AS varchar) "nhs_number"
      , "inputoutcomecode" "feedback_code"
      , COALESCE(
          TRY("date_parse"("inputcompletedoutcomedate", '%Y-%m-%d %H:%i:%s')),
          TRY("date_parse"("inputcompletedoutcomedate", '%Y-%m-%d %H:%i:%s.%f')),
          TRY("date_parse"("inputcompletedoutcomedate", '%d-%m-%Y')),
          TRY("date_parse"("inputcompletedoutcomedate", '%d/%m/%Y'))
          ) "feedback_time"
      , "inputoutcomecomments" "feedback_comments"
  FROM
      raw_la_outcomes_staging
)


/* LA FEEDBACK - CONTINUE */


, feedback_list_filtered_continue AS (
  
     SELECT
       "nhs_number"
     , "feedback_code"
     , "feedback_time"
     , "feedback_comments"
     FROM
       all_la_feedback
     WHERE "feedback_code" IN ('W006', 'F003')
)

, feedback_list_continue AS (
   SELECT
       "nhs_number"
     , "feedback_code"
     , "feedback_time"
     , "feedback_comments"
     , "row_number"() OVER (PARTITION BY "nhs_number", "feedback_time") "rk"
     FROM
       feedback_list_filtered_continue
)

, feedback_list_deduplicated_continue AS (
   SELECT
       "nhs_number"
     , "feedback_code"
     , "feedback_time"
     , "feedback_comments"
     FROM
       feedback_list_continue
     WHERE ("rk" = 1)
)

, latest_la_feedback_to_continue_boxes AS (
  SELECT feedback_list_deduplicated_continue.*
     FROM
       ((
        SELECT
          "max"("feedback_time") "ft"
        , "nhs_number" "nn"
        FROM
          feedback_list_deduplicated_continue
        GROUP BY "nhs_number"
     )
     INNER JOIN feedback_list_deduplicated_continue ON (("nhs_number" = "nn") AND ("ft" = "feedback_time")))
)


/* LA FEEDBACK - OPT OUT */


, feedback_list_filtered_opt_out AS (
  SELECT
       "nhs_number"
     , "feedback_code"
     , "feedback_time"
     , "feedback_comments"
     FROM
       all_la_feedback
     WHERE
        "feedback_code" IN ('W003', 'W004', 'D001', 'F002')
  
)

, feedback_list_opt_out AS (
   SELECT
       "nhs_number"
     , "feedback_code"
     , "feedback_time"
     , "feedback_comments"
     , "row_number"() OVER (PARTITION BY "nhs_number", "feedback_time") "rk"
     FROM
       feedback_list_filtered_opt_out
)

, feedback_list_deduplicated_opt_out AS (
  SELECT
      "nhs_number"
      , "feedback_code"
      , "feedback_time"
      , "feedback_comments"
    FROM
      feedback_list_opt_out
    WHERE ("rk" = 1)
)

, latest_la_feedback_to_stop_boxes AS (
  SELECT
      feedback_list_deduplicated_opt_out.*
    FROM
      (
        (
          SELECT
            "max"("feedback_time") "ft"
            , "nhs_number" "nn"
          FROM
            feedback_list_deduplicated_opt_out
          GROUP BY "nhs_number"
        )
        INNER JOIN feedback_list_deduplicated_opt_out ON (("nhs_number" = "nn") AND ("ft" = "feedback_time"))
      )
)


/* WHOLESALER FEEDBACK */

, wholesaler_opt_out_list AS (
  SELECT id      AS wholesaler_id,
         COALESCE(
             TRY("date_parse"("deldate", '%Y-%m-%d %H:%i:%s')),
             TRY("date_parse"("deldate", '%Y-%m-%d %H:%i:%s.%f')),
             TRY("date_parse"("deldate", '%d-%m-%Y')),
             TRY("date_parse"("deldate", '%d/%m/%Y'))
           )        wholesaler_delivery_date,
         outcome AS wholesaler_outcome,
         comment AS wholesaler_comments
  FROM clean_deliveries_outcome_staging
  where outcome = '3'
)

, wholesaler_opt_out_list_deduplicated AS (
  SELECT wholesaler_id, wholesaler_delivery_date, wholesaler_outcome, wholesaler_comments
  FROM (
         SELECT wholesaler_opt_out_list.*, "row_number"() OVER (PARTITION BY wholesaler_id, wholesaler_delivery_date) rk
         FROM wholesaler_opt_out_list
       )
  WHERE rk = 1
)

, latest_wholesaler_opt_out AS (
  SELECT wholesaler_opt_out_list_deduplicated.*
  FROM (
        (
          SELECT "max"("wholesaler_delivery_date") "ft", "wholesaler_id" "ws_id"
          FROM wholesaler_opt_out_list_deduplicated
          GROUP BY "wholesaler_id"
        )
         INNER JOIN wholesaler_opt_out_list_deduplicated
                    ON (("wholesaler_id" = "ws_id") AND ("wholesaler_delivery_date" = "ft"))
    )
)

/* STAGING OUTPUT */


, full_submission_record AS (
  SELECT latest_submission.*,
         "latest_la_feedback_to_stop_boxes"."feedback_code"         "stop_feedback_code",
         "latest_la_feedback_to_stop_boxes"."feedback_time"         "stop_feedback_time",
         "latest_la_feedback_to_stop_boxes"."feedback_comments"     "stop_feedback_comments",
         "latest_la_feedback_to_continue_boxes"."feedback_code"     "continue_feedback_code",
         "latest_la_feedback_to_continue_boxes"."feedback_time"     "continue_feedback_time",
         "latest_la_feedback_to_continue_boxes"."feedback_comments" "continue_feedback_comments",
         "latest_wholesaler_opt_out"."wholesaler_outcome"           "wholesaler_stop_code",
         "latest_wholesaler_opt_out"."wholesaler_outcome"           "wholesaler_stop_time",
         "latest_wholesaler_opt_out"."wholesaler_comments"           "wholesaler_comments",
         CASE
           WHEN "latest_la_feedback_to_stop_boxes"."feedback_time" >= latest_submission."submission_time"
             THEN 'YES'
           ELSE latest_submission."has_access_to_essential_supplies"
           END AS                                                   "resolved_has_access_to_essential_supplies"
  FROM (
        (
          (
            latest_submission
              LEFT JOIN latest_la_feedback_to_stop_boxes ON (latest_submission."nhs_number" =
                                                             "latest_la_feedback_to_stop_boxes"."nhs_number")
            )
            LEFT JOIN latest_la_feedback_to_continue_boxes ON (latest_submission."nhs_number" =
                                                               "latest_la_feedback_to_continue_boxes"."nhs_number")
          )
         LEFT JOIN latest_wholesaler_opt_out ON (latest_wholesaler_opt_out.wholesaler_id = LOWER(
      TO_HEX(MD5(TO_UTF8(CONCAT('${salt}:', latest_submission.nhs_number))))))
    )
)


/* FINAL OUTPUT */

SELECT
  LOWER(TO_HEX(MD5(TO_UTF8(CONCAT('${salt}:', nhs_number))))) AS ID,
  submission_time AS Timestamp,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_first_name ELSE '' END AS FirstName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_other_name ELSE '' END AS MiddleName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_surname ELSE '' END AS LastName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line1 ELSE '' END AS Address1,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line2 ELSE '' END AS Address2,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line3 ELSE '' END AS Address3,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line4 ELSE '' END AS Address4,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line5 ELSE '' END AS Address5,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_postcode ELSE '' END AS Postcode,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN email_address ELSE '' END AS Email,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN phone_number_calls ELSE '' END AS PhoneCalls,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN phone_number_texts ELSE '' END AS PhoneTexts,
  CASE WHEN nhs_deceased = '0' THEN resolved_has_access_to_essential_supplies ELSE 'YES' END AS EssentialSupplies,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN is_able_to_carry_supplies ELSE '' END AS CarrySupplies,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' AND continue_feedback_code IN ('F003', 'W006') THEN continue_feedback_comments ELSE '' END AS DeliveryComments
FROM full_submission_record
JOIN "nhs_clean_staging" ON nhs_nhs_number = nhs_number