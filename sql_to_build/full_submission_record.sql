SELECT latest_submission.*,
       "latest_la_feedback_to_stop_boxes"."feedback_code"         AS "stop_feedback_code",
       "latest_la_feedback_to_stop_boxes"."feedback_time"         AS "stop_feedback_time",
       "latest_la_feedback_to_stop_boxes"."feedback_comments"     AS "stop_feedback_comments",
       "latest_la_feedback_to_continue_boxes"."feedback_code"     AS "continue_feedback_code",
       "latest_la_feedback_to_continue_boxes"."feedback_time"     AS "continue_feedback_time",
       "latest_la_feedback_to_continue_boxes"."feedback_comments" AS "continue_feedback_comments",
       "latest_wholesaler_opt_out"."wholesaler_outcome"           AS "wholesaler_stop_code",
       "latest_wholesaler_opt_out"."wholesaler_delivery_date"     AS "wholesaler_stop_time",
       "latest_wholesaler_opt_out"."wholesaler_comments"          AS "wholesaler_comments",
       CASE
        WHEN "latest_la_feedback_to_stop_boxes"."feedback_time" >= latest_submission."submission_time"
          THEN 'YES'
        WHEN "latest_wholesaler_opt_out"."wholesaler_delivery_date" >= latest_submission."submission_time"
          THEN 'YES'
        ELSE latest_submission."has_access_to_essential_supplies"
        END AS "resolved_has_access_to_essential_supplies"
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
