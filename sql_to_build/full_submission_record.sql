SELECT
  latest_submission.*
  , "latest_la_feedback_to_stop_boxes"."feedback_code" "stop_feedback_code"
  , "latest_la_feedback_to_stop_boxes"."feedback_time" "stop_feedback_time"
  , "latest_la_feedback_to_stop_boxes"."feedback_comments" "stop_feedback_comments"
  , "latest_la_feedback_to_continue_boxes"."feedback_code" "continue_feedback_code"
  , "latest_la_feedback_to_continue_boxes"."feedback_time" "continue_feedback_time"
  , "latest_la_feedback_to_continue_boxes"."feedback_comments" "continue_feedback_comments"
  , 'NO' AS "resolved_has_access_to_essential_supplies"
FROM
  (
    (
      latest_submission
      LEFT JOIN latest_la_feedback_to_stop_boxes ON (latest_submission."nhs_number" = "latest_la_feedback_to_stop_boxes"."nhs_number")
    )
    LEFT JOIN latest_la_feedback_to_continue_boxes ON (latest_submission."nhs_number" = "latest_la_feedback_to_continue_boxes"."nhs_number")
  )
