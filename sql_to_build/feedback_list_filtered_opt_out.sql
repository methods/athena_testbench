SELECT
     "nhs_number"
   , "feedback_code"
   , "feedback_time"
   , "feedback_comments"
   FROM
     all_la_feedback
   WHERE (("feedback_code" = 'W003') OR ("feedback_code" = 'W004')) OR ("feedback_code" = 'D001')
