
   SELECT
     "nhs_number"
   , "feedback_code"
   , "feedback_time"
   , "feedback_comments"
   FROM
     all_la_feedback
   WHERE "feedback_code" IN ('W006', 'F003')
