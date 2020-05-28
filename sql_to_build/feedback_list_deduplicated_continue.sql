 SELECT
     "nhs_number"
   , "feedback_code"
   , "feedback_time"
   , "feedback_comments"
   FROM
     feedback_list_continue
   WHERE ("rk" = 1)
