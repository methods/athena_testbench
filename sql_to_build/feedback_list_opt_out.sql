 SELECT
     "nhs_number"
   , "feedback_code"
   , "feedback_time"
   , "feedback_comments"
   , "row_number"() OVER (PARTITION BY "nhs_number", "feedback_time") "rk"
   FROM
     feedback_list_filtered_opt_out
