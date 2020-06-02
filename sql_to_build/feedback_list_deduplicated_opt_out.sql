SELECT
    "nhs_number"
    , "feedback_code"
    , "feedback_time"
    , "feedback_comments"
  FROM
    feedback_list_opt_out
  WHERE ("rk" = 1)
