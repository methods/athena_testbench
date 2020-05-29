WITH feedback_deregistered AS (
  SELECT
     "nhs_number"
   , "feedback_code"
   , "feedback_time"
   , "feedback_comments"
   FROM
     all_la_feedback
   WHERE ("feedback_code" = 'W003') OR ("feedback_code" = 'W004') OR ("feedback_code" = 'D001')
)

, feedback_deregistered_w_row_counts AS (
   SELECT
     "nhs_number"
   , "feedback_code"
   , "feedback_time"
   , "feedback_comments"
   , "row_number"() OVER (PARTITION BY "nhs_number", "feedback_time") "rk"
   FROM
     feedback_deregistered
)

, feedback_deregistered_dededuplicated AS (
  SELECT
    "nhs_number"
    , "feedback_code"
    , "feedback_time"
    , "feedback_comments"
  FROM
    feedback_deregistered_w_row_counts
  WHERE ("rk" = 1))

SELECT
    *
  FROM
    (
      (
        SELECT
          "max"("feedback_time") "ft"
          , "nhs_number" "nn"
        FROM
          feedback_deregistered_dededuplicated
        GROUP BY "nhs_number"
      )
      INNER JOIN feedback_deregistered_dededuplicated ON (("nhs_number" = "nn") AND ("ft" = "feedback_time"))
    )
