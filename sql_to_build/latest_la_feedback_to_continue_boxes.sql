SELECT *
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
