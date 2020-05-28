SELECT
    *
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
