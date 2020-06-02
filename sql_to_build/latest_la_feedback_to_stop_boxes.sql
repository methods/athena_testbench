SELECT feedback_list_deduplicated_opt_out.*
  FROM
    (
      (
        SELECT
          "max"("feedback_time") AS "ft"
          , "nhs_number" AS "nn"
        FROM
          feedback_list_deduplicated_opt_out
        GROUP BY "nhs_number"
      )
      INNER JOIN feedback_list_deduplicated_opt_out ON (("nhs_number" = "nn") AND ("ft" = "feedback_time"))
    )
