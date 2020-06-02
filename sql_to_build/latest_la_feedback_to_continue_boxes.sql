SELECT feedback_list_deduplicated_continue.*
  FROM
    (
      (
        SELECT
          "max"("feedback_time") AS "ft"
          , "nhs_number" AS "nn"
        FROM
          feedback_list_deduplicated_continue
        GROUP BY "nhs_number"
      )
      INNER JOIN feedback_list_deduplicated_continue ON (("nhs_number" = "nn") AND ("ft" = "feedback_time"))
    )
