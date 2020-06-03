SELECT wholesaler_opt_out_list_deduplicated.*
  FROM (
    (
      SELECT "max"("wholesaler_delivery_date") "ft", "wholesaler_id" "ws_id"
        FROM wholesaler_opt_out_list_deduplicated
        GROUP BY "wholesaler_id"
    )
    INNER JOIN wholesaler_opt_out_list_deduplicated
      ON (("wholesaler_id" = "ws_id") AND ("wholesaler_delivery_date" = "ft"))
  )
