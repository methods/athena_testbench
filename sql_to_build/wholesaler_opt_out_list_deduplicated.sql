SELECT wholesaler_id, wholesaler_delivery_date, wholesaler_outcome, wholesaler_comments
FROM (
       SELECT wholesaler_opt_out_list.*, "row_number"() OVER (PARTITION BY wholesaler_id, wholesaler_delivery_date) rk
       FROM wholesaler_opt_out_list
     )
WHERE rk = 1