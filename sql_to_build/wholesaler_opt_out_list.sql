SELECT
  id AS wholesaler_id,
  COALESCE(
      TRY("date_parse"("deldate", '%Y-%m-%d %H:%i:%s')),
      TRY("date_parse"("deldate", '%Y-%m-%d %H:%i:%s.%f')),
      TRY("date_parse"("deldate", '%d-%m-%Y')),
      TRY("date_parse"("deldate", '%d/%m/%Y'))
    ) AS wholesaler_delivery_date,
  outcome AS wholesaler_outcome,
  comment AS wholesaler_comments
FROM clean_deliveries_outcome_staging
where outcome = '3'
