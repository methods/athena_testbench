(
  SELECT
    'IVR' AS provenance,
    ivr_nhs_number AS nhs_number,
    CASE WHEN UPPER(ivr_delivery_supplies) = 'TRUE' THEN 'YES' ELSE 'NO' END AS has_access_to_essential_supplies,
    CASE WHEN UPPER(ivr_carry_supplies) = 'TRUE' THEN 'YES' ELSE 'NO' END AS is_able_to_carry_supplies,
    '' AS email_address,
    ivr_phone_number_calls AS phone_number_calls,
    '' AS phone_number_texts,
    COALESCE(
      TRY(DATE_PARSE(ivr_call_timestamp, '%Y-%m-%d %H:%i:%s')),
      TRY(DATE_PARSE(ivr_call_timestamp, '%Y-%m-%d %H:%i:%s.%f'))
    ) AS submission_time
  FROM "ivr_clean_staging"
  WHERE ivr_current_item_id = '17'
)
UNION ALL
(
  SELECT
    'WEB' AS provenance,
    nhs_number,
    essential_supplies AS has_access_to_essential_supplies,
    carry_supplies AS is_able_to_carry_supplies,
    contact AS email_address,
    phone_number_calls,
    phone_number_texts,
    FROM_UNIXTIME(unixtimestamp) AS submission_time
  FROM "web_clean_staging"
  WHERE nhs_number <> ''
)
