SELECT
  LOWER(TO_HEX(MD5(TO_UTF8(CONCAT('${salt}:', full_submission_record.nhs_number))))) AS ID,
  submission_time AS Timestamp,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_first_name ELSE '' END AS FirstName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_other_name ELSE '' END AS MiddleName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_surname ELSE '' END AS LastName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN latest_address.address_line1 ELSE '' END AS Address1,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN latest_address.address_line2 ELSE '' END AS Address2,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN latest_address.address_line3 ELSE '' END AS Address3,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN latest_address.address_line4 ELSE '' END AS Address4,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN latest_address.address_line5 ELSE '' END AS Address5,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN latest_address.address_postcode ELSE '' END AS Postcode,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN email_address ELSE '' END AS Email,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN phone_number_calls ELSE '' END AS PhoneCalls,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN phone_number_texts ELSE '' END AS PhoneTexts,
  CASE WHEN nhs_deceased = '0' THEN resolved_has_access_to_essential_supplies ELSE 'YES' END AS EssentialSupplies,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN is_able_to_carry_supplies ELSE '' END AS CarrySupplies,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' AND continue_feedback_code IN ('F003', 'W006') THEN continue_feedback_comments ELSE '' END AS DeliveryComments
FROM full_submission_record
JOIN "nhs_clean_staging" AS nhs
  ON nhs.nhs_nhs_number = full_submission_record.nhs_number
INNER JOIN "clean_latest_address_staging" AS latest_address
  ON nhs.nhs_nhs_number = latest_address.nhs_number