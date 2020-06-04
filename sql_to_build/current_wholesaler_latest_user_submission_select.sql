SELECT
  LOWER(TO_HEX(MD5(TO_UTF8(CONCAT('{salt}:', nhs.nhs_nhs_number))))) AS ID,
  latest_submission.submission_time AS Timestamp,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN nhs.nhs_patients_first_name ELSE '' END AS FirstName,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN nhs.nhs_patients_other_name ELSE '' END AS MiddleName,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN nhs.nhs_patients_surname ELSE '' END AS LastName,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_address.address_line1 ELSE '' END AS Address1,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_address.address_line2 ELSE '' END AS Address2,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_address.address_line3 ELSE '' END AS Address3,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_address.address_line4 ELSE '' END AS Address4,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_address.address_line5 ELSE '' END AS Address5,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_address.address_postcode ELSE '' END AS Postcode,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_submission.email_address ELSE '' END AS Email,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_submission.phone_number_calls ELSE '' END AS PhoneCalls,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_submission.phone_number_texts ELSE '' END AS PhoneTexts,
  CASE WHEN nhs.nhs_deceased = '0' THEN latest_submission.has_access_to_essential_supplies ELSE 'YES' END AS EssentialSupplies,
  CASE WHEN latest_submission.has_access_to_essential_supplies = 'NO' AND nhs.nhs_deceased = '0' THEN latest_submission.is_able_to_carry_supplies ELSE '' END AS CarrySupplies
FROM latest_submission
JOIN "nhs_clean_staging" AS nhs
  ON nhs_nhs_number = latest_submission.nhs_number
INNER JOIN "clean_latest_address_staging" AS latest_address
  ON nhs.nhs_nhs_number = latest_address.nhs_number