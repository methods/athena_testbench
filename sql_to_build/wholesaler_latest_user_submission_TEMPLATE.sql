WITH all_submissions AS (@all_submissions.sql@)

, latest_submission AS (@latest_submission.sql@)


/* LA FEEDBACK */


, all_la_feedback AS (@all_la_feedback.sql@)


/* LA FEEDBACK - CONTINUE */


, feedback_list_filtered_continue AS (@feedback_list_filtered_continue.sql@)

, feedback_list_continue AS (@feedback_list_continue.sql@)

, feedback_list_deduplicated_continue AS (@feedback_list_deduplicated_continue.sql@)

, latest_la_feedback_to_continue_boxes AS (@latest_la_feedback_to_continue_boxes.sql@)


/* LA FEEDBACK - OPT OUT */


, feedback_list_filtered_opt_out AS (@feedback_list_filtered_opt_out.sql@)

, feedback_list_opt_out AS (@feedback_list_opt_out.sql@)

, feedback_list_deduplicated_opt_out AS (@feedback_list_deduplicated_opt_out.sql@)

, latest_la_feedback_to_stop_boxes AS (@latest_la_feedback_to_stop_boxes.sql@)


/* STAGING OUTPUT */


, full_submission_record AS (@full_submission_record.sql@)


/* FINAL OUTPUT */

SELECT
  LOWER(TO_HEX(MD5(TO_UTF8(CONCAT('${salt}:', nhs_number))))) AS ID,
  submission_time AS Timestamp,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_first_name ELSE '' END AS FirstName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_other_name ELSE '' END AS MiddleName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_surname ELSE '' END AS LastName,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line1 ELSE '' END AS Address1,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line2 ELSE '' END AS Address2,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line3 ELSE '' END AS Address3,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line4 ELSE '' END AS Address4,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_patients_address_line5 ELSE '' END AS Address5,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN nhs_postcode ELSE '' END AS Postcode,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN email_address ELSE '' END AS Email,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN phone_number_calls ELSE '' END AS PhoneCalls,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN phone_number_texts ELSE '' END AS PhoneTexts,
  CASE WHEN nhs_deceased = '0' THEN resolved_has_access_to_essential_supplies ELSE 'YES' END AS EssentialSupplies,
  CASE WHEN resolved_has_access_to_essential_supplies = 'NO' AND nhs_deceased = '0' THEN is_able_to_carry_supplies ELSE '' END AS CarrySupplies,
  delivery_comments AS DeliveryComments
FROM full_submission_record
JOIN "nhs_clean_staging" ON nhs_nhs_number = nhs_number
