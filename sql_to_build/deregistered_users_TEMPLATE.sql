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


/* WHOLESALER FEEDBACK */

, wholesaler_opt_out_list AS (@wholesaler_opt_out_list.sql@)

, wholesaler_opt_out_list_deduplicated AS (@wholesaler_opt_out_list_deduplicated.sql@)

, latest_wholesaler_opt_out AS (@latest_wholesaler_opt_out.sql@)

/* STAGING OUTPUT */


, full_submission_record AS (@full_submission_record.sql@)

SELECT nhs_patients_first_name                                                 AS FirstName,
       nhs_patients_other_name                                                 AS MiddleName,
       nhs_patients_surname                                                    AS LastName,
       nhs_patients_address_line1                                              AS Address1,
       nhs_patients_address_line2                                              AS Address2,
       nhs_patients_address_line3                                              AS Address3,
       nhs_patients_address_line4                                              AS Address4,
       nhs_patients_address_line5                                              AS Address5,
       nhs_postcode                                                            AS Postcode,
       email_address                                                           AS Email,
       phone_number_calls                                                      AS PhoneCalls,
       phone_number_texts                                                      AS PhoneTexts,
       full_submission_record.resolved_has_access_to_essential_supplies_source as DeregistrationSource,
       CASE
         WHEN full_submission_record.resolved_has_access_to_essential_supplies_source='local authority'
           THEN latest_submission.stop_feedback_time
         ELSE latest_submission.wholesaler_stop_time
       END AS DeregistrationDate,
       CASE
         WHEN full_submission_record.resolved_has_access_to_essential_supplies_source='local authority'
           THEN latest_submission.stop_feedback_code
         ELSE latest_submission.wholesaler_stop_code
       END AS DeregistrationCode,
       CASE
         WHEN full_submission_record.resolved_has_access_to_essential_supplies_source='local authority'
           THEN latest_submission.stop_feedback_comments
         ELSE latest_submission.wholesaler_stop_comments
       END AS DeregistrationComments
FROM full_submission_record
       JOIN "nhs_clean_staging" ON nhs_nhs_number = nhs_number

WHERE nhs_deceased = '0'
  AND NOT full_submission_record.resolved_has_access_to_essential_supplies_source = 'Web/IVR'