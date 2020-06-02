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
           THEN full_submission_record.stop_feedback_time
         ELSE full_submission_record.wholesaler_stop_time
       END AS DeregistrationDate,
       CASE
         WHEN full_submission_record.resolved_has_access_to_essential_supplies_source='local authority'
           THEN full_submission_record.stop_feedback_code
         ELSE full_submission_record.wholesaler_stop_code
       END AS DeregistrationCode,
       CASE
         WHEN full_submission_record.resolved_has_access_to_essential_supplies_source='local authority'
           THEN full_submission_record.stop_feedback_comments
         ELSE full_submission_record.wholesaler_comments
       END AS DeregistrationComments
FROM full_submission_record
       JOIN "nhs_clean_staging" ON nhs_nhs_number = nhs_number

WHERE nhs_deceased = '0'
  AND NOT full_submission_record.resolved_has_access_to_essential_supplies_source = 'web/ivr'