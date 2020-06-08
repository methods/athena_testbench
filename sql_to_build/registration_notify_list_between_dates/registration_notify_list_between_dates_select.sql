SELECT change_between_dates.nhs_number AS NhsNumber,
       change_between_dates.id AS ID,
       nhs_clean_staging.nhs_patients_first_name AS FirstName,
       nhs_clean_staging.nhs_patients_other_name AS MiddleName,
       nhs_clean_staging.nhs_patients_surname AS LastName,
       latest_submission.email_address AS Email,
       latest_submission.phone_number_calls AS PhoneCalls,
       latest_submission.phone_number_texts AS PhoneTexts,
       latest_address.address_line1 AS Address1,
       latest_address.address_line2 AS Address2,
       latest_address.address_line3 AS Address3,
       latest_address.address_line4 AS Address4,
       latest_address.address_line5 AS Address5,
       latest_address.address_postcode AS Postcode,
       change_between_dates.provenance AS Provenance,
       change_between_dates.has_access_to_essential_supplies AS HasAccessToEssentialSupplies,
       change_between_dates.event_datetime AS EventDatetime,
       change_between_dates.ingested_datetime AS IngestedDatetime
FROM change_between_dates
       JOIN nhs_clean_staging ON change_between_dates.nhs_number = nhs_clean_staging.nhs_nhs_number
       JOIN latest_submission ON change_between_dates.nhs_number = latest_submission.nhs_number
       JOIN clean_latest_address_staging AS latest_address ON change_between_dates.nhs_nhs_number = latest_address.nhs_number
       LEFT JOIN is_deceased_from_timeline on change_between_dates.nhs_number = is_deceased_from_timeline.nhs_number
WHERE nhs_clean_staging.is_deceased = '0' AND is_deceased_from_timeline.is_deceased IS null