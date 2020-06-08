WITH all_submissions AS (@sql_to_build/all_submissions.sql@)
   , latest_submission AS (@sql_to_build/latest_submission.sql@)
   , all_la_feedback AS (@sql_to_build/all_la_feedback.sql@)
   , wholesaler_opt_out_list AS (@sql_to_build/wholesaler_opt_out_list.sql@)
   , registration_timeline AS (@sql_to_build/timeline/registration_timeline_select.sql@)
   , @sql_to_build/registration_change_between_dates/registration_change_between_dates_stack.sql@
   , @sql_to_build/is_deceased_from_timeline/is_deceased_from_timeline_stack.sql@

@sql_to_build/registration_notify_list_between_dates/registration_notify_list_between_dates_select.sql@