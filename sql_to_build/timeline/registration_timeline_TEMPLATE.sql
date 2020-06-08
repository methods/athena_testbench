WITH all_submissions AS (@sql_to_build/all_submissions.sql@)
   , all_la_feedback AS (@sql_to_build/all_la_feedback.sql@)
   , wholesaler_opt_out_list AS (@sql_to_build/wholesaler_opt_out_list.sql@)
   , registration_timeline AS (@sql_to_build/timeline/registration_timeline_select.sql@)

select * from registration_timeline