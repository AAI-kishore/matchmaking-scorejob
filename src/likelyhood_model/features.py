def get_features(deal_role):
    #Features selection based on deal role
    if deal_role == 'Growth Marketer':
        Continuous_var = ['Price_ratio','Ordinal_How_Soon','Ordinal_How_long','Response_Ratio',
                 'Ordinal_Company_Size','FL_priority_Job_match_attempts']

        Final_categorical_var = ['Indus_Exp_Consumer Tech','Indus_Exp_E-Commerce','Indus_Exp_Local Business',
                         'Indus_Exp_Services','Indus_Exp_Marketing Agency','Engagement_Hourly',
                         'Vetted_Industry_Media','Vetted_Industry_Subscription',
                         'Engagement_Part-time (20 Hours a week)','Role_Growth Marketer']

        scaling_var = ['Ordinal_Hours_Avail','Years of experience','No Response - Decline Reason (Last 30 Days)']
    elif deal_role == 'Paid Social Media Marketer':
        Continuous_var = ['Price_ratio','Ordinal_How_Soon','Ordinal_How_long','Response_Ratio',
                 'Ordinal_Company_Size','FL_priority_Job_match_attempts']

        Final_categorical_var = ['Indus_Exp_E-Commerce','Engagement_Hourly','Indus_Exp_Marketing Agency','Indus_Exp_Services',
                         'Engagement_Part-time (20 Hours a week)','Role_Paid Social Media Marketer']

        scaling_var = ['Ordinal_Hours_Avail','Years of experience','No Response - Decline Reason (Last 30 Days)']
    elif deal_role == 'Email Marketer':
        Continuous_var = ['Price_ratio','Response_Ratio','Ordinal_Company_Size','Ordinal_How_Soon','Ordinal_How_long',
                  'OrdinalDeal Priority','FL_priority_Job_match_attempts']

        Final_categorical_var = ['Role_Email Marketer','Indus_Exp_B2B Tech','Indus_Exp_Consumer Tech','Indus_Exp_E-Commerce',
                         'Engagement_Hourly','Engagement_Part-time (20 Hours a week)']

        scaling_var = ['Ordinal_Hours_Avail','Years of experience','No Response - Decline Reason (Last 30 Days)']
    elif deal_role == 'Paid Search Marketer':
        Continuous_var = ['Price_ratio','Response_Ratio','Ordinal_Company_Size','Ordinal_How_Soon',
                  'OrdinalDeal Priority','FL_priority_Job_match_attempts']

        Final_categorical_var = ['Engagement_Hourly','Engagement_Part-time (20 Hours a week)',
                         'Role_Paid Search Marketer']

        scaling_var = ['Ordinal_Hours_Avail','Years of experience','No Response - Decline Reason (Last 30 Days)']
    else:
        Continuous_var = ['Price_ratio','Response_Ratio','Ordinal_How_Soon','Ordinal_How_long','FL_priority_Job_match_attempts']

        Final_categorical_var = ['Indus_Exp_B2B Tech','Indus_Exp_E-Commerce',
                         'Engagement_Hourly','Engagement_Part-time (20 Hours a week)']

        scaling_var = ['Ordinal_Hours_Avail','Years of experience','No Response - Decline Reason (Last 30 Days)']
        
    return Continuous_var, Final_categorical_var,scaling_var