import pandas as pd
from collections import defaultdict
import math

def null_treatment(df,default_value_df):
    """
    replacing few columns null values with default values
    """
    columns= [
        'Deal Priority',
        'What level of engagement will you require from the marketer?',
        'How soon do you need the marketer?',
        'How long do you need the marketer?',
        'Industry Experience',
        'How big is your company?'
    ]

    for col in columns:
        fillna_value= default_value_df[default_value_df['column_name']==col]['value'].values[0]
        df[col]= df[col].fillna(value= fillna_value)
    return df

def accepted_exp():
    accepted_exp_list= [
        "E-Commerce",
        "Consumer Tech",
        "Marketing Agency",
        "Services",
        "Local Business",
        "B2B Tech",
        "Apps",
        "Media",
        "Non Profit",
        "Events",
        "Other"
    ]
    return accepted_exp_list

def replace_indus_exp(exp,accepted_exp_list):
    assigned_exp= "Other"
    if exp in accepted_exp_list:
        assigned_exp= exp
    elif exp=='DO NOT USE >> Other':
        assigned_exp= 'Other'
    elif exp=='DO NOT USE >> Retail':
        assigned_exp= 'Local Business'
    elif exp=='DO NOT USE >> Subscription':
        assigned_exp= 'E-Commerce'
    return assigned_exp

def dummies_industry_exp(x,accepted_exp_list):
    industry_exp = []
    li = list(x.split(","))
    li= list(set([exp.strip() for exp in li]))
    li= list(set([replace_indus_exp(exp,accepted_exp_list) for exp in li]))
    for i in accepted_exp_list:
        count = 0
        for j in li:
            if i == j:
                count = count + 1                
        if count > 0:
            industry_exp.append(1)
        else:
            industry_exp.append(0)
    return industry_exp

def Cleaned_Company_size(x):
    size = ''
    if (x == '< 10') | (x == 'Less than 10'):
        size = 'Less than 10'
    elif x == '10-50':
        size = 'From 10 to 50'
    elif x == '51-200':
        size = 'From 51 to 200'     
    elif x == '201+':
        size = 'Greater than 200'
    elif x=='null':
        size= 'unknown'
    else:
        size = x
    return size

#it's needed to change
def ordinal_func(x, ordinal_dict):
    if x in ordinal_dict.keys():
        result= ordinal_dict[x]
    else:
        result=0
    return result

def get_ordinal_ratio_dict(ordinal_ratio_df):
    """
    gets ordinal ratio dictionary from ordinal ratio dataframe
    """
    new_dict= defaultdict(lambda: defaultdict(lambda:None))
    for key,value in ordinal_ratio_df.set_index(['column_name','category']).to_dict('index').items():
        new_dict[key[0]][key[1]]=value['value']
    
    return new_dict

def Cleaned_How_Soon(x):
    size = ''
    if (x == 'Decide later') | (x == "I'll decide later"):
        size = 'Decide later'
    elif (x == '1-2 weeks') | (x == "In 1-2 weeks"):
        size = 'one to two weeks' 
    elif (x == '2+ Weeks'):
        size = 'Two plus weeks'  
    elif (x == 'In 2  weeks'):
        size = 'In two weeks'        
    elif x =='null':
        size = 'unknown'       
    else:
        size = x
    return size

def Cleaned_How_long(x):
        size = ''
        if (x == 'Decide later') | (x == "I’ll decide later"):
            size = 'Decide later'  
        elif (x == '<1 month'):
            size = 'Lessthan one month'
        elif (x == '6+ months') | (x == 'Longer than 6 months'):
            size = '6 Plus months'
        elif x =='null':
            size= 'unknown'
        else:
            size = x
        return size

def cleaned_Engagement_level(x):
        x = x
        Engagement_level = ''
        if (x == 'Decide later') | (x == "I'll decide later") | (x == 'Decide later'):
            Engagement_level = 'Decide later'
        elif (x == 'Hourly, Hourly'):
            Engagement_level = 'Hourly'
        else:
            Engagement_level = x
        return Engagement_level

def hourly_budget(x,budget):
    x = x
    budget = budget
    hourly = 0
    if x == 'Part-time (20 Hours a week)':
        if budget < 501:
            hourly = budget
        else:    
            hourly = budget/20
    elif x == 'Full-time (40 Hours a week)':
        if budget < 501:
            hourly = budget
        else:   
            hourly = budget/40
    elif (x == 'Decide later') | (x == "I'll decide later"):
        if (budget > 500) & (budget <=4000):
            hourly = budget/20
        elif budget < 501:
            hourly = budget
        else:
            hourly = budget/40
    else:
        hourly = budget
    return hourly

def fillna_Weekly_Hourly_buget(eng_level,budget,Jobs_Deal_Budget_median_dict):
    final_budget = 0
    if (eng_level == 'Decide later'):
        if math.isnan(budget):
            final_budget = Jobs_Deal_Budget_median_dict['Decide later']
        elif budget < 10:
            final_budget = Jobs_Deal_Budget_median_dict['Decide later']
        else:
            final_budget = budget
    elif (eng_level == 'Hourly'):
        if math.isnan(budget):
            final_budget = Jobs_Deal_Budget_median_dict['Hourly']
        elif budget < 10:
            final_budget = Jobs_Deal_Budget_median_dict['Hourly']    
        else:
            final_budget = budget
    elif (eng_level == 'Part-time (20 Hours a week)'):
        if math.isnan(budget):
            final_budget = Jobs_Deal_Budget_median_dict['Part-time (20 Hours a week)']
        elif budget < 10:
            final_budget = Jobs_Deal_Budget_median_dict['Part-time (20 Hours a week)']    
        else:
            final_budget = budget
    else:
        if math.isnan(budget):
            final_budget = Jobs_Deal_Budget_median_dict['Full-time (40 hours a week)']
        elif budget < 10:
            final_budget = Jobs_Deal_Budget_median_dict['Full-time (40 hours a week)']     
        else:
            final_budget = budget
    return  final_budget

def get_processed_job_data(new_job_data,default_value_df,df_ordinal_ratio):
    
    #selecting required columns
    jobs = new_job_data.loc[:,[
        'Deal ID',
        'Deal Priority',
        'What level of engagement will you require from the marketer?',
        'Weekly or Hourly Budget',
        'How soon do you need the marketer?',
        'How long do you need the marketer?',
        #'What is the size of your business?',
        'Industry Experience',
        #'Industry Experience Other',
        'Type of Marketer',
        # 'Match Status',
        # 'Contact ID',
        'How big is your company?'
        ]]
    
    #null treatment for some columns
    jobs= null_treatment(jobs,default_value_df)

    #renaming the columns
    jobs_rename_cols = {
        #'Match Status' : 'Jobs_Match Status',
        'How soon do you need the marketer?' : 'How_soon',
        'What level of engagement will you require from the marketer?':'Jobs_Engagement Level',
        'How long do you need the marketer?':'How_long',
        'How big is your company?':'Company_size',
        'Industry Experience':'Jobs_Industry Experience',
        #'Industry Experience Other':'Jobs_Industry Experience Other'
        }
    jobs = jobs.rename(columns=jobs_rename_cols)

    #creating dummy variables for Jobs_Industry Experience
    accepted_exp_list= accepted_exp()
    jobs['Jobs_industry_exp'] = jobs['Jobs_Industry Experience'].astype(str).apply(lambda x: dummies_industry_exp(x,accepted_exp_list))
    ind_exp_cols = ['Indus_Exp_' + sub for sub in accepted_exp_list]
    jobs[ind_exp_cols] = pd.DataFrame(jobs.Jobs_industry_exp.tolist(), index= jobs.index)
    
    #get ordinal ratio dict for company_size,how_soon,how_long, deal_priority
    ordinal_ratio_dict= get_ordinal_ratio_dict(df_ordinal_ratio)
    
    #company size
    jobs['Cleaned_Company_Size'] = jobs['Company_size'].astype(str).apply(lambda x: Cleaned_Company_size(x))
    #assigining ordinal ratio
    jobs['Ordinal_Company_Size'] = jobs['Cleaned_Company_Size'].apply(lambda x:ordinal_func(x, ordinal_ratio_dict['company_size']))

    #how soon
    jobs['Cleaned_How_Soon'] = jobs['How_soon'].astype(str).apply(lambda x: Cleaned_How_Soon(x))
    #assiging how soon ordinal value
    jobs['Ordinal_How_Soon'] = jobs['Cleaned_How_Soon'].apply(lambda x:ordinal_func(x, ordinal_ratio_dict['how_soon']))

    #how long
    jobs['Cleaned_How_long'] = jobs['How_long'].astype(str).apply(lambda x: Cleaned_How_long(x))
    #assiging how_long ordinal value
    jobs['Ordinal_How_long'] = jobs['Cleaned_How_long'].apply(lambda x:ordinal_func(x, ordinal_ratio_dict['how_long']))

    #deal_priority
    jobs['OrdinalDeal Priority'] = jobs['Deal Priority'].apply(lambda x:ordinal_func(x, ordinal_ratio_dict['deal_priorty']))

    #preprocessing engagement level column
    jobs['Jobs_Engagement Level'] = jobs['Jobs_Engagement Level'].replace(to_replace ="[DO NOT USE] - Decide later",value ="Decide later")
    jobs['Jobs_Engagement Level'] = jobs['Jobs_Engagement Level'].replace(to_replace ="[DO NOT USE] - I'll decide later",value ="Decide later")
    jobs['Jobs_Engagement Level'] = jobs['Jobs_Engagement Level'].replace(to_replace ="Decide later",value ="Hourly")
    jobs['Jobs_Engagement Level'] = jobs['Jobs_Engagement Level'].replace(to_replace ="Part-time (20 hours a week)",value ="Part-time (20 Hours a week)")
    #clean engagement level
    jobs['Cleaned_Engagement_Level'] = jobs['Jobs_Engagement Level'].astype(str).apply(lambda x: cleaned_Engagement_level(x))
    #adding Engagement prefix columns
    #cleaned eng levels list from default table
    cleaned_eng_levels_list= list(default_value_df[default_value_df['column_name']=='New_Hourly_budget']['criteria'].values)
    new_job_eng_level= jobs['Cleaned_Engagement_Level'].values[0]
    data= [1 if each==new_job_eng_level else 0 for each in cleaned_eng_levels_list]
    Engagement_columns= ["Engagement_"+ each for each in  cleaned_eng_levels_list]
    Engagement_dummies = pd.DataFrame(data=[data],columns=Engagement_columns)
    jobs = pd.concat([jobs, Engagement_dummies], axis=1)

    #Weekly or Hourly Budget
    jobs['Weekly or Hourly Budget']= jobs['Weekly or Hourly Budget'].astype(float)
    jobs['hourly_budget'] = jobs.apply(lambda x: hourly_budget(x['Cleaned_Engagement_Level'],x['Weekly or Hourly Budget']),axis = 1)
    #getting jobs budget median for each cleaned engagement level
    Jobs_Deal_Budget_median_dict=dict(default_value_df[default_value_df['column_name']=='New_Hourly_budget'][['criteria','value']].values)
    #converting budget values from str to float
    Jobs_Deal_Budget_median_dict= {k:float(v) for k,v in Jobs_Deal_Budget_median_dict.items()}
    #getting new hourly budget
    jobs['New_Hourly_budget'] = jobs.apply(lambda x: fillna_Weekly_Hourly_buget(
                                                        x['Cleaned_Engagement_Level'],
                                                        x['hourly_budget'],
                                                        Jobs_Deal_Budget_median_dict
                                                        ),axis = 1)


    jobs['key'] = 1
    #removing jobs_industry_exp
    jobs.drop('Jobs_industry_exp',axis=1,inplace=True)
    
    return jobs