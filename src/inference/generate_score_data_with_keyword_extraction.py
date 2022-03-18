import re
import pandas as pd
import spacy
import os
import logging
output_dir = os.path.join('models','NER_model')
nlp=spacy.load(output_dir)


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
    elif exp=='Non-Profit':
        assigned_exp= 'Non Profit'
    elif exp=='Subscription':
        assigned_exp= 'E-Commerce'
    elif exp=='Retail':
        assigned_exp= 'Local Business'
    elif exp=='B2B':
        assigned_exp= 'B2B Tech'

    return assigned_exp

def len_Paragraphs(text):
    #Par_len = len(text.split("\n\n"))
    Par_len = len(re.split(r'\n+', text))
    return Par_len

def sentences_list(text):
    text = text.strip()
    text = text.replace("-", "")
    text_len = len(text)
    sent_list = re.split(r'\n+', text)
    Final_str = ''
    for i in range(len(sent_list)):
        if (i == 0)&(len(sent_list) > 1):
            if (len(sent_list)==2) &(text[text_len - 1] == '\n'):
                Final_str = Final_str+sent_list[i]
            else:    
                pass
        else:
            Final_str = Final_str+sent_list[i]
    return Final_str

def all_text(str_col,df):
    y = []
    str_len = 0
    for i in range(df.shape[0]):
        x = []
        for j in range(len(str_col)):
            text = ' '.join([df.loc[i,str_col[j]]])
            x.append(text.strip())
        y.append(" ".join([str(elem) for elem in x]))
    #print(len(y))

    z = []
    for i in range(df.shape[0]):
    
        # String writings are always not asexpected ',space' or '/space'
        y[i] = y[i].replace(",", ", ")
        y[i] = y[i].replace("/", " ")
        y[i] = y[i].replace(r"\n", " ")
        y[i] = y[i].replace(r"-", "")
        y[i] = y[i].replace(r"_", "")
        #y[i] = y[i].replace(r"..", ".")
        #y[i] = y[i].replace(r"  ", " ")

        y[i] = y[i].replace("cro website", "website cro")
        y[i] = y[i].replace("directtoconsumer tech", "directtoconsumer")
        y[i] = y[i].replace("launches", "launch")
        y[i] = y[i].replace("tik tok", "tiktok")
        y[i] = y[i].replace("fbook", "Facebook")
        y[i] = y[i].replace("twiter", "twitter")
        y[i] = y[i].replace("Ad Words", "AdWords")
        y[i] = y[i].replace("local businesses", "local business")
        y[i] = re.sub(r'insta\s', 'instagram ',y[i])        

        # Remove punctuations
        #y[i] = y[i].translate(str.maketrans('','',string.punctuation))
        y[i] = y[i].replace(".", " .")
        
        y[i] = re.sub(r'ig\s', 'Instagram ',y[i])
        y[i] = re.sub(r'fb\s', 'Facebook ',y[i])
        y[i] = re.sub(r'IG\s', 'Instagram ',y[i])
        y[i] = re.sub(r'FB\s', 'Facebook ',y[i])
        
        z.append(y[i])
    docs=z[:]
    return docs

def extracting_entities(text,entity_name):
    d = []
    for i in range(len(text)):
        doc = nlp(text[i])
        #doc= list([(ent.text.lower()) for ent in doc.ents if ent.label_== entity_name])
        doc= list([(ent.text.lower()) for ent in doc.ents if (ent.label_== entity_name) & (ent.text != '') & (ent.text != '.') & (ent.text.startswith('.') is False) & (ent.text.startswith('@') is False)])
        doc= ['account based marketing' if entity=='abm' else entity for entity in doc]
        doc= ['app store optimization' if entity=='aso' else entity for entity in doc]
        doc= ['google analytics' if entity=='ga' else entity for entity in doc]
        doc= ['d2c' if entity in ['directtoconsumer','dtc'] else entity for entity in doc]
        doc = (list(set(doc)))
        d.append(",".join([str(elem) for elem in doc]))
    return d

def extract_entities_from_jobs_data(new_job_data,default_value_df):
    
    #required columns for key word generation of jobs data
    required_columns= [ 
        'Deal ID',
        'Type of Marketer',
        'Industry Experience Other',
        'Tools / Tech Experience Needed',
        'Marketer Responsibilities',
        'Industry Experience',
        'Comments for Freelancer',
        'Match Requirements',
        'Describe Project'
        ]
    jobs= new_job_data[required_columns]

    #treating null values replacing values if necessary
    fillna_value= default_value_df[default_value_df['column_name']=='Industry Experience']['value'].values[0]
    jobs['Industry Experience']= jobs['Industry Experience'].fillna(value= fillna_value)
    jobs['Industry Experience']= jobs['Industry Experience'].apply(lambda x: replace_indus_exp(x,accepted_exp()))
    #treating null values
    for i in jobs.columns:
        jobs[i] = jobs[i].fillna(" ")
    
    #getting cleaned describe project column
    jobs['Final_Describe_Project'] = jobs['Describe Project'].apply(lambda x : sentences_list(x))

    #columns required in deals data for each entity
    common_col = ['Final_Describe_Project','Comments for Freelancer','Match Requirements','Marketer Responsibilities']
    Tools_col  = common_col + ['Tools / Tech Experience Needed'] 
    Industry_col = common_col + ['Industry Experience','Industry Experience Other']
    Strategy_col = common_col
    Skills_col = common_col

    #generating text data for each entity
    Jobs_Tools_text = all_text(Tools_col,jobs)
    Jobs_Industry_text = all_text(Industry_col,jobs)
    Jobs_Strategy_text = all_text(Strategy_col,jobs)
    Jobs_Skills_text = all_text(Skills_col,jobs)
    Tools = pd.DataFrame(Jobs_Tools_text, columns = ["Jobs_Tools_text"])
    Industry = pd.DataFrame(Jobs_Industry_text, columns = ["Jobs_Industry_text"])
    Strategy = pd.DataFrame(Jobs_Strategy_text, columns = ["Jobs_Strategy_text"])
    Skills = pd.DataFrame(Jobs_Skills_text, columns = ["Jobs_Skills_text"])

    #merging each entity text with jobs data
    jobs = jobs.merge(Tools, left_index=True, right_index=True)
    jobs = jobs.merge(Industry, left_index=True, right_index=True)
    jobs = jobs.merge(Strategy, left_index=True, right_index=True)
    jobs = jobs.merge(Skills, left_index=True, right_index=True)

    #extracting job tool entities from jobs data from NER model
    Jobs_Tools_list = extracting_entities(Jobs_Tools_text,'Tool_Platform')
    Jobs_Tools = pd.DataFrame(Jobs_Tools_list,columns = ["Jobs_Tools_Platform_Entities_NER"])
    #print(Jobs_Tools.shape)
    jobs_new = jobs.merge(Jobs_Tools, left_index=True, right_index=True)
    logging.debug(jobs_new.shape)

    #extracting job industry entities from jobs data from NER model
    Jobs_Industry_list = extracting_entities(Jobs_Industry_text,'Industry')
    Jobs_Industry = pd.DataFrame(Jobs_Industry_list,columns = ["Jobs_Industry_Entities_NER"])
    #print(Jobs_Industry.shape)
    jobs_new = jobs_new.merge(Jobs_Industry, left_index=True, right_index=True)
    logging.debug(jobs_new.shape)

    #extracting job strategy entities from jobs data from NER model
    Jobs_Strategy_list = extracting_entities(Jobs_Strategy_text,'Strategy')
    Jobs_Strategy = pd.DataFrame(Jobs_Strategy_list,columns = ["Jobs_Strategy_Entities_NER"])
    #print(Jobs_Strategy.shape)
    jobs_new = jobs_new.merge(Jobs_Strategy, left_index=True, right_index=True)
    logging.debug(jobs_new.shape)

    #extracting job skills entities from jobs data from NER model
    Jobs_Skills_list = extracting_entities(Jobs_Skills_text,'Skills')
    Jobs_Skills = pd.DataFrame(Jobs_Skills_list,columns = ["Jobs_Skills_Entities_NER"])
    #print(Jobs_Skills.shape)
    jobs_new = jobs_new.merge(Jobs_Skills, left_index=True, right_index=True)
    logging.debug(jobs_new.shape)

    #selecting required columns
    required_columns= [
        'Deal ID',
        "Type of Marketer",
        "Jobs_Tools_Platform_Entities_NER",	
        "Jobs_Industry_Entities_NER",
        "Jobs_Strategy_Entities_NER",
        "Jobs_Skills_Entities_NER",
    ]
    jobs_data_entities_extracted= jobs_new[required_columns]
    jobs_data_entities_extracted['key']=1

    return jobs_data_entities_extracted

def get_FL_scoring_data(FL_scoring_data):
    #filtering on cleaned hours avail
    FL_scoring_data = FL_scoring_data[~(FL_scoring_data['Cleaned_Hours_Avail'] == 'Not Accepting New Jobs')]
    select_columns= [
        "key",
        "Email",
        "FL_Tools_Platform_Entities_NER",
        "FL_Industry_Entities_NER",
        "FL_Strategy_Entities_NER",
        "FL_Skills_Entities_NER"
    ]
    fl_entities_extracted_data= FL_scoring_data[select_columns]
    return fl_entities_extracted_data

def similarity_score(FL_col,Jobs_col):
    score = 0
    FL_tokens = FL_col.split(",")
    Jobs_tokens = Jobs_col.split(",")
    Total_Job_Tokens = len(Jobs_tokens)
    if (len(Jobs_col.strip())) > 0:
        Matching_Tokens = 0
        for i in Jobs_tokens:
            for j in FL_tokens:
                if j == i:
                    Matching_Tokens = Matching_Tokens + 1
        score =  float(Matching_Tokens/Total_Job_Tokens)
    else:
        score = 0
    return score

def get_entity_weightages():
    Tools_Weightage = 0.2
    Industry_Weightage = 0.45
    Strategy_Weightage = 0.05
    Skills_Weightage = 0.3
    return Tools_Weightage,Industry_Weightage,Strategy_Weightage,Skills_Weightage

def generate_fl_score_data_for_extracted_keyword(new_job_data,default_value_df,FL_scoring_data):
    #getting extracted entities job data
    jobs_data_entities_extracted= extract_entities_from_jobs_data(new_job_data,default_value_df)
    #getting extracted entities freelancers data
    fl_entities_extracted_data= get_FL_scoring_data(FL_scoring_data)

    #merging all the data
    mf= pd.merge(jobs_data_entities_extracted,fl_entities_extracted_data,on='key').drop('key',axis=1)
    #null treatment
    for i in mf.columns:
        mf[i]= mf[i].fillna(" ")
    
    #getting entity weightages
    Tools_Weightage,Industry_Weightage,Strategy_Weightage,Skills_Weightage= get_entity_weightages()

    #calculating similarity score for each entity corresponding to deal and freelancer data
    mf['Tools_score'] = mf.apply(lambda x: similarity_score(x['FL_Tools_Platform_Entities_NER'],x['Jobs_Tools_Platform_Entities_NER']),axis = 1)
    mf['Industry_score'] = mf.apply(lambda x: similarity_score(x['FL_Industry_Entities_NER'],x['Jobs_Industry_Entities_NER']),axis = 1)
    mf['Strategy_score'] = mf.apply(lambda x: similarity_score(x['FL_Strategy_Entities_NER'],x['Jobs_Strategy_Entities_NER']),axis = 1)
    mf['Skills_score'] = mf.apply(lambda x: similarity_score(x['FL_Skills_Entities_NER'],x['Jobs_Skills_Entities_NER']),axis = 1)

    #calculating weightage score based on strategy
    strategy = jobs_data_entities_extracted['Jobs_Strategy_Entities_NER'].iloc[0]
    len_Strategy = len(strategy.strip())
    value = 4
    if len_Strategy > 0:
        weight_value = 1
        #value = 4
    else:
        weight_value = 0.95
        #value = 3

    mf['score_keyword_search'] = (mf['Tools_score'] * Tools_Weightage) + (mf['Industry_score']*Industry_Weightage) + (mf['Strategy_score']*Strategy_Weightage) + (mf['Skills_score']*Skills_Weightage)
    mf['score_keyword_search'] = mf['score_keyword_search']/weight_value

    #calculating rank based on Final score
    mf['rank_keyword_search']= mf['score_keyword_search'].rank(method='min',ascending=False)

    #select required columns
    mf= mf[[
        'Deal ID',
        'Type of Marketer',
        'Email',
        "score_keyword_search",
        "rank_keyword_search"
        ]]
    mf= mf.sort_values(by='rank_keyword_search').reset_index(drop=True)
    
    return mf









    
    
















    