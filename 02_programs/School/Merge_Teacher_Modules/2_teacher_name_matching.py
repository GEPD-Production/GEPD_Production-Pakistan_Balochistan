#Uncomment the command below to install fuzzywuzzy on your machine
#!pip install fuzzywuzzy[speedup]


from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import time
start_time = time.time()
# Note that this script takes 4 minutes to run

#set paths

# Enter the country you are running the fuzzy match for here:
countries=["PAK_Balochistan"]
    
#Enter the same file path you have under {$clone} here:
project_folder  = "C:/Users/Hersheena/OneDrive/Desktop/Professional/WBG_GEPD_2023/GEPD Production Balochistan/"

#These paths DO NOT CHANGE
save_input_folder = project_folder + "01_GEPD_raw_data\School\Cleaned_teacher_modules"
save_output_folder = project_folder + "01_GEPD_raw_data\School\Cleaned_teacher_modules"
   

# # Define a function to calculate the similarity score between two strings
# def get_similarity_score(str1, str2):
#     return fuzz.token_set_ratio(str1, str2)

# Load the two dataframes into Python
teacher_roster = pd.read_stata(save_input_folder + "/" + "teacher_absence.dta")


# Preprocess columns
teacher_roster['school_name'] = teacher_roster['school_code'].str.lower().str.strip()
teacher_roster['teacher_name'] = teacher_roster['m2saq2'].str.lower().str.strip()
teacher_roster['teacher_unique_id'] = teacher_roster['teachers_id']

#keep just three columns
teacher_roster = teacher_roster[['school_code', 'school_name', 'teacher_name', 'teacher_unique_id', 'm2saq2', 'teachers_id']]

# create a unique id that concatenates school code and teachers id
#unique_teach_id = teacher_roster['teachers_id'].map(str) + ", " + teacher_roster['school_code']

#teacher pedagogy
teacher_pedagogy = pd.read_stata(save_input_folder + "/" + "teacher_pedagogy.dta")

teacher_pedagogy['school_name'] = teacher_pedagogy['school_code'].str.lower().str.strip()
teacher_pedagogy['teacher_name'] = teacher_pedagogy['m4saq1'].str.lower().str.strip()
teacher_pedagogy['teacher_unique_id'] = teacher_pedagogy['m4saq1_number']

#keep just three columns
teacher_pedagogy = teacher_pedagogy[['school_code', 'school_name', 'teacher_name', 'teacher_unique_id', 'm4saq1', 'm4saq1_number']]

#teacher content knowedge
teacher_content = pd.read_stata(save_input_folder + "/" + "teacher_assessment.dta")

teacher_content['school_name'] = teacher_content['school_code'].str.lower().str.strip()
teacher_content['teacher_name'] = teacher_content['m5sb_troster'].str.lower().str.strip()
teacher_content['teacher_unique_id'] = teacher_content['m5sb_tnumber']

#keep just three columns
teacher_content = teacher_content[['school_code', 'school_name', 'teacher_name', 'teacher_unique_id', 'm5sb_troster', 'm5sb_tnumber']]

#teacher questionaire
teacher_questionaire = pd.read_stata(save_input_folder + "/" + "teacher_questionnaire.dta")

teacher_questionaire['school_name'] = teacher_questionaire['school_code'].str.lower().str.strip()
teacher_questionaire['teacher_name'] = teacher_questionaire['m3sb_troster'].str.lower().str.strip()
teacher_questionaire['teacher_unique_id'] = teacher_questionaire['m3sb_tnumber']

#keep just three columns
teacher_questionaire = teacher_questionaire[['school_code', 'school_name', 'teacher_name', 'teacher_unique_id', 'm3sb_troster', 'm3sb_tnumber']]









#turn the last code into a function with two incputs: df1 and df2
def fuzzy_teacher_match(df1, df2):
    # Create an empty list to store matches
    matches = []

    # Loop over each row in df1 and find the best match in df2
    for idx1, row1 in df1.iterrows():
        best_match_score = -1
        best_match_idx = -1
        
        for idx2, row2 in df2.iterrows():
            # Calculate a fuzzy match score for school names and teacher names
            school_name_score = fuzz.ratio(row1['school_name'], row2['school_name'])
            teacher_name_score = fuzz.ratio(row1['teacher_name'], row2['teacher_name'])
            
            # if school match score is 100, and teacher match score higher than current best match score, update the best match
            if school_name_score == 100 and teacher_name_score > best_match_score:
                best_match_score = teacher_name_score
                best_match_idx = idx2
                        
        # If the best match score is above a threshold, consider the two rows a match and add them to the matches list
        if best_match_score > 80:
            matches.append((idx1, best_match_idx))
            
    # Create a new data frame to store the merged data
    merged_df = pd.DataFrame()

    # Loop over each pair of matching rows and merge the data
    for match in matches:
        merged_row = pd.concat([df1.loc[match[0]], df2.loc[match[1]].drop(['school_name', 'teacher_name', 'teacher_unique_id', 'school_code'])])
        merged_df = merged_df.append(merged_row, ignore_index=True)


    # for any unmatched teachers in df1, left join with df2 on school_code and teacher_unique_id
    df1_teachid_matched = df1[~df1.index.isin([match[0] for match in matches])]

    #drop teacher_name from df2
    df2 = df2.drop(['teacher_name', 'school_name'], axis=1)
    df1_teachid_matched = df1_teachid_matched.merge(df2, how='inner', on=['school_code', 'teacher_unique_id'])


    #append df1_teachid_matched to merged_df
    merged_df = merged_df.append(df1_teachid_matched, ignore_index=True)

    # Reset the index of merged_df to avoid duplicates
    merged_df = merged_df.reset_index(drop=True)

    #add any unmatched teachers to merged_df
    df1_unmatched = df1[~df1.index.isin([match[0] for match in matches]) & ~df1.index.isin(df1_teachid_matched.index)]
    df1_unmatched = df1_unmatched.reset_index(drop=True)

    merged_df = merged_df.append(df1_unmatched, ignore_index=True)

    #return the merged_df
    return merged_df


# call the function for the two data frames teacher_roster and teacher_pedagogy
merged_pedagogy_df = fuzzy_teacher_match(teacher_roster, teacher_pedagogy)

# call the function for the two data frames teacher_roster and teacher_content
merged_content_df = fuzzy_teacher_match(teacher_roster, teacher_content)

#call the function for the two data frames in teacher_roster and teacher_questionnaire
merged_questionnaire_df = fuzzy_teacher_match(teacher_roster, teacher_questionaire)


#join merged_pedagogy_df, merged_content, and merged_questionnaire in one file
merged_df1 = pd.merge(teacher_roster, merged_pedagogy_df, on=['school_code','school_name', 'teacher_unique_id', 'teachers_id', 'm2saq2'], how="outer")

merged_df2 = pd.merge(merged_df1, merged_content_df, on=['school_code','school_name', 'teacher_unique_id', 'teachers_id', 'm2saq2'], how="outer")

merged_df = pd.merge(merged_df2, merged_questionnaire_df, on=['school_code', 'school_name', 'teacher_unique_id', 'teachers_id', 'm2saq2'], how="outer")

merged_df = merged_df[['school_code', 'teacher_unique_id', 'm2saq2', 'teachers_id', 'm4saq1', 'm4saq1_number', 'm5sb_troster', 'm5sb_tnumber', 'm3sb_troster', 'm3sb_tnumber']]

#save merged_df as a csv
merged_df.to_stata(save_output_folder + "/" + "teacher_merged.dta", version=118)

print('\ndone')

print("Process finished --- %s seconds ---" % (time.time() - start_time))