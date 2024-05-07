/*******************************************************************************
Purpose: Cleaning all variables in raw data 

Last modified on: 1/30/2024
By: 
    
*******************************************************************************/

clear all
set more off
*macro drop _all
cap log close
program drop _all
matrix drop _all
*set trace on
*set tracedepth 1

global date = c(current_date)
global username = c(username)

*global clone "C:\Users\Hersheena\OneDrive\Desktop\Professional\WBG_GEPD_2023\GEPD Production Balochistan"

/*Our goal is to clean all variables in all modules before matching teacher names across modules */ 

* Enter the country we are looking at here: PAK_Balochistan

********
*read in the raw school file
********
*set the paths
gl data_dir ${clone}/01_GEPD_raw_data/
gl processed_dir ${clone}/03_GEPD_processed_data/

global strata district location // Strata for sampling

* Execution parameters
global weights_file_name "GEPD_Balochistan_weights_200_2023-09-18" // Name of the file with the sampling
global school_code_name "bemiscode" //"ïbemiscode" // // Name of the school code variable in the weights file
global other_info tehsil shift schoollevel // other info needed in sampling frame
*-------------------------------------------------------------------------------

*save some useful locals
local preamble_info_individual school_code 
local preamble_info_school school_code 
local not school_code
local not1 interview__id


frame create school
frame change school

use "${data_dir}\\School\\EPDashboard2.dta" 

********
*read in the school weights
********

frame create weights
frame change weights
import delimited "${data_dir}/Sampling/${weights_file_name}"

* rename school code
rename ${school_code_name} school_code 


keep school_code ${strata} ${other_info} strata_prob ipw 

gen strata=" "
foreach var in $strata {
	replace strata=strata + `var' + " - "
}

gen urban_rural=location

destring school_code, replace force
destring ipw, replace force
duplicates drop school_code, force

******
* Merge the weights
*******
frame change school

gen school_code = school_emis_preload
*fix missing cases
replace school_code = m1s0q2_emis if school_info_correct==0

destring school_code, force replace

drop if missing(school_code)

frlink m:1 school_code, frame(weights)
frget ${strata} ${other_info} urban_rural strata_prob ipw strata, from(weights)



* Firm confirmed this change:
replace school_code = 2871 if school_code ==2875


*create weight variable that is standardized
gen school_weight=strata_prob // school level weight

*fourth grade student level weight
egen g4_stud_count = mean(m4scq4_inpt), by(school_code)


*create collapsed school file as a temp
frame copy school school_collapse_temp
frame change school_collapse_temp

order school_code
sort school_code

* collapse to school level
ds, has(type numeric)
local numvars "`r(varlist)'"
local numvars : list numvars - not

ds, has(type string)
local stringvars "`r(varlist)'"
local stringvars : list stringvars- not

 foreach v of var * {
	local l`v' : variable label `v'
       if `"`l`v''"' == "" {
 	local l`v' "`v'"
 	}
 }

collapse (max) `numvars' (firstnm) `stringvars', by(school_code)

 foreach v of var * {
	label var `v' `"`l`v''"'
 }



* Now, we start cleaning our datasets

/*********************** Step 1: Start with roster data ************************/
use "${clone}/01_GEPD_raw_data/School/TEACHERS.dta", replace

frlink m:1 interview__key, frame(school)
frget school_code ${strata} $other_info urban_rural strata school_weight numEligible numEligible4th, from(school)


* Dataset should be unique at teacher-id - interview__key level
rename TEACHERS__id teachers_id
isid teachers_id interview__key 													
replace m2saq2=lower(m2saq2)

* firm confirmed, the correct name in school code 3380, id 3 is Shaista Barkat
replace m2saq2="Shaista Barkat" if school_code==3380 & teachers_id==3


* Run do file with all value labels
do "${clone}/02_programs/School/Merge_Teacher_Modules/z_value_labels.do"
 
* Sex - Recode sex variable as 1 for female and 0 for male
recode m2saq3 2=1 1=0
tab m2saq3
* label values
label define sex 0 "Male" 1 "Female", modify
label val m2saq3 sex 

* Contract status
tab m2saq5
tab m2saq5_other

* Full time status
* Recode part time to 0
recode m2saq6 2=0
label val m2saq6 fulltime

* Destring urban rural variable and recode
cap rename rural urban_rural 
*if urban_rural is string
cap replace urban_rural ="1" if urban_rural =="Rural"
cap replace urban_rural ="0" if urban_rural =="Urban"
cap destring urban_rural, replace

* if urban_rural if numeric
cap recode urban_rural 2=0
cap la val urban_rural rural

gen in_roster=1

tostring school_code, replace

* Save file
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_absence.dta", replace
 
 
/************************** Step 2: Clean pedagogy data ****************************/
use "${clone}/01_GEPD_raw_data/School/EPDashboard2.dta", clear
count

* Drop if missing m4saq1_number and m4saq1 - we only want obs in the pedagogy module
drop if missing(m4saq1_number) & missing(m4saq1)
order m4saq1 m4saq1_number interview__key interview__id school_name_preload school_address_preload school_province_preload school_district_preload school_code_preload school_emis_preload 

frlink m:1 interview__key, frame(school)
frget school_code ${strata} $other_info urban_rural strata school_weight, from(school)


* Destring urban rural variable and recode
cap rename rural urban_rural 
*if urban_rural is string
cap replace urban_rural ="1" if urban_rural =="Rural"
cap replace urban_rural ="0" if urban_rural =="Urban"
cap destring urban_rural, replace

* if urban_rural if numeric
cap recode urban_rural 2=0
cap la val urban_rural rural


* Label variables 
cap la var m4scq4_inpt "How many pupils are in the room?" 
cap la var m4scq4n_girls "How many of them are boys?"
cap la var m4scq5_inpt "How many total pupils have the textbook for class?"
cap la var m4scq6_inpt "How many pupils have pencil/pen?" 
cap la var m4scq7_inpt "How many pupils have an exercise book?"
cap la var m4scq11_inpt "How many pupils were not sitting on desks?"
cap la var m4scq12_inpt "How many students in class as per class list?"

gen in_pedagogy=1

tostring school_code, replace

*There is 1 duplicate Bashir Ahmed, id 4-looks like a perfect duplicates
duplicates drop m4saq1 school_code, force

* Comment_AR: checked not dropping 5523.

* Save file
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_pedagogy.dta", replace

/*************************** Step 3: m3(questionnaire) data **************************/

use "${clone}/01_GEPD_raw_data/School/questionnaire_roster.dta", clear
* Teacher name is m3sb_troster and teacher id is m3sb_tnumber
* Data should be unique at m3sb_tnumber- interview key

frlink m:1 interview__key, frame(school)
frget school_code ${strata} $other_info urban_rural strata school_weight numEligible numEligible4th, from(school)

*isid interview__key m3sb_tnumber
duplicates tag m3sb_tnumber interview__key, g(tag)
tab tag
br if tag ==1
sort m3sb_tnumber interview__key
drop tag
* There are duplicates here

*Age - there are some outliers here - this removes one obs of 0 and 4 obs that was above 300
sum m3saq6,d	
winsor m3saq6, g(m3saq6_w) p(0.006)
drop m3saq6
rename m3saq6_w m3saq6
	
* Destring urban rural variable and recode
cap rename rural urban_rural 
*if urban_rural is string
cap replace urban_rural ="1" if urban_rural =="Rural"
cap replace urban_rural ="0" if urban_rural =="Urban"
cap destring urban_rural, replace

* if urban_rural if numeric
cap recode urban_rural 2=0
cap la val urban_rural rural

gen in_questionnaire=1

tostring school_code, replace

* Save file
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_questionnaire.dta", replace

/************************ Step 4: Merge in m5(assessment) data *************************/
use "${clone}/01_GEPD_raw_data/School/teacher_assessment_answers.dta", clear

frlink m:1 interview__key, frame(school)
frget school_code ${strata} $other_info urban_rural strata school_weight numEligible numEligible4th, from(school)

* Data should be unique at m5sb_tnumber - interview key

* Rename m5sb_tnumber
rename m5sb_tnum m5sb_tnumber

* Destring urban rural variable and recode
cap rename rural urban_rural 
*if urban_rural is string
cap replace urban_rural ="1" if urban_rural =="Rural"
cap replace urban_rural ="0" if urban_rural =="Urban"
cap destring urban_rural, replace

* if urban_rural if numeric
cap recode urban_rural 2=0
cap la val urban_rural rural

gen in_assessment=1

tostring school_code, replace

* Save file
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_assessment.dta", replace

/************* Run python script for fuzzy matching ****************************/
python script "${clone}/02_programs/School/Merge_Teacher_Modules/2_teacher_name_matching.py"
disp "End of teacher name matching"


