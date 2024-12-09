clear all

*set the paths
gl data_dir ${clone}/01_GEPD_raw_data/
gl processed_dir ${clone}/03_GEPD_processed_data/


*save some useful locals
local preamble_info_individual school_code 
local preamble_info_school school_code 
local not school_code
local not1 interview__id

***************
***************
* Append files from various questionnaires
***************
***************
/*
gl dir_v7 "${data_dir}\\School\\School Survey - Version 7 - without 10 Revisited Schools\\"
gl dir_v8 "${data_dir}\\School\\School Survey - Version 8 - without 10 Revisited Schools\\"

* get the list of files
local files_v7: dir "${dir_v7}" files "*.dta"

di `files_v7'
* loop through the files and append into a single file saved in dir_saved
gl dir_saved "${data_dir}\\School\\"

foreach file of local files_v7 {
	di "`file'"
	use "${dir_v7}`file'", clear
	append using "${dir_v8}`file'", force
	save "${dir_saved}`file'", replace
}
*/

***************
***************
* School File
***************
***************

********
*read in the raw school file
********
frame create school
frame change school

use "${data_dir}\\School\\EPDashboard2.dta" 

********
*read in the school weights
********

frame create weights
frame change weights
import delimited "${data_dir}\\Sampling\\${weights_file_name}"

* rename school code
rename ${school_code_name} school_code 
clonevar  urban_rural = location

keep school_code ${strata} ${other_info} strata_prob ipw urban_rural

gen strata=" "
foreach var in $strata {
	replace strata=strata + `var' + " - "
}

destring school_code, replace force
destring ipw, replace force
duplicates drop school_code, force

******
* Merge the weights
*******
frame change school

* School Code cleaning: This has to be specific to every roll-out:

unique school_code_preload 

gen school_code=school_code_preload


* br school_code school_info_correct m1s0q2_emis if school_info_correct ==0
* replace school_code = m1s0q2_emis if school_info_correct ==0


unique school_code
replace school_code = m1s0q2_emis if school_info_correct ==0  & school_code =="BLANK INTERVIEW"
unique school_code


destring school_code, force replace

drop if missing(school_code)

frlink m:1 school_code, frame(weights)
frget ${strata} ${other_info} urban_rural strata_prob ipw strata, from(weights)

unique school_code 


*create weight variable that is standardized
gen school_weight=1/strata_prob // school level weight

*fourth grade student level weight
egen g4_stud_count = mean(m4scq4_inpt), by(school_code)



********************************************************************************
********************************************************************************

* Fixes in the data after running the modules_check:


* Comment_AR: Fix: wrongly assigned value to modules__1:
replace modules__1 = 0 if interview__id == "b1b890345b5c47279bf30530dd93a4fa"

* Comment_AR: Fix duplicates. Remove Extra m4 filled for school_codes: 2875 5275 5307 5399 where we have 2 m4 sections filled. 


foreach var in m4saq1 m4saq1_number m4scq1_infr m4scq2_infr m4scq3_infr m4scq4_inpt m4scq4n_girls m4scq5_inpt m4scq6_inpt m4scq7_inpt m4scq8_inpt m4scq9_inpt m4scq10_inpt m4scq11_inpt m4scq12_inpt m4scq13_girls m4scq14_see m4scq14_sound m4scq14_walk m4scq14_comms m4scq14_learn m4scq14_behav m4scq15_lang s1_0_1_1 s1_0_1_2 s1_0_2_1 s1_0_2_2 s1_0_3_1 s1_0_3_2 s1_a1 s1_a1_1 s1_a1_2 s1_a1_3 s1_a1_4a s1_a1_4b s1_a2 s1_a2_1 s1_a2_2 s1_a2_3 s1_b3 s1_b3_1 s1_b3_2 s1_b3_3 s1_b3_4 s1_b4 s1_b4_1 s1_b4_2 s1_b4_3 s1_b5 s1_b5_1 s1_b5_2 s1_b6 s1_b6_1 s1_b6_2 s1_b6_3 s1_c7 s1_c7_1 s1_c7_2 s1_c7_3 s1_c8 s1_c8_1 s1_c8_2 s1_c8_3 s1_c9 s1_c9_1 s1_c9_2 s1_c9_3 s2_0_1_1 s2_0_1_2 s2_0_2_1 s2_0_2_2 s2_0_3_1 s2_0_3_2 s2_a1 s2_a1_1 s2_a1_2 s2_a1_3 s2_a1_4a s2_a1_4b s2_a2 s2_a2_1 s2_a2_2 s2_a2_3 s2_b3 s2_b3_1 s2_b3_2 s2_b3_3 s2_b3_4 s2_b4 s2_b4_1 s2_b4_2 s2_b4_3 s2_b5 s2_b5_1 s2_b5_2 s2_b6 s2_b6_1 s2_b6_2 s2_b6_3 s2_c7 s2_c7_1 s2_c7_2 s2_c7_3 s2_c8 s2_c8_1 s2_c8_2 s2_c8_3 s2_c9 s2_c9_1 s2_c9_2 s2_c9_3 subject_test {
    // Check if the variable is numeric or string using ds
    ds `var', has(type numeric)
    if _rc == 0 { // If ds finds the variable is numeric
        replace `var' = . if interview__id == "5151d414826d4e3c9c95471768743c58" | interview__id == "7ba916155b8e49fbbd63e2155a479c30" | interview__id == "5b5791209adc4f02a386f9fe04800304" | interview__id == "a3c272727a01434d8d1e86cd6afef20a"
 
		
    }
    else { // Otherwise, assume it is string
        replace `var' = "" if interview__id == "5151d414826d4e3c9c95471768743c58" | interview__id == "7ba916155b8e49fbbd63e2155a479c30" | interview__id == "5b5791209adc4f02a386f9fe04800304" | interview__id == "a3c272727a01434d8d1e86cd6afef20a"

    }
}


replace modules__4 = 0 if interview__id == "5151d414826d4e3c9c95471768743c58" | interview__id == "7ba916155b8e49fbbd63e2155a479c30" | interview__id == "5b5791209adc4f02a386f9fe04800304" | interview__id == "a3c272727a01434d8d1e86cd6afef20a"

* Fix Not dropping the duplicates from the ECD and First grade datasets as we dont know which is the correct module as 2 M6 sections are filled. (for school_code 8522)

foreach var in m6_teacher_name m6_teacher_code m6_class_count m6_instruction_time m6s1q1__0 m6s1q1__1 m6s1q1__2 m6s1q1__3 m6s1q1__4 m6s1q1__5 {
    // Check if the variable is numeric or string using ds
    ds `var', has(type numeric)
    if _rc == 0 { // If ds finds the variable is numeric
        replace `var' = . if interview__id == "669619d20992428380c8722f250c332d" 
		
    }
    else { // Otherwise, assume it is string
        replace `var' = "" if interview__id == "669619d20992428380c8722f250c332d" 

    }
}


replace modules__6 = 0 if interview__id == "669619d20992428380c8722f250c332d" 


* Fixed incorrectly assigned module 8 variable as it wrongly entered causing duplicates for school_codes:  3651  & 6770
replace modules__8 = 0 if interview__id == "58486177ed0e433a84ae2eae445e3995" |  interview__id == "c01c37a3c4eb4ed5b8968d86b7956365"

* Add Missing school information to match with the teacher roster:

replace m4saq1_number =2 if school_code == 9309 & interview__id == "7eb8bf2cb87e4bfd949a602d81959cbd"
replace m4saq1 = "ehsan ali" if school_code == 9309 & interview__id == "7eb8bf2cb87e4bfd949a602d81959cbd"


* Balochistan Module Check Summary:

* Module__1: Roster is done for 199 schools. Missing for one school i.e. 8789.
* Module__4: Teacher questionnaire is filled for 195 sample schools, removing 4 duplicate for school_codes: 2875 5275 5307 5399
* Modules__6: The ECD direct assessment was filled for 197 sample schools. Removed duplicates for school_code 8522, retaining all observations for this school code in the assessment dataset. 
* Modules__8: The 4th grade assessment should be filled for 195 schools but we have data for 189 schools in the grade 4 assessment file. 

********************************************************************************


********************************************************************************
********************************************************************************


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

 
 /*
 
 *Comment_AR: tempfile 
 
 tempfile test_ar
 save `test_ar', replace
 
 */
 
***************
***************
* Teacher File
***************
***************

frame create teachers
frame change teachers
********
* Addtional Cleaning may be required here to link the various modules
* We are assuming the teacher level modules (Teacher roster, Questionnaire, Pedagogy, and Content Knowledge have already been linked here)
* See Merge_Teacher_Modules code folder for help in this task if needed
********
use "${data_dir}\\School\\Balochistan_teacher_level_revised.dta" 

/*
recode m2saq3 1=2 0=1

la def sex2 1 "Males" 2 "Females"
la val m2saq3 sex2

* fre m2saq3


foreach var in $other_info {
	cap drop `var'
}
cap drop $strata


* Comment_AR: I had drop these variables from the new Balochistan_teacher_level.dta file for the code to work:
drop school school_code urban_rural strata school_weight numEligible numEligible4th

/*
destring school_code, replace
merge m:1 school_code  using `test_ar', force
keep if _merge ==3
*/

*/


* Comment_AR: I had drop these variables from the new Balochistan_teacher_level.dta file for the code to work:
drop school school_code urban_rural strata school_weight numEligible numEligible4th district location tehsil shift schoollevel

*  g4_teacher_count g1_teacher_count teacher_abs_count teacher_abs_weight teacher_quest_count teacher_questionnaire_weight  teacher_content_count teacher_content_weight teacher_pedagogy_weight


frlink m:1 interview__key, frame(school)
frget school_code ${strata} $other_info urban_rural strata school_weight numEligible numEligible4th, from(school)

/*

* Comment_AR: Dropped school_code from the RAW file Balochistan. 

* A school gets dropped from the teacher file when its merged with school frame dataset: This is happening because observation against this school_code is only in the teachers file. Data entry mistake by the firm. This 2233 isn't in the final sample (and hence not in the school file or main file).

list _merge school_code teachers_id school_weight if school_code == 2233
*/

*get number of 4th grade teachers for weights
egen g4_teacher_count=sum(m3saq2__4), by(school_code)
egen g1_teacher_count=sum(m3saq2__1), by(school_code)



order school_code
sort school_code

*weights
*teacher absense weights
*get number of teachers checked for absense
egen teacher_abs_count=count(m2sbq6_efft), by(school_code)
gen teacher_abs_weight=numEligible/teacher_abs_count
replace teacher_abs_weight=1 if missing(teacher_abs_weight) //fix issues where no g1 teachers listed. Can happen in very small schools


*teacher questionnaire weights
*get number of teachers checked for absense
egen teacher_quest_count=count(m3s0q1), by(school_code)
gen teacher_questionnaire_weight=numEligible4th/teacher_quest_count
replace teacher_questionnaire_weight=1 if missing(teacher_questionnaire_weight) //fix issues where no g1 teachers listed. Can happen in very small schools

*teacher content knowledge weights
*get number of teachers checked for absense
egen teacher_content_count=count(m3s0q1), by(school_code)
gen teacher_content_weight=numEligible4th/teacher_content_count
replace teacher_content_weight=1 if missing(teacher_content_weight) //fix issues where no g1 teachers listed. Can happen in very small schools

*teacher pedagogy weights
gen teacher_pedagogy_weight=numEligible4th/1 // one teacher selected
replace teacher_pedagogy_weight=1 if missing(teacher_pedagogy_weight) //fix issues where no g1 teachers listed. Can happen in very small schools



unique school_code 

/*
* Comment_AR:
* Why are we doing this step?
drop if missing(school_weight) // One school code for which school weight is missing 

*/


unique school_code 

********************************************************************************
save "${processed_dir}\\School\\Confidential\\Merged\\teachers.dta" , replace
********************************************************************************

********
* Add some useful info back onto school frame for weighting
********

*collapse to school level
frame copy teachers teachers_school
frame change teachers_school

collapse g1_teacher_count g4_teacher_count, by(school_code)

frame change school
frlink m:1 school_code, frame(teachers_school)

frget g1_teacher_count g4_teacher_count, from(teachers_school)



***************
***************
* 1st Grade File
***************
***************

frame create first_grade
frame change first_grade
use "${data_dir}\\School\\ecd_assessment.dta" 



frlink m:1 interview__key interview__id, frame(school)
frget school_code ${strata} $other_info urban_rural strata school_weight m6_class_count g1_teacher_count, from(school)


order school_code
sort school_code

*weights
gen g1_class_weight=g1_teacher_count/1, // weight is the number of 1st grade streams divided by number selected (1)
replace g1_class_weight=1 if g1_class_weight<1 //fix issues where no g1 teachers listed. Can happen in very small schools

bysort school_code: gen g1_assess_count=_N
gen g1_student_weight_temp=m6_class_count/g1_assess_count // 3 students selected from the class

gen g1_stud_weight=g1_class_weight*g1_student_weight_temp

save "${processed_dir}\\School\\Confidential\\Merged\\first_grade_assessment.dta" , replace

***************
***************
* 4th Grade File
***************
***************

frame create fourth_grade
frame change fourth_grade
use "${data_dir}\\School\\fourth_grade_assessment.dta" 


frlink m:1 interview__key interview__id, frame(school)
frget school_code ${strata}  $other_info urban_rural strata school_weight m4scq4_inpt g4_teacher_count g4_stud_count, from(school)

order school_code
sort school_code

*weights
gen g4_class_weight=g4_teacher_count/1, // weight is the number of 4tg grade streams divided by number selected (1)
replace g4_class_weight=1 if g4_class_weight<1 //fix issues where no g4 teachers listed. Can happen in very small schools

bysort school_code: gen g4_assess_count=_N

gen g4_student_weight_temp=g4_stud_count/g4_assess_count // max of 25 students selected from the class

gen g4_stud_weight=g4_class_weight*g4_student_weight_temp


save "${processed_dir}\\School\\Confidential\\Merged\\fourth_grade_assessment.dta" , replace

***************
***************
* Collapse school data file to be unique at school_code level
***************
***************

frame change school

*******
* collapse to school level
*******

*drop some unneeded info
drop enumerators*

order school_code
sort school_code

* collapse to school level
ds, has(type numeric)
local numvars "`r(varlist)'"
local numvars : list numvars - not

ds, has(type string)
local stringvars "`r(varlist)'"
local stringvars : list stringvars- not






* Store variable labels:

 foreach v of var * {
	local l`v' : variable label `v'
       if `"`l`v''"' == "" {
 	local l`v' "`v'"
 	}
 }
 
 * Store value labels: 
 
label dir 
return list


local list_of_valuelables = r(names)  // specify labels you want to keep
* local list_of_valuelables =  "m7saq7 m7saq10 teacher_obs_gender"

// save the label values in labels.do file to be executed after the collapse:
label save using "${clone}/02_programs/School/Stata/labels.do", replace
// note the names of the label values for each variable that has a label value attached to it: need the variable name - value label correspodence
   local list_of_vars_w_valuelables
 * foreach var of varlist m7saq10 teacher_obs_gender m7saq7 {
   
   foreach var of varlist * {
   
   local templocal : value label `var'
   if ("`templocal'" != "") {
      local varlabel_`var' : value label `var'
      di "`var': `varlabel_`var''"
      local list_of_vars_w_valuelables "`list_of_vars_w_valuelables' `var'"
   }
}
di "`list_of_vars_w_valuelables'"


********************************************************************************
*drop labels and then reattach
label drop _all
collapse (mean) `numvars' (firstnm) `stringvars', by(school_code)

********************************************************************************

* Comment_AR: After the collpase above the variable type percision changes from byte to double 


/*
fre m1*
fre m2*
fre m3*
fre m4*
fre m5*
fre m6*
fre m7*
fre m8*
fre s1*
fre s2*
*/





// Round variables to convert them from a new variable with byte precision

local lab_issue "s1_c7_2 s1_c9_3 s1_c9_1 s1_c9 s1_c8_3 s1_c8_2 s1_c8_1 s1_c8 s1_c7_3 s1_c7_2 s1_b6_3 s1_b6_2 s1_b6_1 s1_b6 s1_b5_2 s1_b5_1 s1_b4_3 s1_b4_2 s1_b4_1 s1_b4 s1_b3_4 s1_b3_3 s1_b3_1 s1_b3 s1_a2_3 s1_a2_2 s1_a2_1 s1_a2 s1_a1_3 s1_a1_2 s1_a1_1 s1_a1 s1_0_3_2 s1_0_2_2 s1_0_2_1 s1_0_1_2 s1_0_1_1"

foreach var of local lab_issue {	
replace `var' = round(`var')
}
*/


* Redefine var labels:  
  foreach v of var * {
	label var `v' `"`l`v''"'
 }
 
// Run labels.do to redefine the label values in collapsed file
do "${clone}/02_programs/School/Stata/labels.do"
// reattach the label values
foreach var of local list_of_vars_w_valuelables {
   cap label values `var' `varlabel_`var''
}

fre s1_c7_2 s1_c9_3 s1_c9_1 s1_c9 s1_c8_3 s1_c8_2 s1_c8_1 s1_c8 s1_c7_3 s1_c7_2 s1_b6_3 s1_b6_2 s1_b6_1 s1_b6 s1_b5_2 s1_b5_1 s1_b4_3 s1_b4_2 s1_b4_1 s1_b4 s1_b3_4 s1_b3_3 s1_b3_1 s1_b3 s1_a2_3 s1_a2_2 s1_a2_1 s1_a2 s1_a1_3 s1_a1_2 s1_a1_1 s1_a1 s1_0_3_2 s1_0_2_2 s1_0_2_1 s1_0_1_2 s1_0_1_1


*Format school code:
format school_code %12.0f


*Comment_AR: This part of the code is only specific to each roll_out:
* Firm confirmed following errors in data entry in m6_teacher_code:

list m6_teacher_code if school_code == 5035

replace m6_teacher_code = 10 if school_code == 5035
replace m6_teacher_code = 3 if school_code == 9309

* check this: In this school_code 5523 there was no grade 1 teacher found in Balochistan during the time of the survey. 
* replace m6_teacher_code = 3 if school_code == 5523

save "${processed_dir}\\School\\Confidential\\Merged\\school.dta" , replace