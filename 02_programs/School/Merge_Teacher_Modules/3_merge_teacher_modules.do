/*******************************************************************************
Purpose: Merging all teacher modules (roster, pedagogy, asssessment, and questionnaire)

Last modified on: 1/30/2024
By: Hersheena Rajaram
    
*******************************************************************************/
* Install packages
ssc install matchit
ssc install freqindex

global date = c(current_date)
global username = c(username)

/*Our goal is to have a teacher level file that combines modules 1(roster), 
4 (questionnaire), 5 (assessment) and 7 (pedagogy/classroom observation).
The final data should be unique at the teacher_id - school_code level.
*/ 


global clone "C:\Users\Hersheena\OneDrive\Desktop\Professional\WBG_GEPD_2023\GEPD Production Balochistan"

* Enter the country we are looking at here: 
global cty PAK_Balochistan

/********************* Step 1: Start with roster data ***************************/
use "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_absence.dta", clear

* Dataset should be unique at teacher-id - school_code
isid teachers_id school_code
replace m2saq2=lower(m2saq2)
 
count
di "There are `r(N)' teachers in the teacher roster for `cty'"


/************************ Step 2: Merge in modules key with roster *********************/

*Import fuzzy match result, check validity and clean as much as we can
preserve

use "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_merged.dta", clear

*isid teachers_id school_code

* Create a flag for duplicates
duplicates tag teachers_id school_code, g(tag)
tab tag


foreach tag in 1 2 3 7 11 {
	* create a score system to see which observation is most complete
	gen pedagogy_case`tag'_tag=`tag' if missing(m4saq1) & missing(m4saq1_number) & tag==`tag'
		foreach v in m3 m5 {
			gen `v'_case`tag'_tag=1 if missing(`v'sb_troster) & missing(`v'sb_tnumber) & tag==`tag'
		}
	egen temp_case`tag'=rowtotal(pedagogy_case`tag'_tag m3_case`tag'_tag m5_case`tag'_tag)
	bys m2saq2 teachers_id school_code: egen tag_case`tag'=min(temp_case`tag')

	* drop observations with less data
	drop if (temp_case`tag'!=tag_case`tag') & tag==`tag'

	*drop extra vars
	drop temp_case`tag' pedagogy_case`tag'_tag m3_case`tag'_tag m5_case`tag'_tag temp_case`tag' tag_case`tag'
}


* 4 duplicates remain
/*
school_code	teacher_unique_id	m2saq2				m4saq1	m4saq1_number	m5sb_troster	m5sb_tnumber	m3sb_troster	m3sb_tnumber	
3431		1					razia																		abida			1	
3431		1					razia																		sajida			1	
3431		4					wahida																		fareeda			4	
3431		4					wahida																		feroza			4	
5307		4					bashir				Bashir Ahmed	4		Bashir Ahmad		4			Bashir ahmad	4	
5307		4					bashir				Bashir Ahmed	4		Bashir Ahmad		4			Bashir ahmad	4	
6643		3					miss noreen akhtar	3														habiba			3
6643		3					miss noreen akhtar	3														fatima			3

*/
duplicates drop teachers_id school_code if school_code=="5307" & m2saq2=="bashir", force
duplicates drop teachers_id school_code if school_code=="6643" & m2saq2=="miss noreen akhtar", force
replace m3sb_tnumber=. 	if school_code=="6643" & m2saq2=="miss noreen akhtar"
replace m3sb_troster="" 	if school_code=="6643" & m2saq2=="miss noreen akhtar"

drop if school_code=="3431" & m2saq2=="wahida" & m3sb_troster=="fareeda"

*create a flag for those that were wrong
gen flag_mismatch=0
replace flag_mismatch=1 if school_code=="3431" & m2saq2=="razia" & m3sb_troster=="sajida"
replace m3sb_tnumber=24 if school_code=="3431" & m2saq2=="razia" & m3sb_troster=="sajida"

replace flag_mismatch=1 if school_code=="3431" & m2saq2=="razia" & m3sb_troster=="abida"
replace m3sb_tnumber=22 if school_code=="3431" & m2saq2=="razia" & m3sb_troster=="abida"

replace flag_mismatch=1 if school_code=="3431" & m2saq2=="wahida" & m3sb_troster=="feroza"
replace m3sb_tnumber=13 if school_code=="3431" & m2saq2=="wahida" & m3sb_troster=="feroza"

* Now check duplicates in each modules

/********************** Module 4 - Pedagogy ***************************/
duplicates tag m4saq1_number m4saq1 school_code, g(module4)
replace module4=0 if missing(m4saq1_number) & missing(m4saq1)
tab module4																		//9 dups


* Through the fuzzy match, some teacher names were matched from module 4 to the wrong names in absences/roster
*Create a match score between the name in roster and the name in pedagogy
*Replace module 4 variables to missing for the obs with the least match score
gen m4saq1_lwr=lower(m4saq1)
matchit m2saq2 m4saq1_lwr
bys school_code: egen max_matchscore=max(similscore)
foreach v in similscore max_matchscore {
    replace `v'  =round(`v' , 0.001)
	*tostring `v', replace force
}

replace m4saq1="" if similscore<max_matchscore & similscore!=0
replace m4saq1_number=. if similscore<max_matchscore & similscore!=0


* Drop if similscore is 0 and teacher name in module 3 and roster are not missing
drop if similscore==0 & !missing(m2saq2) & !missing(m4saq1_lwr) & module4!=0

* Check duplicates again
drop module4 similscore max_matchscore
duplicates tag m4saq1_number m4saq1 school_code, g(module4)
replace module4=. if missing(m4saq1_number) & missing(m4saq1)
tab module4																		//2 duplicates
 
*Manually clean them
*school code 2233, muhammad azum, m4saq1_number==1
*school code 6484, muhammad anif, id 3
gen flag_m4=(teachers_id==m4saq1_number)
replace m4saq1="" if flag_m4==0 & module4==1
replace m4saq1_number=. if flag_m4==0 & module4==1

drop flag_m4 module4

****************************** Module 3 - Questionnaire *************************
duplicates tag m3sb_tnumber m3sb_troster school_code, g(module3)
replace module3=0 if missing(m3sb_tnumber) & missing(m3sb_troster)
tab module3			
																				//138 dups
*Create a match score between the name in roster and the name in questionnaire
*Replace module 3 variables to missing for the obs with the least match score
gen m3_lwr=lower(m3sb_troster)
matchit m2saq2 m3_lwr
bys school_code: egen max_matchscore=max(similscore)
foreach v in similscore max_matchscore {
    replace `v'  =round(`v' , 0.001)
	*tostring `v', replace force
}

replace m3sb_troster="" if similscore<max_matchscore & similscore!=0 & module3!=0
replace m3sb_tnumber=. if similscore<max_matchscore & similscore!=0 & module3!=0

* Drop if similscore is 0 and teacher name in module 3 and roster are not missing
drop if similscore==0 & !missing(m2saq2) & !missing(m3_lwr) & module3!=0

drop module3 similscore max_matchscore
duplicates tag m3sb_tnumber m3sb_troster school_code, g(module3)
replace module3=0 if missing(m3sb_tnumber) & missing(m3sb_troster)
tab module3																		//6 dups

* Duplicated names but different IDs - replace m3sb_tnumber and m3sb_troster to missing
gen flag_dup_names=0
replace flag_dup_names=1 if teachers_id==m3sb_tnumber & module3!=0
replace m3sb_tnumber=. if flag_dup_names==1
replace m3sb_troster="" if flag_dup_names==1
drop flag_dup_names module3

************************* Module 5 - Assessment **********************************
duplicates tag m5sb_tnumber m5sb_troster school_code, g(module5)
replace module5=0 if missing(m5sb_tnumber) & missing(m5sb_troster)
tab module5																		//9 dups

*Create a match score between the name in roster and the name in assessment
*Replace module 5 variables to missing for the obs with the least match score
gen m5_lwr=lower(m5sb_troster)
matchit m2saq2 m5sb_troster
bys school_code: egen max_matchscore=max(similscore)
foreach v in similscore max_matchscore {
    replace `v'  =round(`v' , 0.001)
	*tostring `v', replace force
}

replace m5sb_troster="" if similscore<max_matchscore & similscore!=0 & module5!=0
replace m5sb_tnumber=. if similscore<max_matchscore & similscore!=0 & module5!=0


* Drop if similscore is 0 and teacher name in module 5 and roster are not missing
drop if similscore==0 & !missing(m2saq2) & !missing(m5_lwr) & module5!=0


drop module5
duplicates tag m5sb_tnumber m5sb_troster school_code, g(module5)
replace module5=0 if missing(m5sb_tnumber) & missing(m5sb_troster)
tab module5																		//4 dups


* Duplicated names but different IDs - replace m5sb_tnumber and m5sb_troster to missing
gen flag_dup_names=0
replace flag_dup_names=1 if teachers_id==m5sb_tnumber & module5!=0
replace m5sb_tnumber=. if flag_dup_names==1
replace m5sb_troster="" if flag_dup_names==1
drop flag_dup_names module5

save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\teacher_merged_clean.dta", replace

restore

*Save flagged duplicates/mismatches
preserve 
use "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\teacher_merged_clean.dta", clear

keep if flag_mismatch==1
drop flag_mismatch teachers_id

gen m3 = m3sb_tnumber
rename m3 teachers_id
isid teachers_id school_code

save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\teacher_merged_mismatches.dta", replace
restore

*Merge back mismatches in modules key data
preserve 

use "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\teacher_merged_clean.dta", replace

drop if flag_mismatch==1
isid teachers_id school_code
merge 1:1 teachers_id school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\teacher_merged_mismatches.dta"
drop _merge
isid teachers_id school_code

save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_merged_clean.dta", replace
restore

************************ MERGE ROSTER WITH MODULES KEY NOW ********************
/* Master is roster and is unique at teachers_id - school_code level
   Using is the modules key data - unique at teachers id and hashed_school_code
   Merge 1:m on teachers_id and hashed_school_code
*/


* Merge modules key and roster
merge 1:1 teachers_id school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_merged_clean.dta"
drop _merge
isid teachers_id school_code

/******************** Step 2: Merge in pedagogy data ******************************/

/* Merge pedagogy data
Master is unique at teachers_id and school_code but not at m4saq1_number and school_code
Using is unique at m4saq1_number and school_code
We want to merge pedagogy on the m4saq1_number and school_code from the modules key
*/

merge m:1 m4saq1_number m4saq1 school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_pedagogy.dta", gen(merge1)
* 145 matches out of 198

*save obs that were matched
preserve 

keep if merge1==3
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_matches.dta", replace 

restore
 
* 53 obs unmatched. Most of them are because the ID or name in module 4 does not match the roster.
* We will try to split teacher names into first names and then match on teachers_id school_code and first name.
split m4saq1
split m2saq2

* There are some common names in PAK where merging on firstname might not work
*save obs that were unmatched
preserve

keep if merge1==2
replace m4saq11=m4saq12 if m4saq11=="Miss"
replace m4saq11=m4saq12 if m4saq11=="M"|m4saq11=="Mohammad"|m4saq11=="Muhammad"|m4saq11=="muhmmad"|m2saq21=="abdul"
rename m4saq11 first_name
rename m4saq12 second_name
replace first_name=lower(first_name)
replace second_name=lower(second_name)

*drop teachers_id
*gen teachers_id=m4saq1_number

isid first_name school_code
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_first_name_match.dta", replace 

restore

* Rename m2saq2 to first name
replace m2saq21=m2saq22 if m2saq21=="m"|m2saq21=="mohammad"|m2saq21=="muhammad"|m2saq21=="muhmmad"|m2saq21=="abdul"|m2saq21=="Miss"
rename m2saq21 first_name
rename m2saq22 second_name
replace second_name=lower(second_name)

*Drop obs to be be merged and merge it back in
drop if merge1==2
drop if merge1==3

merge m:1 first_name school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_first_name_match.dta", ///
			update gen(merge2)
* 28 more matches



* try a match on just m4saq1_number and teachers_id and school_code
gen flag_merge2=(merge2==2)
replace flag_merge2=0 if missing(m4saq1_number)

preserve
keep if flag_merge2==1

drop teachers_id
gen teachers_id= m4saq1_number

isid teachers_id school_code
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_id_match.dta", replace 
 
restore

*Drop obs to be be merged and merge it back in
drop if flag_merge2==1
merge m:1 teachers_id school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_id_match.dta", ///
			update gen(merge3)
* 21 more matches

*add perfect matches from merge 1
append using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_matches.dta" 
*199 obs

* export names that were matched
preserve
keep if merge3==5
keep school_code teachers_id m2saq2 m4saq1 m4saq1_number
export excel using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\m4_matched.xlsx", sheetreplace firstrow(variables) 
restore

*export names that did not match
preserve
keep if merge3==2
keep school_code teachers_id m2saq2 m4saq1 m4saq1_number
export excel using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\m4_unmatched.xlsx", sheetreplace firstrow(variables) 
restore

			
*How many matches do we have? 
gen pedagogy_match=(merge1==3|merge2==5|merge3==5)


drop merge* m2saq22 m2saq23 m2saq24 m2saq25 m2saq21 m4saq11 m4saq12 m4saq13 m4saq14 first_name
/************************* Step 3: Merge in m3(questionnaire) data **************************/
/* Master is unique at teacher id and interview key BUT not unique at m3sbt_number and school_code
Using is unique at m3sb_tnumber- m3sb_troster - interview key
DO a m:1 merge in m3sb_tnumber m3sb_troster school_code
*/
merge m:1 m3sb_troster m3sb_tnumber school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_questionnaire.dta", gen(merge1)

*save obs that were matched
preserve 

keep if merge1==3
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_matches.dta", replace 

restore

* 604 matches out of 642
* 40 obs unmatched. Most of them are because the ID in module 3 does not match the roster.
* We will try to split teacher names into first names and then match on school_code and first name.
split m3sb_troster
split m2saq2

* There are some common names in PAK where merging on firstname might not work
*save obs that were unmatched
preserve

keep if merge1==2
replace m3sb_troster1=m3sb_troster2 if m3sb_troster1=="Miss"
replace m3sb_troster1=m3sb_troster2 if m3sb_troster1=="M"|m3sb_troster1=="Mohammad"|m3sb_troster1=="Muhammad"|m3sb_troster1=="muhmmad"
rename m3sb_troster1 first_name
replace first_name=lower(first_name)
isid first_name school_code
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\questionnaire_first_name_match.dta", replace 

restore

* Rename m2saq2 to first name
replace m2saq21=m2saq22 if m2saq21=="m"|m2saq21=="mohammad"|m2saq21=="muhammad"|m2saq21=="muhmmad"|m2saq21=="Miss"
rename m2saq21 first_name

*Drop pbs to be be merged and merge it back in
drop if merge1==2|merge1==3
merge m:1 first_name school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\questionnaire_first_name_match.dta", ///
			update gen(merge2)

*23 more merges. Total merge = 604+23 = 627
* export names that were matched
preserve
keep if merge2==5
keep school_code teachers_id m2saq2 m2saq21 m3sb_tnumber m3sb_troster m3sb_troster1
export excel using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\m3_matched.xlsx", sheetreplace firstrow(variables) 
restore

*export names that did not match
preserve
keep if merge2==2
keep school_code teachers_id m2saq2 m2saq21 m3sb_tnumber m3sb_troster m3sb_troster1
export excel using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\m3_unmatched.xlsx", sheetreplace firstrow(variables) 
restore

* Final match for mismatches- flag them first
cap drop flag_mismatch
gen flag_mismatch=0
replace flag_mismatch=1 if (school_code=="2869"   & m3sb_troster=="Dawood Shah" 	  & m3sb_tnumber==2  & missing(teachers_id) & missing(m2saq2))|	///
							(school_code=="3431"  & m3sb_troster=="feroza"			  & m3sb_tnumber==4  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="6643"  & m3sb_troster=="Ganj malik"		  & m3sb_tnumber==2  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="9261"  & m3sb_troster=="gulali"            & m3sb_tnumber==15 & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="1824"  & m3sb_troster=="Gulbashra"         & m3sb_tnumber==1  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="9489"  & m3sb_troster=="HAMEEDA RAFIQ"     & m3sb_tnumber==4  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="2869"  & m3sb_troster=="Hedayt ullah"      & m3sb_tnumber==1  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="5923"  & m3sb_troster=="m Naeem"           & m3sb_tnumber==9  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="12729" & m3sb_troster=="marhem sadiq"      & m3sb_tnumber==6  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="7615"  & m3sb_troster=="rasol bux"         & m3sb_tnumber==11 & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="3247"  & m3sb_troster=="Miss rubina batol" & m3sb_tnumber==4  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="2875"  & m3sb_troster=="shaheena muneer"   & m3sb_tnumber==5  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="9269"  & m3sb_troster=="Shahida hassan"    & m3sb_tnumber==15 & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="5433"  & m3sb_troster=="zaffar ullha"	  & m3sb_tnumber==4  & missing(teachers_id) & missing(m2saq2))| ///
							(school_code=="9250"  & m3sb_troster=="ZAHID ASHRAF"	  & m3sb_tnumber==2  & missing(teachers_id) & missing(m2saq2))|	///
							(school_code=="5306"  & m3sb_troster=="Ayesha" 			  & m3sb_tnumber==4  & missing(teachers_id) & missing(m2saq2))|	///
							(school_code=="6274"  & m3sb_troster=="A.Razzaq" 		  & m3sb_tnumber==20 & missing(teachers_id) & missing(m2saq2))|	///
							(school_code=="6613"  & m3sb_troster=="M Ranzan" 		  & m3sb_tnumber==3  & missing(teachers_id) & missing(m2saq2))|	///
							(school_code=="7997"  & m3sb_troster=="shumaira basharat" & m3sb_tnumber==8  & missing(teachers_id) & missing(m2saq2))
							

* All obs flagged are perfect duplicates in terms of teacher id and school_code
duplicates drop teachers_id school_code if flag_mismatch==1, force

preserve

keep if flag_mismatch==1
replace teachers_id=18 if school_code=="2869" & missing(teachers_id) & m3sb_troster=="Dawood Shah" & m3sb_tnumber==2
replace teachers_id=13 if school_code=="3431" & missing(teachers_id) & m3sb_troster=="feroza" & m3sb_tnumber==4
replace teachers_id=17 if school_code=="6643" & missing(teachers_id) & m3sb_troster=="Ganj malik" & m3sb_tnumber==2
replace teachers_id=16 if school_code=="9261" & missing(teachers_id) & m3sb_troster=="gulali" & m3sb_tnumber==15
replace teachers_id=17 if school_code=="1824" & missing(teachers_id) & m3sb_troster=="Gulbashra" & m3sb_tnumber==1
replace teachers_id=24 if school_code=="9489" & missing(teachers_id) & m3sb_troster=="HAMEEDA RAFIQ" & m3sb_tnumber==4
replace teachers_id=17 if school_code=="2869" & missing(teachers_id) & m3sb_troster=="Hedayt ullah" & m3sb_tnumber==1
replace teachers_id=11 if school_code=="5923" & missing(teachers_id) & m3sb_troster=="m Naeem" & m3sb_tnumber==9
replace teachers_id=7 if school_code=="12729" & missing(teachers_id) & m3sb_troster=="marhem sadiq" & m3sb_tnumber==6
replace teachers_id=1 if school_code=="7615" & missing(teachers_id) & m3sb_troster=="rasol bux" & m3sb_tnumber==11
replace teachers_id=23 if school_code=="3247" & missing(teachers_id) & m3sb_troster=="Miss rubina batol" & m3sb_tnumber==4
replace teachers_id=8 if school_code=="2875" & missing(teachers_id) & m3sb_troster=="shaheena muneer" & m3sb_tnumber==5
replace teachers_id=5 if school_code=="9269" & missing(teachers_id) & m3sb_troster=="Shahida hassan" & m3sb_tnumber==15
replace teachers_id=2 if school_code=="5433" & missing(teachers_id) & m3sb_troster=="zaffar ullha" & m3sb_tnumber==4
replace teachers_id=11 if school_code=="9250" & missing(teachers_id) & m3sb_troster=="ZAHID ASHRAF" & m3sb_tnumber==2

replace teachers_id=8  if school_code=="6274" & missing(teachers_id) & m3sb_troster=="A.Razzaq" & m3sb_tnumber==20
replace teachers_id=24 if school_code=="5306" & missing(teachers_id) & m3sb_troster=="Ayesha" & m3sb_tnumber==4
replace teachers_id=5 if school_code=="6613" & missing(teachers_id) & m3sb_troster=="M Ranzan" & m3sb_tnumber==3
replace teachers_id=22 if school_code=="7997" & missing(teachers_id) & m3sb_troster=="shumaira basharat" & m3sb_tnumber==8

isid teachers_id school_code
save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\questionnaire_unmatched.dta", replace 

restore

*Drop pbs to be be merged and merge it back in
drop if flag_mismatch==1
merge m:1 teachers_id school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\questionnaire_unmatched.dta", ///
			update gen(merge3)

			
*Adds obs that were matched in obs 1
append using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\pedagogy_matches.dta" 

*How many matches do we have? 
gen questionnaire_match=(merge1==3|merge2==5|merge3==5)
*640 matches out of 642 - 99% match

/**************************** Step 6: Merge in m5(assessment) data ************************/

/* Master is not unique at m5sb_tnumber and school_code
Using is unique at m5sb_tnumber and school_code
Do a m:1 merge on m5sb_tnumber m5sb_troster school_code
*/

drop merge*
merge m:1 m5sb_troster m5sb_tnumber school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\teacher_assessment.dta", gen(merge1)
*615 out of 635 matches

/* there are 20 observations from assessment only. A quick check shows that the 
m5sb_tnumber is missing from modules key while these teachers had m5sb_tnumber 
in assessment data.Save a temp data with observations 
with _merge==2 and merge on teachers_id - school_code instead
*/
preserve
keep if merge1==2
drop teachers_id
gen m5_id=m5sb_tnumber
rename m5_id teachers_id

isid teachers_id school_code

save "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\assessment_temp.dta", replace
restore

* merge on teachers_id and school code
drop if merge1==2
merge m:1 teachers_id school_code using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\assessment_temp.dta", update gen(merge2)

*20 more merges. Now a total of 635 matches.
* export names that were matched
preserve
keep if merge2==5
keep school_code teachers_id m2saq2 m5sb_tnumber m5sb_troster
export excel using "${clone}\01_GEPD_raw_data\School\Cleaned_teacher_modules\temp\m5_matched.xlsx", sheetreplace firstrow(variables) 
restore


*How many matches do we have? 
gen assessment_match=(merge1==3|merge2==5)
*635 matches out of 635- 100%

* Check on duplicates
duplicates tag teachers_id school_code, g(tag_dup_final)
tab tag_dup_final

*There are some perfect duplicates in terms of m3sb_tnumber m3sb_troster and school_code
duplicates drop m3sb_tnumber m3sb_troster school_code if tag_dup_final!=0, force

* drop temp/unecessary vars 
drop tag_dup_final merge*

*drop 1 duplicates
/*
teachers_id	m2saq2	school_code	m4saq1	m4saq1_number
4	kamran khan	5399	kamran khan	4
4		5399	Kamran Khan	4

*/
drop if school_code=="5399" & teachers_id==4 & missing(m2saq2) 

* label variables 
do "${clone}/02_programs/School/Merge_Teacher_Modules/zz_label_all_variables.do"


sort school_code teachers_id
rename school_code school_code

* Save final data
save "${clone}\01_GEPD_raw_data\School\Balochistan_teacher_level.dta", replace


