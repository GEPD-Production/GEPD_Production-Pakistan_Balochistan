*-------------------------------------------------------------------------------
* Please configure the following parameters before executing this task
*-------------------------------------------------------------------------------

* Set a number of key parameters for the GEPD country implementation
global master_seed  17893   // Ensures reproducibility

global country "PAK"
global country_name  "Pakistan - Balochistan"
global year  "2023"
global strata district location // Strata for sampling

* Execution parameters
global weights_file_name "GEPD_Balochistan_weights_200_2023-09-18" // Name of the file with the sampling
global school_code_name "bemiscode" // Name of the school code variable in the weights file
global other_info tehsil shift schoollevel // other info needed in sampling frame
*-------------------------------------------------------------------------------

