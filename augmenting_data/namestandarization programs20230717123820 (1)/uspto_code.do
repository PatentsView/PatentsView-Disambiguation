******************************************************************************************************
** PROCEDURE 0 SPECIAL RECODE FOR USPTO NAMES
**
** Additions made for USPTO dataset by BHH  August 2006
* jb 1/15/08 -> index => strpos
**
******************************************************************************************************

replace standard_name = subinstr(standard_name,"-CONN.","",1)


** individuals have the format lastname; firstname
* replace asstype = "indiv" if index(standard_name,";")>0
replace asstype = "indiv" if strpos(standard_name,";")>0

replace standard_name = subinstr(standard_name,";"," ; ",.)


