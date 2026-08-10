**************************************************************************
** Version: ??
**
** Purpose: this do-file takes the datafile currently in memory looks for
**          a variable that is passed to it as an argument and standardises
**          this variable.
**
** History: v0.1 Original file
**          v0.2 Complete re-write. Uses subinstr command instead of loops.
**               Also handles foreign countries.
**          v0.3 Make procedural and handle different countries separately.
**          v0.4 Add remaining countries
**          v0.5 Use Derwent.
**          v0.6 Change for PATSTAT
**          v0.7 Changes for USPTO     
**
** Created on: 07/06/2006
** By: Gareth Macartney
** modified August 2006 for USPTO data by Bronwyn H. Hall
* mod 1/2007 by bessen,  to use punctuation2.do and combabbrev.do, to output coname.txt
**************************************************************************
* site-specific code
*-------------------
*global CSDIR ="C:/docume~1/HP_Owner/mydocu~1/data/pdp/freqmatch"
*global NAMDIR ="C:/docume~1/HP_Owner/mydocu~1/data/pdp/freqmatch"
*cap cd $CSDIR
*cap log close
*cap log using $NAMDIR/name_coname04,t replace
global CSDIR="/data/staff/galen/tmintensity"
global NAMDIR="/data/staff/galen/tmintensity/freqmatch"
********************************************************************************************************
** MAIN
**
********************************************************************************************************
**
** Recode the CONAME04 file from the USPTO

rename aname assname
label var assname "Name of assignee"
compress
** NOTE: USPTO assignee number should eventually be replaced by PDPASS
aorder

sort assname
gen same = assname==assname[_n-1]
tab same
drop same

*	set seed 458925
gen file = "USPTO"
*	gen list = uniform()>.998
gen asstype = ""                              /*  will be used to store non-corporate type  */
gen standard_name = " "+trim(assname)+" "           /* so we can handle words at beg and end of string*/
replace standard_name=upper(standard_name)
drop if stand=="   "
*	list assname standard_name if list,clean compress noobs str(35) 

/*0*/ qui do $NAMDIR/uspto_code

/*1*/ qui do $NAMDIR/punctuation2 

/*2*/ qui do $NAMDIR/standard_name 
sort standard_name
gen same = standard_name==standard_name[_n-1]
tab same
drop same

/*3*/ 
qui do $NAMDIR/corporates
qui do $NAMDIR/non_corporates if asstype~="firm"

/* 3b */
qui do $NAMDIR/combabbrev

/*4*/ qui do $NAMDIR/stem_name
sort stem_name
duplicates tag stem_name,gen(same)
tab stem if same>20
drop same

gsort stem - asstype
replace asstype = asstype[_n-1] if asstype=="" & stem==stem[_n-1]
replace stem = trim(stem)
sort stem_name
gen same = stem_name==stem_name[_n-1] | stem_name==stem_name[_n+1]
tab same
tab asstype,missing
tab asstype if ~same,missing

*	outsheet assname asstype standard_name stem_name using $NAMDIR/coname04_std.txt,noquote nocomma replace
*outsheet stem_name standard_name assignee using $NAMDIR/coname.txt if asstype=="firm" | asstype=="",noquote nocomma replace noname
