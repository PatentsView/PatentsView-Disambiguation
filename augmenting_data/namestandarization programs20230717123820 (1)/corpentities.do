* corpentities.do
* 
* J Bessen 1/2007
* makes list of unique corporate entities
* unique by standard_name, (not stem_name)
*
*	takes 2005 compustat header (cshdr05.dta)
*		generates standard_names and stem_names
*		flags duplicate standard_names for elimination
*		matches standard_names against standard_names to find similar names
*		allows manual assignment of similar names as precursors/subsidiaries (selfassign.dta)
* output
*	- cstatnames.dta
*		stat = status code
*			= 0, eliminate
*			= 1, precursor/subsidiary
*			= 2,. successor/parent
*	- cstat.txt, unique list of standard_names for matching

*------------------
* site specific code
* -----------------
clear
global CSDIR ="C:/docume~1/HP_Owner/mydocu~1/data/pdp/freqmatch"
global NAMDIR ="C:/docume~1/HP_Owner/mydocu~1/data/pdp/freqmatch"
cap cd $CSDIR
set memory 150m
*-------------------


* clean company names -------------------------------------------------

insheet using compustat_country_codes.csv,comma clear
rename code ctrycode
drop country
rename uspto country
label var country "Headquarters country from Compustat"
sort ctrycode
save temp,replace

use cshdr05,clear
rename finc ctrycode
sort ctrycode
merge ctrycode using temp,nokeep
tab _m
drop _m
replace country = "US" if ctrycode==. | ctrycode==0
drop ctrycode
tab country

do nameonly_main
save cstatnames,replace

* eliminate duplicate standard names ----------------------------------
* 
* COMPARISON RULES
* if firstyr>= and lastyr< => eliminate
* if firstyr= and lastyr = => take one with >sales, eliminate other
* if firstyr< and lastyr<  => make precursor
* NOTE: the sales rule will eliminate pre-FASB 94 records

sort standard_name
by standard_name: egen dup=count(firstyr)
keep if dup>1
gsort standard_name lastyr firstyr -sales
by standard_name: egen maxyr = max(lastyr)
gen mintmp = firstyr if lastyr==maxyr
by standard_name: egen minyr = min(mintmp) 
drop mintmp
gen double mintmp = sales if lastyr==maxyr & minyr==firstyr
by standard_name: egen maxsls=max(mintmp)

gen stat = 0 /* eliminate */
replace stat=1 if firstyr < minyr	/* precursor */
replace stat=2 if firstyr==minyr & lastyr==maxyr & maxsls<.01+sales


keep standard_name gvkey stat firstyr lastyr
sort gvkey
save dupnames,replace

* now do self matches---------------------------------------------------
* first, output list of names for freqmatch2.pl

use cstatnames,clear
sort gvkey
merge gvkey using dupnames
drop if stat==0 | stat==1
outsheet stem_name standard_name gvkey using selfmatch.txt,replace noq noname

* RUN MANUAL MAtch
*
* perl freqmatch2.pl selfmatch.txt selfmatch.txt >self.csv
*
* onetime update using "update self.do"
* creates selfassign.dta
* 

log using cstatnames.log,text replace
use dupnames,clear
* special code
replace stat=0 if gvkey==139901 /* eliminate this one for UBS */

sort stand stat
gen gvtmp = gvkey if stat==2
by stand: egen pdpco= max(gvtmp)

keep gvkey pdpco stat
sort gvkey

merge gvkey using selfassign,replace update
tab _merge
replace stat=1 if stat==.
sort gvkey
gen dup=gvkey==gvkey[_n-1]
tab dup
keep gvkey pdpco stat
save temp,replace

use cstatnames,clear
sort gvkey
merge gvkey using temp
tab _merge
tab stat

* special code...now in corpfix.csv
*replace stat = 1 if gvkey==17346 | gvkey==101410 /* ABB */
*replace pdpco = 210418 if gvkey==17346 | gvkey==101410 /* ABB */
*replace stat = 1 if gvkey==117298 | gvkey==118653 | gvkey==121742 | gvkey==143176 /* genzyme */
*replace pdpco=12233 if gvkey==117298 | gvkey==118653 | gvkey==121742 | gvkey==143176 /* genzyme */
*replace stat=1 if gvkey==66013 /* georgia pacific */
*replace pdpco=5134 if gvkey==66013 /* georgia pacific */
*replace stat=1 if gvkey==124015 /* quantum */
*replace pdpco=8867 if gvkey==124015 /* quantum */


***drop if stat==0
replace pdpco = gvkey if pdpco==.
replace stat=2 if stat==.
label variable stat "0=ignore, 1=precursor/subsidiary, 2=successor/parent"
label variable pdpco "PDP corporate entity key"
sort pdpco
drop _merge
save cstatnames,replace

* manual fixes
clear
insheet using corpfix.csv, comma
drop comment
sort gvkey
save temp,replace

use cstatnames, clear
sort gvkey
merge gvkey using temp, replace update
tab _m
drop _merge
save cstatnames,replace

sort standard_name
keep if gvkey==pdpco
outsheet stem_name standard_name pdpco using cstat.txt,replace noq noname
log close

