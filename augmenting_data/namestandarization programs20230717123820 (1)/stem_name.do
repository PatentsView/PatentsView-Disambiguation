************************************************************************************************
** Procedure 4 CREATE STEM NAME
**
** This section creates a name with all legal entity identifiers removed
**
** Much of this problem (below) can be fixed by keeping the words separate. I am rewriting so 
** that the name begins and ends with a space - in this way, all changes of this kind can be 
** done by word. If you want to collapse the words for matching, it is best to do it at the end
** of the cleaning.  BHH   Augsut 2006
**
** This is rather crude, and can create problems. For example a firm called 
** "WINTER COATING LTD" would be changed to "WINTER ATING ", but it is only used as
** a second match, after the standard name match, and the standard name is always retained.
** Because of this problem, not all identifier types are removed, for example for Sweden we only
** remove the AB idenitier, as it is by far the most common.
************************************************************************************************
*cap prog drop stem_name
*prog def stem_name

gen stem_name = standard_name

** UNITED KINGDOM
replace stem_name = subinstr( stem_name, " LTD ", " ", 1)
replace stem_name = subinstr( stem_name, " CO LTD ", " ", 1)
replace stem_name = subinstr( stem_name, " TRADING LTD ", " ", 1)
*replace stem_name = subinstr( stem_name, " HLDGS ", " ", 1)      
replace stem_name = subinstr( stem_name, " CORP ", " ", 1)       
replace stem_name = subinstr( stem_name, " INTL ", " ", 1)       
replace stem_name = subinstr( stem_name, " INC ", " ", 1)        
replace stem_name = subinstr( stem_name, " PLC ", " ", 1)        
replace stem_name = subinstr( stem_name, " SPA ", " ", 1)        
replace stem_name = subinstr( stem_name, " CLA ", " ", 1)        
replace stem_name = subinstr( stem_name, " LLP ", " ", 1)        
replace stem_name = subinstr( stem_name, " LLC ", " ", 1)        
replace stem_name = subinstr( stem_name, " AIS ", " ", 1)        
replace stem_name = subinstr( stem_name, " INVESTMENTS ", " ", 1)
replace stem_name = subinstr( stem_name, " PARTNERSHIP ", " ", 1)
replace stem_name = subinstr( stem_name, " & CO ", " ", 1)         
replace stem_name = subinstr( stem_name, " CO ", " ", 1)         
replace stem_name = subinstr( stem_name, " COS ", " ", 1)        
replace stem_name = subinstr( stem_name, " CP ", " ", 1)         
replace stem_name = subinstr( stem_name, " LP ", " ", 1)         
replace stem_name = subinstr( stem_name, " BLSA ", " ", 1)
replace stem_name = subinstr( stem_name, " GROUP ", " ", 1)

** FRANCE
replace stem_name = subinstr( stem_name, " SA ", " ", 1)         
replace stem_name = subinstr( stem_name, " SARL ", " ", 1)         
replace stem_name = subinstr( stem_name, " SAS ", " ", 1)         
replace stem_name = subinstr( stem_name, " EURL ", " ", 1)         
replace stem_name = subinstr( stem_name, " ETCIE ", " ", 1)         
replace stem_name = subinstr( stem_name, " ET CIE ", " ", 1)
replace stem_name = subinstr( stem_name, " CIE ", " ", 1)
replace stem_name = subinstr( stem_name, " GIE ", " ", 1)         
replace stem_name = subinstr( stem_name, " SC ", " ", 1)         
replace stem_name = subinstr( stem_name, " SNC ", " ", 1)         
replace stem_name = subinstr( stem_name, " SP ", " ", 1)         
replace stem_name = subinstr( stem_name, " SCS ", " ", 1)         

** GERMANY
replace stem_name = subinstr( stem_name, " GMBHCOKG ", " ", 1)         
replace stem_name = subinstr( stem_name, " EGENOSSENSCHAFT ", " ", 1)         
replace stem_name = subinstr( stem_name, " GMBHCO ", " ", 1)         
replace stem_name = subinstr( stem_name, " COGMBH ", " ", 1)         
replace stem_name = subinstr( stem_name, " GESMBH ", " ", 1)         
replace stem_name = subinstr( stem_name, " GMBH ", " ", 1)         
replace stem_name = subinstr( stem_name, " KGAA ", " ", 1)         
replace stem_name = subinstr( stem_name, " KG ", " ", 1)         
replace stem_name = subinstr( stem_name, " AG ", " ", 1)         
replace stem_name = subinstr( stem_name, " EG ", " ", 1)         
replace stem_name = subinstr( stem_name, " GMBHCOKGAA ", " ", 1)         
replace stem_name = subinstr( stem_name, " MIT ", " ", 1)         
replace stem_name = subinstr( stem_name, " OHG ", " ", 1)         
replace stem_name = subinstr( stem_name, " GRUPPE ", " ", 1)
replace stem_name = subinstr( stem_name, " GBR ", " ", 1)

** Spain
replace stem_name = subinstr( stem_name, " SL ", " ", 1)         
replace stem_name = subinstr( stem_name, " SA ", " ", 1)         
replace stem_name = subinstr( stem_name, " SC ", " ", 1)         
replace stem_name = subinstr( stem_name, " SRL ", " ", 1)
replace stem_name = subinstr( stem_name, " ESPANA ", " ", 1)

** Italy
replace stem_name = subinstr( stem_name, " SA ", " ", 1)         
replace stem_name = subinstr( stem_name, " SAS ", " ", 1)         
replace stem_name = subinstr( stem_name, " SNC ", " ", 1)         
replace stem_name = subinstr( stem_name, " SPA ", " ", 1)
replace stem_name = subinstr( stem_name, " SRL ", " ", 1)

** SWEDEN - front and back
replace stem_name = subinstr( stem_name, " AB ", " ", 1)
replace stem_name = subinstr( stem_name, " HB ", " ", 1)
replace stem_name = subinstr( stem_name, " KB ", " ", 1)

** Belgium
** Note: the belgians use a lot of French endings, so we include all the French ones.
** Also, they use NV (belgian) and SA (french) interchangably, so standardise to SA

* French ones again
replace stem_name = subinstr( stem_name, " SAS ", " ", 1)
replace stem_name = subinstr( stem_name, " SA ", " ", 1)
replace stem_name = subinstr( stem_name, " SARL ", " ", 1)
replace stem_name = subinstr( stem_name, " SARLU ", " ", 1)
replace stem_name = subinstr( stem_name, " SAS ", " ", 1)
replace stem_name = subinstr( stem_name, " SASU ", " ", 1)
replace stem_name = subinstr( stem_name, " EURL ", " ", 1)
replace stem_name = subinstr( stem_name, " ETCIE ", " ", 1)
replace stem_name = subinstr( stem_name, " CIE ", " ", 1)
replace stem_name = subinstr( stem_name, " GIE ", " ", 1)
replace stem_name = subinstr( stem_name, " SC ", " ", 1)
replace stem_name = subinstr( stem_name, " SNC ", " ", 1)
replace stem_name = subinstr( stem_name, " SP ", " ", 1)
replace stem_name = subinstr( stem_name, " SCS ", " ", 1)

* Specifically Belgian ones
replace stem_name = subinstr( stem_name, " BV ", " ", 1)
replace stem_name = subinstr( stem_name, " CVA ", " ", 1)
replace stem_name = subinstr( stem_name, " SCA ", " ", 1)
replace stem_name = subinstr( stem_name, " SPRL ", " ", 1)

* Change to French language equivalents where appropriate
replace stem_name = subinstr( stem_name, " SCS ", " ", 1)
replace stem_name = subinstr( stem_name, " SA ", " ", 1)
replace stem_name = subinstr( stem_name, " SPRL ", " ", 1)

** Denmark - front and back
* Usually danish identifiers have a slash (eg. A/S or K/S), but these will have been removed with all
* other punctuation earlier (so just use AS or KS).
replace stem_name = subinstr( stem_name, " AMBA ", " ", 1)
replace stem_name = subinstr( stem_name, " APS ", " ", 1)
replace stem_name = subinstr( stem_name, " AS ", " ", 1)
replace stem_name = subinstr( stem_name, " IS ", " ", 1)
replace stem_name = subinstr( stem_name, " KAS ", " ", 1)
replace stem_name = subinstr( stem_name, " KS ", " ", 1)
replace stem_name = subinstr( stem_name, " PF ", " ", 1)

** Norway - front and back
replace stem_name = subinstr( stem_name, " AL ", " ", 1)
replace stem_name = subinstr( stem_name, " ANS ", " ", 1)
replace stem_name = subinstr( stem_name, " AS ", " ", 1)
replace stem_name = subinstr( stem_name, " ASA ", " ", 1)
replace stem_name = subinstr( stem_name, " DA ", " ", 1)
replace stem_name = subinstr( stem_name, " KS ", " ", 1)

** Netherlands - front and back
replace stem_name = subinstr( stem_name, " BV ", " ", 1) 
replace stem_name = subinstr( stem_name, " CV ", " ", 1)
replace stem_name = subinstr( stem_name, " CVOA ", " ", 1)
replace stem_name = subinstr( stem_name, " NV ", " ", 1)
replace stem_name = subinstr( stem_name, " VOF ", " ", 1)

** Finland - front and back
** We get some LTD and PLC strings for finland. Remove.
replace stem_name = subinstr( stem_name, " AB ", " ", 1)
replace stem_name = subinstr( stem_name, " APB ", " ", 1)
replace stem_name = subinstr( stem_name, " KB ", " ", 1)
replace stem_name = subinstr( stem_name, " KY ", " ", 1)
replace stem_name = subinstr( stem_name, " OY ", " ", 1)
replace stem_name = subinstr( stem_name, " OYJ ", " ", 1)
replace stem_name = subinstr( stem_name, " OYJ AB ", " ", 1)
replace stem_name = subinstr( stem_name, " OY AB ", " ", 1)
replace stem_name = subinstr( stem_name, " LTD ", " ", 1)
replace stem_name = subinstr( stem_name, " PLC ", " ", 1)
replace stem_name = subinstr( stem_name, " INC ", " ", 1)

** Poland
replace stem_name = subinstr( stem_name, " SA ", " ", 1) 
replace stem_name = subinstr( stem_name, " SC ", " ", 1)
replace stem_name = subinstr( stem_name, " SK ", " ", 1)
replace stem_name = subinstr( stem_name, " SPZOO ", " ", 1)

** Greece
** Also see limited and so on sometimes
replace stem_name = subinstr( stem_name, " AE ", " ", 1)
replace stem_name = subinstr( stem_name, " EE ", " ", 1)
replace stem_name = subinstr( stem_name, " EPE ", " ", 1)
replace stem_name = subinstr( stem_name, " OE ", " ", 1)
replace stem_name = subinstr( stem_name, " SA ", " ", 1)
replace stem_name = subinstr( stem_name, " LTD ", " ", 1)
replace stem_name = subinstr( stem_name, " PLC ", " ", 1)
replace stem_name = subinstr( stem_name, " INC ", " ", 1)

** Czech Republic
replace stem_name = subinstr( stem_name, " AS ", " ", 1)
replace stem_name = subinstr( stem_name, " KS ", " ", 1)
replace stem_name = subinstr( stem_name, " SRO ", " ", 1)
replace stem_name = subinstr( stem_name, " VOS ", " ", 1) 

** Bulgaria
replace stem_name = subinstr( stem_name, " AD ", " ", 1)
replace stem_name = subinstr( stem_name, " KD ", " ", 1)
replace stem_name = subinstr( stem_name, " KDA ", " ", 1)
replace stem_name = subinstr( stem_name, " OCD ", " ", 1)
replace stem_name = subinstr( stem_name, " KOOP ", " ", 1)
replace stem_name = subinstr( stem_name, " DF ", " ", 1)
replace stem_name = subinstr( stem_name, " EOOD ", " ", 1)
replace stem_name = subinstr( stem_name, " EAD ", " ", 1)
replace stem_name = subinstr( stem_name, " OOD ", " ", 1)
replace stem_name = subinstr( stem_name, " KOOD ", " ", 1)
replace stem_name = subinstr( stem_name, " ET ", " ", 1)

** Japan
replace stem_name = subinstr( stem_name, " KOGYO KK ", " ", 1)
replace stem_name = subinstr( stem_name, " KK ", " ", 1)

replace standard_name = trim(subinstr(standard_name,"  "," ",30))
