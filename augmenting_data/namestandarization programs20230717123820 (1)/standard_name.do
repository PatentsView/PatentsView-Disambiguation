******************************************************************************************************
** PROCEDURE 2 CREATE STANDARD NAME
**
** This section standardises the way in which the type of legal entity is idenitified. For example
** LIMITED is converted to LTD. These identifiers are country specific and, therefore, this is 
** section is organised by country.
** It does this by, first, using the Derwent system and then doing some of our own country specific
** changes that are important. The specific changes come from sources such as Dinesh's code, Bronwyn's code
** and other files we have for Germany, France and Sweden from various other people.
** Additions made for USPTO dataset by BHH  August 2006
**
******************************************************************************************************
*cap prog drop standard_name
*prog def standard_name

** 1) Call Derwent code

do $NAMDIR/derwent_standardisation_BHH

** 2) Perform some additional changes
replace standard_name = subinstr( standard_name, " RES & DEV ", " R&D ", 1)
replace standard_name = subinstr( standard_name, " RECH & DEV ", " R&D ", 1)

** 3) Perform some country specific work
** UNITED STATES (most of this is in Derwent)

** UNITED KINGDOM
replace standard_name = subinstr( standard_name, " PUBLIC LIMITED ", " PLC ", 1)
replace standard_name = subinstr( standard_name, " PUBLIC LIABILITY COMPANY ", " PLC ", 1)
replace standard_name = subinstr( standard_name, " HOLDINGS ", " HLDGS ", 1)
replace standard_name = subinstr( standard_name, " HOLDING ", " HLDGS ", 1)
replace standard_name = subinstr( standard_name, " GREAT BRITAIN ", " GB ", 1)
replace standard_name = subinstr( standard_name, " LTD CO ", " CO LTD ", 1)

** SPANISH
replace standard_name = subinstr( standard_name, " SOC LIMITADA ", " SL ", 1)
replace standard_name = subinstr( standard_name, " SOC EN COMMANDITA ", " SC ", 1)
replace standard_name = subinstr( standard_name, " & CIA ", " CO ", 1)

** ITALIAN
replace standard_name = subinstr( standard_name, " SOC IN ACCOMANDITA PER AZIONI ", " SA ", 1)
replace standard_name = subinstr( standard_name, " SAPA ", " SA ", 1)
replace standard_name = subinstr( standard_name, " SOC A RESPONSABILITÀ LIMITATA ", " SRL ", 1)

** SWEDISH
replace standard_name = subinstr( standard_name, " HANDELSBOLAG ", " HB  ", 1)

** GERMAN
replace standard_name = subinstr( standard_name, " KOMANDIT GESELLSCHAFT ", " KG ", 1)
replace standard_name = subinstr( standard_name, " KOMANDITGESELLSCHAFT ", " KG ", 1)
replace standard_name = subinstr( standard_name, " EINGETRAGENE GENOSSENSCHAFT ", " EG ", 1)
replace standard_name = subinstr( standard_name, " GENOSSENSCHAFT ", " EG ", 1)
replace standard_name = subinstr( standard_name, " GESELLSCHAFT M B H ", " GMBH ", 1)
replace standard_name = subinstr( standard_name, " OFFENE HANDELS GESELLSCHAFT ", " OHG ", 1)
replace standard_name = subinstr( standard_name, " GESMBH ", " GMBH ", 1)
replace standard_name = subinstr( standard_name, " GESELLSCHAFT BURGERLICHEN RECHTS ", " GBR ", 1)
replace standard_name = subinstr( standard_name, " GESELLSCHAFT ", " GMBH ", 1)
* The following is common format. If conflict assume GMBH & CO KG over GMBH & CO OHG as more common.
replace standard_name = subinstr( standard_name, " GMBH CO KG ", " GMBH & CO KG ", 1)
replace standard_name = subinstr( standard_name, " GMBH COKG ", " GMBH & CO KG ", 1)
replace standard_name = subinstr( standard_name, " GMBH U CO KG ", " GMBH & CO KG ", 1)
replace standard_name = subinstr( standard_name, " GMBH U COKG ", " GMBH & CO KG ", 1)
replace standard_name = subinstr( standard_name, " GMBH U CO ", " GMBH & CO KG ", 1)
replace standard_name = subinstr( standard_name, " GMBH CO ", " GMBH & CO KG ", 1)
replace standard_name = subinstr( standard_name, " AG CO KG ", " AG & CO KG ", 1)
replace standard_name = subinstr( standard_name, " AG COKG ", " AG & CO KG ", 1)
replace standard_name = subinstr( standard_name, " AG U CO KG ", " AG & CO KG ", 1)
replace standard_name = subinstr( standard_name, " AG U COKG ", " AG & CO KG ", 1)
replace standard_name = subinstr( standard_name, " AG U CO ", " AG & CO KG ", 1)
replace standard_name = subinstr( standard_name, " AG CO ", " AG & CO KG ", 1)
replace standard_name = subinstr( standard_name, " GMBH CO OHG ", " GMBH &CO OHG ", 1)
replace standard_name = subinstr( standard_name, " GMBH COOHG ", " GMBH & CO OHG ", 1)
replace standard_name = subinstr( standard_name, " GMBH U CO OHG ", " GMBH & CO OHG ", 1)
replace standard_name = subinstr( standard_name, " GMBH U COOHG ", " GMBH & CO OHG ", 1)
replace standard_name = subinstr( standard_name, " AG CO OHG ", " AG & CO OHG ", 1)
replace standard_name = subinstr( standard_name, " AG COOHG ", " AG & CO OHG ", 1)
replace standard_name = subinstr( standard_name, " AG U CO OHG ", " AG & CO OHG ", 1)
replace standard_name = subinstr( standard_name, " AG U COOHG ", " AG & CO OHG ", 1)

** FRENCH and BELGIAN
replace standard_name = subinstr( standard_name, " SOCIETE ANONYME SIMPLIFIEE ", " SAS ", 1)
replace standard_name = subinstr( standard_name, " SOC ANONYME ", " SA ", 1)
replace standard_name = subinstr( standard_name, " STE ANONYME ", " SA ", 1)
replace standard_name = subinstr( standard_name, " SARL UNIPERSONNELLE ", " SARLU ", 1)
replace standard_name = subinstr( standard_name, " SOC PAR ACTIONS SIMPLIFIEES ", " SAS ", 1)
replace standard_name = subinstr( standard_name, " SAS UNIPERSONNELLE ", " SASU ", 1)
replace standard_name = subinstr( standard_name, " ENTREPRISE UNIPERSONNELLE A RESPONSABILITE LIMITEE ", " EURL ", 1)
replace standard_name = subinstr( standard_name, " SOCIETE CIVILE IMMOBILIERE ", " SCI ", 1)
replace standard_name = subinstr( standard_name, " GROUPEMENT D INTERET ECONOMIQUE ", " GIE ", 1)
replace standard_name = subinstr( standard_name, " SOCIETE EN PARTICIPATION ", " SP ", 1)
replace standard_name = subinstr( standard_name, " SOCIETE EN COMMANDITE SIMPLE ", " SCS ", 1)
replace standard_name = subinstr( standard_name, " ANONYME DITE ", " SA ", 1)
replace standard_name = subinstr( standard_name, " SOC DITE ", " SA ", 1)
replace standard_name = subinstr( standard_name, " & CIE ", " CO ", 1)

** BELGIAN
** Note: the Belgians use a lot of French endings, so handle as above.
** Also, they use NV (belgian) and SA (french) interchangably, so standardise to SA

replace standard_name = subinstr( standard_name, " BV BEPERKTE AANSPRAKELIJKHEID ", " BVBA ", 1)
replace standard_name = subinstr( standard_name, " COMMANDITAIRE VENNOOTSCHAP OP AANDELEN ", " CVA ", 1)
replace standard_name = subinstr( standard_name, " GEWONE COMMANDITAIRE VENNOOTSCHAP ", " GCV ", 1)
replace standard_name = subinstr( standard_name, " SOCIETE EN COMMANDITE PAR ACTIONS ", " SCA ", 1)

* Change to French language equivalents where appropriate
* Don't do this for now
*replace standard_name = subinstr( standard_name, " GCV ", " SCS ", 1)
*replace standard_name = subinstr( standard_name, " NV ", " SA ", 1)
*replace standard_name = subinstr( standard_name, " BVBA ", " SPRL ", 1)

** DENMARK
* Usually danish identifiers have a slash (eg. A/S or K/S), but these will have been removed with all
* other punctuation earlier (so just use AS or KS).
replace standard_name = subinstr( standard_name, " ANDELSSELSKABET ", " AMBA ", 1)
replace standard_name = subinstr( standard_name, " ANDELSSELSKAB ", " AMBA ", 1)
replace standard_name = subinstr( standard_name, " INTERESSENTSKABET ", " IS ", 1)
replace standard_name = subinstr( standard_name, " INTERESSENTSKAB ", " IS ", 1)
replace standard_name = subinstr( standard_name, " KOMMANDITAKTIESELSKABET ", " KAS ", 1)
replace standard_name = subinstr( standard_name, " KOMMANDITAKTIESELSKAB ", " KAS ", 1)
replace standard_name = subinstr( standard_name, " KOMMANDITSELSKABET ", " KS ", 1)
replace standard_name = subinstr( standard_name, " KOMMANDITSELSKAB ", " KS ", 1)

** NORWAY
replace standard_name = subinstr( standard_name, " ANDELSLAGET ", " AL ", 1)
replace standard_name = subinstr( standard_name, " ANDELSLAG ", " AL ", 1)
replace standard_name = subinstr( standard_name, " ANSVARLIG SELSKAPET ", " ANS ", 1)
replace standard_name = subinstr( standard_name, " ANSVARLIG SELSKAP ", " ANS ", 1)
replace standard_name = subinstr( standard_name, " AKSJESELSKAPET ", " AS ", 1)
replace standard_name = subinstr( standard_name, " AKSJESELSKAP ", " AS ", 1)
replace standard_name = subinstr( standard_name, " ALLMENNAKSJESELSKAPET ", " ASA ", 1)
replace standard_name = subinstr( standard_name, " ALLMENNAKSJESELSKAP ", " ASA ", 1)
replace standard_name = subinstr( standard_name, " SELSKAP MED DELT ANSAR ", " DA ", 1)
replace standard_name = subinstr( standard_name, " KOMMANDITTSELSKAPET ", " KS ", 1)
replace standard_name = subinstr( standard_name, " KOMMANDITTSELSKAP ", " KS ", 1)

** NETHERLANDS
replace standard_name = subinstr( standard_name, " COMMANDITAIRE VENNOOTSCHAP ", " CV ", 1)
replace standard_name = subinstr( standard_name, " COMMANDITAIRE VENNOOTSCHAP OP ANDELEN ", " CVOA ", 1)
replace standard_name = subinstr( standard_name, " VENNOOTSCHAP ONDER FIRMA ", " VOF ", 1)

** FINLAND
replace standard_name = subinstr( standard_name, " PUBLIKT AKTIEBOLAG ", " APB ", 1)
replace standard_name = subinstr( standard_name, " KOMMANDIITTIYHTIO ", " KY ", 1)
replace standard_name = subinstr( standard_name, " JULKINEN OSAKEYHTIO ", " OYJ ", 1)

** POLAND
replace standard_name = subinstr( standard_name, " SPOLKA AKCYJNA ", " SA ", 1) 
replace standard_name = subinstr( standard_name, " SPOLKA PRAWA CYWILNEGO ", " SC ", 1)
replace standard_name = subinstr( standard_name, " SPOLKA KOMANDYTOWA ", " SK ", 1)
replace standard_name = subinstr( standard_name, " SPOLKA Z OGRANICZONA ODPOWIEDZIALNOSCIA ", " SPZOO ", 1)
replace standard_name = subinstr( standard_name, " SP Z OO ", " SPZOO ", 1)
replace standard_name = subinstr( standard_name, " SPZ OO ", " SPZOO ", 1)
replace standard_name = subinstr( standard_name, " SP ZOO ", " SPZOO ", 1)

** GREECE
replace standard_name = subinstr( standard_name, " ANONYMOS ETAIRIA ", " AE ", 1)
replace standard_name = subinstr( standard_name, " ETERRORRYTHMOS ", " EE ", 1)
replace standard_name = subinstr( standard_name, " ETAIRIA PERIORISMENIS EVTHINIS ", " EPE ", 1)
replace standard_name = subinstr( standard_name, " OMORRYTHMOS ", " OE ", 1)

** CZECH REPUBLIC
replace standard_name = subinstr( standard_name, " AKCIOVA SPOLECNOST ", " AS ", 1)
replace standard_name = subinstr( standard_name, " KOMANDITNI SPOLECNOST ", " KS ", 1)
replace standard_name = subinstr( standard_name, " SPOLECNOST S RUCENIM OMEZENYM ", " SRO ", 1)
replace standard_name = subinstr( standard_name, " VEREJNA OBCHODNI SPOLECNOST ", " VOS ", 1) 
                
** BULGARIA
replace standard_name = subinstr( standard_name, " AKTIONIERNO DRUSHESTWO ", " AD ", 1)
replace standard_name = subinstr( standard_name, " KOMANDITNO DRUSHESTWO ", " KD ", 1)
replace standard_name = subinstr( standard_name, " KOMANDITNO DRUSHESTWO S AKZII ", " KDA ", 1)
replace standard_name = subinstr( standard_name, " DRUSHESTWO S ORGRANITSCHENA OTGOWORNOST ", " OCD ", 1)
