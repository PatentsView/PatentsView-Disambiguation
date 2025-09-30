**
** Clean Compustat name file
** Bronwyn Hall, 11 Sep 2006
**
** additions made by Jim Bessen, 1-16-07
** made into nameonly_main.do from names_main_compustat2.do
* 	changed to be called with different files
* 	uses punctuation2 and combabbrev
*
* NOTE: this leaves multiple firms with same stem_name
*
set more 1
*global CSDIR ="C:/docume~1/HP_Owner/mydocu~1/data/pdp/freqmatch"
*global NAMDIR ="C:/docume~1/HP_Owner/mydocu~1/data/pdp/freqmatch"
*use $CSDIR/cshdr05,clear



  **   Clean names
  
  rename coname name
  rename file csfile
  gen file = "CS"
  gen asstype = "firm"



  gen standard_name = " "+trim(name)+" "        /* so we can handle words at beg and end of string*/
  replace standard_name=upper(standard_name)

  /*0  Special Compustat recoding */
  replace standard_name = subinstr(standard_name,"-ADR"," ",30)
  replace standard_name = subinstr(standard_name,"-ADS"," ",30)
  replace standard_name = subinstr(standard_name,"-CL A "," ",30)
  replace standard_name = subinstr(standard_name,"-CL B "," ",30)
  replace standard_name = subinstr(standard_name,"-CONN "," ",30)
  replace standard_name = subinstr(standard_name,"-CONSOLIDATED "," ",30)
  replace standard_name = subinstr(standard_name,"-DEL "," ",30)
  replace standard_name = subinstr(standard_name,"-DE "," ",30)
  replace standard_name = subinstr(standard_name,"-NY SHARES "," ",30)
  replace standard_name = subinstr(standard_name,"-OLD "," ",30)
  replace standard_name = subinstr(standard_name,"-ORD "," ",30)
  replace standard_name = subinstr(standard_name,"-PRE AMEND "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PRE DIVEST "," ",30) 	/* JB */
  replace standard_name = subinstr(standard_name,"-PREAMEND "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PREDIVEST "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PROJ "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PROJECTED "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PREF "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PRE FASB "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PREFASB "," ",30) 		/* JB */
  replace standard_name = subinstr(standard_name,"-PRO FORMA "," ",30)
  replace standard_name = subinstr(standard_name,"- PRO FORMA "," ",30)
  replace standard_name = subinstr(standard_name,"-PRO FORMA1 "," ",30)
  replace standard_name = subinstr(standard_name,"-PRO FORMA2 "," ",30)
  replace standard_name = subinstr(standard_name,"-PRO FORMA3 "," ",30)
  replace standard_name = subinstr(standard_name,"-REDH "," ",30)
  replace standard_name = subinstr(standard_name,"-SER A COM "," ",30)
  replace standard_name = subinstr(standard_name,"-SER A "," ",30)
  replace standard_name = subinstr(standard_name,"-SPN "," ",30)

  replace standard_name = subinstr(standard_name," ACCPTNCE "," ACCEPTANCE ",30)
  replace standard_name = subinstr(standard_name," BANCORPORATION "," BANCORP ",30)
  replace standard_name = subinstr(standard_name," BANCORPORTN "," BANCORP ",30)
  replace standard_name = subinstr(standard_name," BANCRP "," BANCORP ",30)
  replace standard_name = subinstr(standard_name," BNCSHRS "," BANCSHARES ",30)
  replace standard_name = subinstr(standard_name," BRWG "," BREWING ",30)
  replace standard_name = subinstr(standard_name," CHEVRONTEXACO "," CHEVRON TEXACO ",30)
  replace standard_name = subinstr(standard_name," CHSE "," CHASE ",30)
  replace standard_name = subinstr(standard_name," COMMN "," COMMUNICATION ",30)
  replace standard_name = subinstr(standard_name," COMMUN "," COMMUNICATION ",30)
  replace standard_name = subinstr(standard_name," COMMUNICATNS "," COMMUNICATION ",30)
  replace standard_name = subinstr(standard_name," COMMUNICATIONS "," COMMUNICATION ",30)
  replace standard_name = subinstr(standard_name," DPT STS "," DEPT STORES ",30)
  replace standard_name = subinstr(standard_name," DPT "," DEPT ",30)
  replace standard_name = subinstr(standard_name," ENRGY "," ENERGY ",30)
  replace standard_name = subinstr(standard_name," FINL "," FINANCIAL ",30)
  replace standard_name = subinstr(standard_name," FNCL "," FINANCIAL ",30)
  replace standard_name = subinstr(standard_name," GRP "," GROUP ",30)
  replace standard_name = subinstr(standard_name," HLDGS "," HOLDINGS ",30)  
  replace standard_name = subinstr(standard_name," HLDG "," HOLDING ",30)
  replace standard_name = subinstr(standard_name," HLT NTWK "," HEALTH NETWORK ",30)
  replace standard_name = subinstr(standard_name," HTLS RES "," HOTELS & RESORTS ",30)
  replace standard_name = subinstr(standard_name," HLTH "," HEALTH ",30)
  replace standard_name = subinstr(standard_name," INTRTECHNLGY "," INTERTECHNOLOGY ",30)
  replace standard_name = subinstr(standard_name," JPMORGAN "," J P MORGAN ",30)
  replace standard_name = subinstr(standard_name," MED OPTIC "," MEDICAL OPTICS ",30)
  replace standard_name = subinstr(standard_name," MINNESOTA MINING AND MANUFACTURING COMPANY "," 3M COMPANY ",30)
  replace standard_name = subinstr(standard_name," NAT RES "," NATURAL RESOURCES ",30)
  replace standard_name = subinstr(standard_name," NETWRKS "," NETWORK ",30)
  replace standard_name = subinstr(standard_name," PHARMACTICALS "," PHARM ",30)
  replace standard_name = subinstr(standard_name," PHARMACT "," PHARM ",30)
  replace standard_name = subinstr(standard_name," PPTYS TST "," PROPERTIES TRUST ",30)
  replace standard_name = subinstr(standard_name," PPTY "," PROPERTY ",30)
  replace standard_name = subinstr(standard_name," PROPERTY TR "," PROPERTY TRUST ",30)
  replace standard_name = subinstr(standard_name," PAC RAILWY "," PACIFIC RAILWAY ",30)
  replace standard_name = subinstr(standard_name," SEMICONDTR "," SEMICONDUCTOR ",30)
  replace standard_name = subinstr(standard_name," SOLU "," SOLUTIONS ",30)
  replace standard_name = subinstr(standard_name," ST & ALMN "," STEEL & ALUMINUM ",30)
  replace standard_name = subinstr(standard_name," STD "," STANDARD ",30)
  replace standard_name = subinstr(standard_name," TECHNOLGS "," TECH ",30)
  replace standard_name = subinstr(standard_name," TECHNOL "," TECH ",30)
  replace standard_name = subinstr(standard_name," TRANSPORTATN "," TRANSPORTATION ",30)

/* added items */
	replace standard_name = subinstr(standard_name," ADVERTSG "," ADVERTISING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ADVNTGE "," ADVANTAGE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," AIRLN "," AIRLINES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," AIRLS "," AIRLINES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," AM "," AMERICA ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," AMER "," AMERICAN ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," APPLIAN "," APPLIANCES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," APPLICTN "," APPLICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ARCHTCTS "," ARCHITECTS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ASSD "," ASSOCIATED ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ASSOC "," ASSOCIATES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ASSOCS "," ASSOCIATES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ATOMC "," ATOMIC ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," BANCSH "," BANCSHARES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," BANCSHR "," BANCSHARES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," BCSHS "," BANCSHARES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," BK "," BANK ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," BLDGS "," BUILDINGS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," BROADCASTG "," BROADCASTING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," BTLNG "," BOTTLING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CBLVISION "," CABLEVISION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CENTRS "," CENTERS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CHAMPNSHIP "," CHAMPIONSHIP ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CMMNCTNS "," COMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CNVRSION "," CONVERSION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," COFF "," COFFEE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," COMM "," COMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," COMMUN "," COMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," COMMUNCTN "," COMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," COMMUNICTNS "," COMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," COMP "," COMPUTERS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," COMPUTR "," COMPUTER ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CONFERENCG "," CONFERENCING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CONSTRN "," CONSTR ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CONTL "," CONTINENTAL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CONTNT "," CONTINENTAL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CONTRL "," CONTROL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CONTRL "," CONTROL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CTR "," CENTER ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CTRS "," CENTERS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," CVRGS "," COVERINGS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," DEV "," DEVELOPMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," DEVL "," DEVELOPMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," DEVLP "," DEVELOPMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," DISTR "," DISTRIBUTION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," DISTRIBUT "," DISTRIBUTION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," DISTRIBUTN "," DISTRIBUTION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ELCTRNCS "," ELECTRONICS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ELECTR "," ELECTRONICS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENGNRD "," ENGINEERED ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENMT "," ENTERTAINMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENTERTAIN "," ENTERTAINMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENTERTNMNT "," ENTERTAINMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENTMNT "," ENTERTAINMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENTMT "," ENTERTAINMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENTRPR "," ENTERPRISES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENTRPRISE "," ENTERPRISES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENTRPRS "," ENTERPRISES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENVIR "," ENVIRONMENTAL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENVIRNMNTL "," ENVIRONMENTAL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ENVR "," ENVIRONMENTAL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," EQUIPMT "," EQUIPMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," EXCHG "," EXCHANGE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," EXPLOR "," EXPLORATION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," FNDG "," FUNDING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," GLD "," GOLD ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," GP "," GROUP ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," HLDS "," HLDGS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," HLTHCARE "," HEALTHCARE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," HLTHCR "," HEALTHCARE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," HOMEMDE "," HOMEMADE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," HSPTL "," HOSPITAL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ILLUM "," ILLUMINATION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INDL "," INDUSTRIAL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INDPT "," INDEPENDENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INDTY "," INDEMNITY ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INFORMATN "," INFO ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INSTNS "," INSTITUTIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INSTRUMEN "," INSTRUMENTS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INSTRUMNT "," INSTRUMENTS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INTEGRATRS "," INTEGRATORS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INTERNATIONL "," INT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INVS "," INVESTMENTS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," INVT "," INVESTMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MANAGEMNT "," MANAGEMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MANAGMNT "," MANAGEMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MANHATN "," MANHATTAN ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MANUF "," MFG ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MDSE "," MERCHANDISING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MEASURMNT "," MEASUREMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MERCHNDSNG "," MERCHANDISING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MGMT "," MANAGEMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MGRS "," MANAGERS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MGT "," MANAGEMENT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MICROWAV "," MICROWAVE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MKTS "," MARKETS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MLTIMEDIA "," MULTIMEDIA ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MTG "," MORTGAGE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MTNS "," MOUTAINS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," MTRS "," MOTORS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," NETWRK "," NETWORK ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," NOWEST "," NORTHWEST ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," NTWRK "," NETWORK ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," OFFSHRE "," OFFSHORE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," ORGANIZTN "," ORG ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PBLG "," PUBLISHING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PHARMACEUTICL "," PHARM ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PLAST "," PLASTICS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PPTYS "," PROPERTIES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PRODS "," PROD ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PRODTN "," PRODN ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PRODUCTN "," PRODN ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PRPANE "," PROPANE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PTS "," PARTS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PUBLISH "," PUBLISHING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PUBLSHING "," PUBLISHING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PUBN "," PUBLICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PUBNS "," PUBLICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," PWR "," POWER ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RAILRD "," RAILROAD ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RECREATN "," RECREATION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RECYCL "," RECYCLING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," REFIN "," REFINING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," REFNG "," REFINING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RESTR "," RESTAURANT ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RESTS "," RESTAURANTS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RETAILNG "," RETAILING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RLTY "," REALTY ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RR "," RAILROAD ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RSCH "," RESEARCH ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," RTNG "," RATING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SCIENTIF "," SCIENTIFIC ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SERV "," SERVICES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SLTNS "," SOLUTIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SOFTWRE "," SOFTWARE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SOLTNS "," SOLUTIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SOLUT "," SOLUTIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SRVC "," SERVICES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SRVCS "," SERVICES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," STEAKHSE "," STEAKHOUSE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," STHWST "," SOUTHWEST ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," STL "," STEEL ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," STRS "," STORES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SUP "," SUPPLY ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SUPERMKTS "," SUPERMARKETS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SUPP "," SUPPLIES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SURVYS "," SURVEYS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SVC "," SERVICES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SVCS "," SERVICES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SVSC "," SERVICES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SYS "," SYSTEMS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," SYSTM "," SYSTEMS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TCHNLGY "," TECH ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TECHNGS "," TECHNOLOGIES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TECHNL "," TECH ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TECHNLGIES "," TECHNOLOGIES ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TEL "," TELEPHONE ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TELE-COMM "," TELECOMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TELE-COMMUN "," TELECOMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TELECOMMS "," TELECOMMUNICATIONS ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TELECONFERENC "," TELECONFERENCING ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TELEG "," TELEGRAPH ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TELEGR "," TELEGRAPH ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TELVSN "," TELEVISION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TR "," TRUST ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TRANSN "," TRANSPORTATION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TRANSPORTN "," TRANSPORTATION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," TRNSACTN "," TRANSACTION ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," UTD "," UNITED ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," WSTN "," WESTERN ",30)	 /* JB */
	replace standard_name = subinstr(standard_name," WTR "," WATER ",30)	 /* JB */
	

 
*  replace standard_name=" U.S. PHILIPS CORPORATION " if trim(standard_name)=="NORTH AMERICAN PHILIPS CORP"
  replace standard_name=" A. L. WILLIAMS CORP. " if trim(standard_name)=="WILLIAMS (A.L.) CORP"
  replace standard_name=" B. F. GOODRICH CO. " if trim(standard_name)=="GOODRICH CORP"
  replace standard_name=" BELL + HOWELL COMPANY " if trim(standard_name)=="BELL & HOWELL OPERATING CO"
  replace standard_name=" BENDIX CORPORATION(NOW ALLIED-SIGNAL INC.) " if trim(standard_name)=="BENDIX CORP"
  replace standard_name=" BORG-WARNER CORPORATION " if trim(standard_name)=="BORGWARNER INC"
  replace standard_name=" CHRYSLER MOTORS CORPORATION " if trim(standard_name)=="CHRYSLER CORP"
  replace standard_name=" CISCO TECHNOLOGY, INC. " if trim(standard_name)=="CISCO SYSTEMS INC"
  replace standard_name=" DELL PRODUCTS, L.P. " if trim(standard_name)=="DELL INC"
  replace standard_name=" DELPHI TECHNOLOGIES, INC. " if trim(standard_name)=="DELPHI CORP"
  replace standard_name=" E. I. DU PONT DE NEMOURS AND COMPANY " if trim(standard_name)=="DU PONT (E I) DE NEMOURS"
  replace standard_name=" E. R. SQUIBB + SONS, INC. " if trim(standard_name)=="SQUIBB CORP"
  replace standard_name=" ELI LILLY AND COMPANY " if trim(standard_name)=="LILLY (ELI) & CO"
  replace standard_name=" G. D. SEARLE & CO. " if trim(standard_name)=="SEARLE (G.D.) & CO"
  replace standard_name=" MINNESOTA MINING AND MANUFACTURING COMPANY " if trim(standard_name)=="3M CO"
  replace standard_name=" OWENS-CORNING FIBERGLAS CORPORATION " if trim(standard_name)=="OWENS CORNING"
  replace standard_name=" SCHLUMBERGER TECHNOLOGY CORPORATION " if trim(standard_name)=="SCHLUMBERGER LTD"
  replace standard_name=" SCI-MED LIFE SYSTEMS, INC. " if trim(standard_name)=="SICMED LIFE SYSTEMS"
  replace standard_name=" TDK CORPORATION " if trim(standard_name)=="TDK CORP"
  replace standard_name=" UNITED STATES SURGICAL CORPORATION " if trim(standard_name)=="U S SURGICAL CORP"
  replace standard_name=" W. R. GRACE & CO. " if trim(standard_name)=="GRACE (W R) & CO"
  replace standard_name=" WESTINGHOUSE ELECTRIC CORP. " if trim(standard_name)=="WESTINGHOUSE ELEC"

  /*1*/ do $NAMDIR/punctuation2 

  /*2*/ qui do $NAMDIR/standard_name 

  /*3*/ 
  qui do $NAMDIR/corporates

/* 3b */ 
	qui do $NAMDIR/combabbrev

  /*4*/ qui do $NAMDIR/stem_name

  /*5*/ replace stem_name = trim(stem_name)




