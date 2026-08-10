************************************************************************************************
** Procedure 3 IDENTIFY NON-CORPORATES
**
** This section attempts to identify non-corporates by looking for words such as "UNIVERSITY".
** A flag is set if such words are found. Again this is country specific and laid out as such.
** Again this is very crude and the results of it should be used with caution. 
** Recode by BHH to remove checks for country. It needs more work to find country specific terms
* jb 1/15/08 index=>strpos
************************************************************************************************
*cap prog drop non_corporates
*prog def non_corporates

** Individuals

replace asstype = "indiv" if strpos(standard_name," GEB ")>0
replace asstype = "indiv" if strpos(standard_name," DECEASED ")>0
replace asstype = "indiv" if strpos(standard_name," DECEDE ")>0
replace asstype = "indiv" if strpos(standard_name," DESEASED ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL BETRIEBSWIRT ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL CHEM ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL GEOGR ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL ING ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL ING ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL PHYS ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL PHYS ")>0
replace asstype = "indiv" if strpos(standard_name," DIPL WIRTSCH ING ")>0
replace asstype = "indiv" if strpos(standard_name," DOTT ING ")>0
replace asstype = "indiv" if strpos(standard_name," DR ")>0
replace asstype = "indiv" if strpos(standard_name," EPOSE ")>0
replace asstype = "indiv" if strpos(standard_name," EPOUSE ")>0
replace asstype = "indiv" if strpos(standard_name," EPSE ")>0
replace asstype = "indiv" if strpos(standard_name," GEBOREN ")>0
replace asstype = "indiv" if strpos(standard_name," GEBORENE ")>0
replace asstype = "indiv" if strpos(standard_name," GES VERTRETEN DURCH ")>0
replace asstype = "indiv" if strpos(standard_name," GRAD ")>0
replace asstype = "indiv" if strpos(standard_name," HERITIERE ")>0
replace asstype = "indiv" if strpos(standard_name," ING ")>0
replace asstype = "indiv" if strpos(standard_name," ING GRAD ")>0
replace asstype = "indiv" if strpos(standard_name," ING DIPL ")>0
replace asstype = "indiv" if strpos(standard_name," JR ")>0 
replace asstype = "indiv" if strpos(standard_name," LA SUCCESSION ")>0
replace asstype = "indiv" if strpos(standard_name," LEGAL ")>0
replace asstype = "indiv" if strpos(standard_name," LEGALLY REPR ")>0
replace asstype = "indiv" if strpos(standard_name," LEGALLY REPRESENTED ")>0
replace asstype = "indiv" if strpos(standard_name," LEGALY REPRESENTED BY ")>0
replace asstype = "indiv" if strpos(standard_name," NEE ")>0
replace asstype = "indiv" if strpos(standard_name," PHD ")>0
replace asstype = "indiv" if strpos(standard_name," PROF ")>0
replace asstype = "indiv" if strpos(standard_name," REPRESENTATIVE OF ")>0
replace asstype = "indiv" if strpos(standard_name," REPRESENTEE PAR SON LEGAL ")>0
replace asstype = "indiv" if strpos(standard_name," VERSTORBEN ")>0
replace asstype = "indiv" if strpos(standard_name," VERSTORBEN ERFINDERS ")>0
replace asstype = "indiv" if strpos(standard_name," VERSTORBENEN ERFINDERS ")>0
replace asstype = "indiv" if strpos(standard_name," VERTRETEN DURCH ")>0

** Government

replace asstype = "govt" if strpos(standard_name, " NAT RES COUNCIL ")>0 
replace asstype = "govt" if strpos(standard_name, " NAT RES INST ")>0 
replace asstype = "govt" if strpos(standard_name, " NAT SCI COUNCIL ")>0 
replace asstype = "govt" if strpos(standard_name, " NAT SCI INST ")>0 
replace asstype = "govt" if strpos(standard_name," AGENCY ")>0
replace asstype = "govt" if strpos(standard_name," STATE OF ")>0

replace asstype = "govt" if strpos(standard_name," US ADMIN ")>0
replace asstype = "govt" if strpos(standard_name," ADMINISTRATOR ")>0
replace asstype = "govt" if strpos(standard_name," COMMISSIONER OF PATENTS ")>0
replace asstype = "govt" if strpos(standard_name," US DEPT ")>0
replace asstype = "govt" if strpos(standard_name," US SEC ")>0

replace asstype = "govt" if strpos(standard_name, " UK SEC FOR ")>0 
replace asstype = "govt" if strpos(standard_name, " UK ")>0
replace asstype = "govt" if strpos(standard_name, " COMMONWEALTH ")>0 
replace asstype = "govt" if strpos(standard_name, " MIN OF ")>0 
replace asstype = "govt" if strpos(standard_name, " MIN FOR ")>0 

replace asstype = "govt" if strpos(standard_name, " LETAT FR REPRESENTE ")>0
replace asstype = "govt" if strpos(standard_name, " LA POSTE ")>0

replace asstype = "govt" if strpos(standard_name," ADMINISTRATOR ")>0
replace asstype = "govt" if strpos(standard_name," AGENCE ")>0
replace asstype = "govt" if strpos(standard_name," AGENCY ")>0
replace asstype = "govt" if strpos(standard_name," AMMINISTRAZIONE ")>0
replace asstype = "govt" if strpos(standard_name," AMMINISTRAZIONE ")>0
replace asstype = "govt" if strpos(standard_name," AUTHORITY ")>0
replace asstype = "govt" if strpos(standard_name," BOTANICAL GARDEN ")>0
replace asstype = "govt" if strpos(standard_name," BUNDESANSTALT ")>0
replace asstype = "govt" if strpos(standard_name," BUNDESREPUBLIK ")>0
replace asstype = "govt" if strpos(standard_name," CHAMBRE ")>0
replace asstype = "govt" if strpos(standard_name," CITY ")>0
replace asstype = "govt" if strpos(standard_name," COMISSARIAT ")>0
replace asstype = "govt" if strpos(standard_name," COMMISARIAT ")>0
replace asstype = "govt" if strpos(standard_name," COMMISSARAIT ")>0
replace asstype = "govt" if strpos(standard_name," COMMISSARAT ")>0
replace asstype = "govt" if strpos(standard_name," COMMISSARIAT ")>0
replace asstype = "govt" if strpos(standard_name," COMMISSARIET ")>0
replace asstype = "govt" if strpos(standard_name," COMMISSION ")>0
replace asstype = "govt" if strpos(standard_name," COMMISSRIAT ")>0
replace asstype = "govt" if strpos(standard_name," COMMONWEALTH ")>0
replace asstype = "govt" if strpos(standard_name," COMMUNAUTE ")>0
replace asstype = "govt" if strpos(standard_name," CONFEDERATED TRIBES ")>0
replace asstype = "govt" if strpos(standard_name," COOUNCIL OF ")>0
replace asstype = "govt" if strpos(standard_name," COUCIL OF ")>0
replace asstype = "govt" if strpos(standard_name," COUNCIL ")>0
replace asstype = "govt" if strpos(standard_name," COUNSEL OF ")>0
replace asstype = "govt" if strpos(standard_name," COUNTY ")>0
replace asstype = "govt" if strpos(standard_name," DEN PRAESIDENTEN ")>0
replace asstype = "govt" if strpos(standard_name," DEPARTMENT OF AGRICULTURE ")>0
replace asstype = "govt" if strpos(standard_name," DETAT ")>0
replace asstype = "govt" if strpos(standard_name," ETAT ")>0
replace asstype = "govt" if strpos(standard_name," GERMANY ")>0
replace asstype = "govt" if strpos(standard_name," GEZONDHEIDSDIENST ")>0
replace asstype = "govt" if strpos(standard_name," GOUVERNEMENT ")>0
replace asstype = "govt" if strpos(standard_name," GOUVERNMENT ")>0
replace asstype = "govt" if strpos(standard_name," GOVERNER ")>0
replace asstype = "govt" if strpos(standard_name," GOVERNMENT ")>0
replace asstype = "govt" if strpos(standard_name," GOVERNOR ")>0
replace asstype = "govt" if strpos(standard_name," HER MAJESTY ")>0
replace asstype = "govt" if strpos(standard_name," KEN ")>0
replace asstype = "govt" if strpos(standard_name," LETAT ")>0
replace asstype = "govt" if strpos(standard_name," MINISTER ")>0
replace asstype = "govt" if strpos(standard_name," MINISTERO ")>0
replace asstype = "govt" if strpos(standard_name," MINISTRE ")>0
replace asstype = "govt" if strpos(standard_name," MINISTRI ")>0
replace asstype = "govt" if strpos(standard_name," MINISTRO ")>0
replace asstype = "govt" if strpos(standard_name," MINISTRY ")>0
replace asstype = "govt" if strpos(standard_name," MUNICIPAL UTILITY DISTRICT ")>0
replace asstype = "govt" if strpos(standard_name," NACIONAL ")>0
replace asstype = "govt" if strpos(standard_name," NATIONAL ")>0
replace asstype = "govt" if strpos(standard_name," NAZIONALE ")>0
replace asstype = "govt" if strpos(standard_name," POLICE ")>0
replace asstype = "govt" if strpos(standard_name," PREFECTURE ")>0
replace asstype = "govt" if strpos(standard_name," PRESIDENZA DEL CONSIGLIO DEI MINISTRI ")>0
replace asstype = "govt" if strpos(standard_name," PRESIDENZADEL CONSIGLIO DEL MINISTRI ")>0
replace asstype = "govt" if strpos(standard_name," REPUBLIC ")>0
replace asstype = "govt" if strpos(standard_name," RESEARCH COUNCIL ")>0
replace asstype = "govt" if strpos(standard_name," SECRETARIAT ")>0
replace asstype = "govt" if strpos(standard_name," SECRETARY ")>0
replace asstype = "govt" if strpos(standard_name," STAAT ")>0
replace asstype = "govt" if strpos(standard_name," STADT ")>0
replace asstype = "govt" if strpos(standard_name," STATE ")>0 
replace asstype = "govt" if strpos(standard_name," STATO ")>0
replace asstype = "govt" if strpos(standard_name," THE QUEEN ")>0
replace asstype = "govt" if strpos(standard_name," VILLE ")>0


** clean up government departments
replace standard_name = subinstr(standard_name," SEC OF DEPT OF "," DEPT OF ",1)
replace standard_name = subinstr(standard_name," SEC OF THE DEPT OF "," DEPT OF ",1)

replace asstype = "govt" if strpos(standard_name," CNRS ")>0
replace asstype = "govt" if strpos(standard_name," CENT NAT DE LA RECH ")>0
replace asstype = "govt" if strpos(standard_name," CENT NAT DETUDES SPATIALES ")>0

replace asstype = "govt" if strpos(standard_name, " DESY ")>0 

** universities

replace asstype = "univ" if strpos(standard_name," ACADEM")>0
replace asstype = "univ" if strpos(standard_name," ACAD ")>0
replace asstype = "univ" if strpos(standard_name," AKAD ")>0
replace asstype = "univ" if strpos(standard_name," COLLEGE ")>0
replace asstype = "univ" if strpos(standard_name," CURATORS ")>0
replace asstype = "univ" if strpos(standard_name," ECOLE ")>0
replace asstype = "univ" if strpos(standard_name," FACULTE ")>0
replace asstype = "univ" if strpos(standard_name," INST OF TECH ")>0
replace asstype = "univ" if strpos(standard_name," INST OF TECH")>0
replace asstype = "univ" if strpos(standard_name," INSTITUTE OF TECHNOLOGY ")>0
replace asstype = "univ" if strpos(standard_name," INTERNUIVERSITAIR ")>0
replace asstype = "univ" if strpos(standard_name," INTERUNIVERITAIR ")>0
replace asstype = "univ" if strpos(standard_name," POLITEC ")>0
replace asstype = "univ" if strpos(standard_name," POLYTEC ")>0
replace asstype = "univ" if strpos(standard_name," REGENTS ")>0
replace asstype = "univ" if strpos(standard_name," RIJKSUNIVERSTTEIT ")>0
replace asstype = "univ" if strpos(standard_name," SCHOOL ")>0
replace asstype = "univ" if strpos(standard_name," SCHULE ")>0
replace asstype = "univ" if strpos(standard_name," SUPERVISORS ")>0
replace asstype = "univ" if strpos(standard_name," TRUSTEES ")>0
replace asstype = "univ" if strpos(standard_name," UMIVERSIDAD ")>0
replace asstype = "univ" if strpos(standard_name," UNIV ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERISITY ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERISTY ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSATIES ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSI")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSTIA ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSTITAT ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSTITAET ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSTITY ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSTIY ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERSY ")>0
replace asstype = "univ" if strpos(standard_name," UNIVERZ ")>0
replace asstype = "univ" if strpos(standard_name," UNVERSITY ")>0

** clean up university names
replace standard_name = subinstr(standard_name," BOARD OF REGENTS OF THE "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF REGENTS OF "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF REGENTS "," ",1) 
replace standard_name = subinstr(standard_name," REGENTS OF THE "," ",1) 
replace standard_name = subinstr(standard_name," REGENTS OF "," ",1) 
replace standard_name = subinstr(standard_name," REGENTS "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF TRUSTEES OF THE "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF TRUSTEES OF "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF TRUSTEES OPERATING "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF TRUSTEES "," ",1) 
replace standard_name = subinstr(standard_name," TRUSTEES OF THE "," ",1) 
replace standard_name = subinstr(standard_name," TRUSTEES OF "," ",1) 
replace standard_name = subinstr(standard_name," TRUSTEES "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF SUPERVISORS OF THE "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF SUPERVISORS OF "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF SUPERVISORS "," ",1) 
replace standard_name = subinstr(standard_name," SUPERVISORS OF THE "," ",1) 
replace standard_name = subinstr(standard_name," SUPERVISORS OF "," ",1) 
replace standard_name = subinstr(standard_name," SUPERVISORS "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF GOVERNORS OF THE "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF GOVERNORS OF "," ",1) 
replace standard_name = subinstr(standard_name," BOARD OF GOVERNORS "," ",1) 
replace standard_name = subinstr(standard_name," GOVERNORS OF THE "," ",1) 
replace standard_name = subinstr(standard_name," GOVERNORS OF "," ",1) 
replace standard_name = subinstr(standard_name," GOVERNORS "," ",1) 
replace standard_name = subinstr(standard_name," CURATORS OF THE "," ",1) 
replace standard_name = subinstr(standard_name," CURATORS "," ",1) 
replace standard_name = subinstr(standard_name," THE "," ",30)


replace asstype = "univ" if strpos(standard_name," KU LEUVEN ")>0

** Non-profit institutes

replace asstype = "inst" if strpos(standard_name," RESEARCH COUNCIL ")!=0
replace asstype = "inst" if strpos(standard_name," RES COUNCIL ")!=0

replace asstype = "inst" if strpos(standard_name, " FRAUNHOFER GES ")>0 
replace asstype = "inst" if strpos(standard_name, " MAX PLANCK GES ")>0 

replace asstype = "inst" if strpos(standard_name," COUNCIL OF ")>0 & strpos(standard_name," RES ")>0

replace asstype = "inst" if strpos(standard_name," ASBL ")>0
replace asstype = "inst" if strpos(standard_name," ASOCIACION ")>0
*replace asstype = "inst" if strpos(standard_name," ASSOC ")>0
replace asstype = "inst" if strpos(standard_name," ASSOCIATION ")>0
replace asstype = "inst" if strpos(standard_name," ASSOCIAZIONE ")>0
replace asstype = "inst" if strpos(standard_name," BLOOD ")>0
replace asstype = "inst" if strpos(standard_name," BLOOD CENTER ")>0
replace asstype = "inst" if strpos(standard_name," BLOOD SERVICES ")>0
replace asstype = "inst" if strpos(standard_name," BLOOD TRANSFUSION SERVICE ")>0
replace asstype = "inst" if strpos(standard_name," CHURCH ")>0
replace asstype = "inst" if strpos(standard_name," COOPERATIVE ")>0
replace asstype = "inst" if strpos(standard_name," E V ")>0
replace asstype = "inst" if strpos(standard_name," EV ")>0
replace asstype = "inst" if strpos(standard_name," FEDERATION ")>0
replace asstype = "inst" if strpos(standard_name," FONDATION ")>0
replace asstype = "inst" if strpos(standard_name," FONDATIONE ")>0
replace asstype = "inst" if strpos(standard_name," FOUNDATION ")>0
replace asstype = "inst" if strpos(standard_name," FOUND ")~=0 & asstype~="univ"
replace asstype = "inst" if strpos(standard_name," FORSKNINGSINSTITUT ")>0
replace asstype = "inst" if strpos(standard_name," FUNDACAO ")>0
replace asstype = "inst" if strpos(standard_name," FUNDACIO ")>0
replace asstype = "inst" if strpos(standard_name," FUNDACION ")>0
replace asstype = "inst" if strpos(standard_name," FUNDATION ")>0
replace asstype = "inst" if strpos(standard_name," INDUSTRIAL TECHNOLOGY RESEARCH ")>0
replace asstype = "inst" if strpos(standard_name," INSITUT ")>0
replace asstype = "inst" if strpos(standard_name," INSITUTE ")>0
replace asstype = "inst" if strpos(standard_name," INST ")>0 & asstype~="univ"
replace asstype = "inst" if strpos(standard_name," INSTIT ")>0
replace asstype = "inst" if strpos(standard_name," INSTYTUT ")>0
replace asstype = "inst" if strpos(standard_name," INSTYTUT ")>0
replace asstype = "inst" if strpos(standard_name," INTITUTE ")>0
replace asstype = "inst" if strpos(standard_name," ISTITUTO ")>0
replace asstype = "inst" if strpos(standard_name," KENKYUSHO ")>0
replace asstype = "inst" if strpos(standard_name," MINISTRIES ")>0
replace asstype = "inst" if strpos(standard_name," SOCIETY ")>0
replace asstype = "inst" if strpos(standard_name," STICHTING ")>0
replace asstype = "inst" if strpos(standard_name," STIFTELSE ")>0
replace asstype = "inst" if strpos(standard_name," STIFTUNG ")>0
replace asstype = "inst" if strpos(standard_name," TRANSFUSION ")>0
replace asstype = "inst" if strpos(standard_name," TRANSFUSION SANGUINE ")>0
replace asstype = "inst" if strpos(standard_name," TRUST ")>0
replace asstype = "inst" if strpos(standard_name," VERENINING ")>0
replace asstype = "inst" if strpos(standard_name," VZW ")>0

** GERMANY
* EINGETRAGENER VEREIN. NON PROFIT SOCIETY/ASSOCIATION. 
replace asstype = "inst" if (strpos(standard_name, " EINGETRAGENER VEREIN ")!=0|strpos(standard_name," STIFTUNG ")~=0) ///
 &strpos(standard_name," UNIV ")==0 ///
 &strpos(standard_name," GMBH ")==0&strpos(standard_name," KGAA ")==0&strpos(standard_name," KG ")==0 /// 
 &strpos(standard_name," AG ")==0&strpos(standard_name," EG ")==0&strpos(standard_name," OHG ")==0

** Hospitals
replace asstype = "hosp" if strpos(standard_name," AMTS SYGEHUS ")>0
replace asstype = "hosp" if strpos(standard_name," AMTSSYGEHUS ")>0
replace asstype = "hosp" if strpos(standard_name," BOLNHITSA ")>0
replace asstype = "hosp" if strpos(standard_name," BOLNISN ")>0
replace asstype = "hosp" if strpos(standard_name," BOLNITSA ")>0
replace asstype = "hosp" if strpos(standard_name," BOLNYITSA ")>0
replace asstype = "hosp" if strpos(standard_name," CENTRE ")>0
replace asstype = "hosp" if strpos(standard_name," CLINIC ")>0
replace asstype = "hosp" if strpos(standard_name," CLINICA ")>0
replace asstype = "hosp" if strpos(standard_name," CLINIQUE ")>0
replace asstype = "hosp" if strpos(standard_name," HAIGLA ")>0
replace asstype = "hosp" if strpos(standard_name," HOPITAL ")>0
replace asstype = "hosp" if strpos(standard_name," HOPITAUX ")>0
replace asstype = "hosp" if strpos(standard_name," HOPSITAL ")>0
replace asstype = "hosp" if strpos(standard_name," HOSITAL ")>0
replace asstype = "hosp" if strpos(standard_name," HOSP ")>0
replace asstype = "hosp" if strpos(standard_name," HOSPIDAL ")>0
replace asstype = "hosp" if strpos(standard_name," HOSPITAL ")>0
replace asstype = "hosp" if strpos(standard_name," HOSPITALARIO ")>0
replace asstype = "hosp" if strpos(standard_name," HOSPITALET ")>0
replace asstype = "hosp" if strpos(standard_name," HOSPITAUX ")>0
replace asstype = "hosp" if strpos(standard_name," KESKUSSAIRAALA ")>0
replace asstype = "hosp" if strpos(standard_name," KLIINIK ")>0
replace asstype = "hosp" if strpos(standard_name," KLINIK ")>0
replace asstype = "hosp" if strpos(standard_name," KLINIKA ")>0
replace asstype = "hosp" if strpos(standard_name," KLINIKKA ")>0
replace asstype = "hosp" if strpos(standard_name," KLINIKUM ")>0
replace asstype = "hosp" if strpos(standard_name," KORHAZ ")>0
replace asstype = "hosp" if strpos(standard_name," KRANKENHAUS ")>0
replace asstype = "hosp" if strpos(standard_name," LHOSPTALET ")>0
replace asstype = "hosp" if strpos(standard_name," LIGONINE ")>0
*replace asstype = "hosp" if strpos(standard_name," MEDICAL ")>0
replace asstype = "hosp" if strpos(standard_name," MEDICAL CENTER ")>0
replace asstype = "hosp" if strpos(standard_name," NEMOCNICA ")>0
replace asstype = "hosp" if strpos(standard_name," NEMOCNICE ")>0
replace asstype = "hosp" if strpos(standard_name," NOSOCOMIO ")>0
replace asstype = "hosp" if strpos(standard_name," NOSOKOMIO ")>0
replace asstype = "hosp" if strpos(standard_name," OSPEDALE ")>0
replace asstype = "hosp" if strpos(standard_name," OSPETALE ")>0
replace asstype = "hosp" if strpos(standard_name," OSPITALIERI ")>0
replace asstype = "hosp" if strpos(standard_name," POLICLINICA ")>0
replace asstype = "hosp" if strpos(standard_name," POLICLINICO ")>0
replace asstype = "hosp" if strpos(standard_name," POLIKLINIK ")>0
replace asstype = "hosp" if strpos(standard_name," SAIRAALA ")>0
replace asstype = "hosp" if strpos(standard_name," SJUKHUS ")>0
replace asstype = "hosp" if strpos(standard_name," SJUKHUSET ")>0
replace asstype = "hosp" if strpos(standard_name," SLIMNICA ")>0
replace asstype = "hosp" if strpos(standard_name," SPITAL ")>0
replace asstype = "hosp" if strpos(standard_name," STACIONARS ")>0
replace asstype = "hosp" if strpos(standard_name," STANICA ")>0
replace asstype = "hosp" if strpos(standard_name," STREDISKO ")>0
replace asstype = "hosp" if strpos(standard_name," SYGEHUS ")>0
replace asstype = "hosp" if strpos(standard_name," SYGEHUSET ")>0
replace asstype = "hosp" if strpos(standard_name," SYKEHUS ")>0
replace asstype = "hosp" if strpos(standard_name," SZPITAL ")>0
replace asstype = "hosp" if strpos(standard_name," UNIVERSITAETSKLINIK ")>0
replace asstype = "hosp" if strpos(standard_name," ZIEKENHUIS ")>0

replace asstype = "hosp" if strpos(standard_name," CITY OF HOPE ")>0

** remove double spaces
replace standard_name = subinstr(standard_name," "," ",30)
