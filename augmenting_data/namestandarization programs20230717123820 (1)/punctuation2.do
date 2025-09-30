****************************************************************
** Procedure 1 Remove punctuation and standardise some symbols
**
** modified by BHH August 2006 to change var name and remove some 
** initializations and arguments
* modified JB 1/2007 turn off Y and E for all file types
****************************************************************
*cap prog drop punctuation
*prog def punctuation

gen len=length(standard_name)

** EPO Espace specific character format problems
** For files downloaded from EPO Espace & appears as &amp; 
** Also recode all common words for "AND" to &
replace standard_name = subinstr( standard_name, "&AMP;", " & ", 5)
replace standard_name = subinstr( standard_name, "+", " & ", 5)
replace standard_name = subinstr( standard_name, " AND ", " & ", 5)
replace standard_name = subinstr( standard_name, " ET ", " & ", 5)
***replace standard_name = subinstr( standard_name, " Y ", " & ", 5) if file~="CS" /* JB */
replace standard_name = subinstr( standard_name, " UND ", " & ", 5)
***replace standard_name = subinstr( standard_name, " E ", " & ", 5) if file~="CS" /* JB */
replace standard_name = subinstr( standard_name, "&", " & ",30)     /* BHH - ensure that & is separate word */

** British - specific problem with names that end in (THE) and names that start with
** THE, so remove these
replace standard_name=substr(standard_name, 1, len-5) if substr(standard_name, -5, 5)=="(THE)"
replace standard_name=substr(standard_name, 5, .) if substr(standard_name, 1, 4)=="THE "

** Replace accented characters with non-accented equivalents ****

** 1) AMADEUS - this one is easy as it uses the standard Ascii extended character set (ISO8859-1)
** The number in char() are the decimal representations of these characters
** They are decoded in I:\Projects\patents\matching\iso8859-1 table.htm
if file == "AMA" {
replace standard_name = subinstr( standard_name, char(192), "A", 30)
replace standard_name = subinstr( standard_name, char(193), "A", 30)
replace standard_name = subinstr( standard_name, char(194), "A", 30)
replace standard_name = subinstr( standard_name, char(195), "A", 30)
replace standard_name = subinstr( standard_name, char(196), "AE", 30)
replace standard_name = subinstr( standard_name, char(197), "A", 30)
replace standard_name = subinstr( standard_name, char(198), "AE", 30)
replace standard_name = subinstr( standard_name, char(199), "C", 30)
replace standard_name = subinstr( standard_name, char(200), "E", 30)
replace standard_name = subinstr( standard_name, char(201), "E", 30)
replace standard_name = subinstr( standard_name, char(202), "E", 30)
replace standard_name = subinstr( standard_name, char(203), "E", 30)
replace standard_name = subinstr( standard_name, char(204), "I", 30)
replace standard_name = subinstr( standard_name, char(205), "I", 30)
replace standard_name = subinstr( standard_name, char(206), "I", 30)
replace standard_name = subinstr( standard_name, char(207), "I", 30)
replace standard_name = subinstr( standard_name, char(208), "D", 30)
replace standard_name = subinstr( standard_name, char(209), "N", 30)
replace standard_name = subinstr( standard_name, char(210), "O", 30)
replace standard_name = subinstr( standard_name, char(211), "O", 30)
replace standard_name = subinstr( standard_name, char(212), "O", 30)
replace standard_name = subinstr( standard_name, char(213), "O", 30)
replace standard_name = subinstr( standard_name, char(214), "OE", 30)
replace standard_name = subinstr( standard_name, char(216), "O", 30)
replace standard_name = subinstr( standard_name, char(217), "U", 30)
replace standard_name = subinstr( standard_name, char(218), "U", 30)
replace standard_name = subinstr( standard_name, char(219), "U", 30)
replace standard_name = subinstr( standard_name, char(220), "UE", 30)
replace standard_name = subinstr( standard_name, char(221), "Y", 30)
replace standard_name = subinstr( standard_name, char(223), "SS", 30)
replace standard_name = subinstr( standard_name, char(224), "a", 30)
replace standard_name = subinstr( standard_name, char(225), "a", 30)
replace standard_name = subinstr( standard_name, char(226), "a", 30)
replace standard_name = subinstr( standard_name, char(227), "a", 30)
replace standard_name = subinstr( standard_name, char(228), "ae", 30)
replace standard_name = subinstr( standard_name, char(229), "a", 30)
replace standard_name = subinstr( standard_name, char(230), "ae", 30)
replace standard_name = subinstr( standard_name, char(231), "c", 30)
replace standard_name = subinstr( standard_name, char(232), "e", 30)
replace standard_name = subinstr( standard_name, char(233), "e", 30)
replace standard_name = subinstr( standard_name, char(234), "e", 30)
replace standard_name = subinstr( standard_name, char(235), "e", 30)
replace standard_name = subinstr( standard_name, char(236), "i", 30)
replace standard_name = subinstr( standard_name, char(237), "i", 30)
replace standard_name = subinstr( standard_name, char(238), "i", 30)
replace standard_name = subinstr( standard_name, char(239), "i", 30)
replace standard_name = subinstr( standard_name, char(240), "o", 30)
replace standard_name = subinstr( standard_name, char(241), "n", 30)
replace standard_name = subinstr( standard_name, char(242), "o", 30)
replace standard_name = subinstr( standard_name, char(243), "o", 30)
replace standard_name = subinstr( standard_name, char(244), "o", 30)
replace standard_name = subinstr( standard_name, char(245), "o", 30)
replace standard_name = subinstr( standard_name, char(246), "oe", 30)
replace standard_name = subinstr( standard_name, char(248), "o", 30)
replace standard_name = subinstr( standard_name, char(249), "u", 30)
replace standard_name = subinstr( standard_name, char(250), "u", 30)
replace standard_name = subinstr( standard_name, char(251), "u", 30)
replace standard_name = subinstr( standard_name, char(252), "ue", 30)
replace standard_name = subinstr( standard_name, char(253), "y", 30)
replace standard_name = subinstr( standard_name, char(255), "y", 30)
}

** 2) PATSTAT - this uses some non-standard set so I have come up with a mapping as follows.
** Again number in brackets refer to decimal representation, decode in above reference.
** However, some are control/disused characters not in reference, they are: 156, 132
** The codes have been discovered using "hexdump <filename>, tab"
if file == "EPO" {
* a grave
replace standard_name = subinstr( standard_name, char(195)+char(160), "a", 30)
* a acute
replace standard_name = subinstr( standard_name, char(195)+char(161), "a", 30)
* A acute
replace standard_name = subinstr( standard_name, char(195)+char(128), "A", 30)
* Some sort of o (Italian)
replace standard_name = subinstr( standard_name, char(195)+char(178), "o", 30)
* a circumflex
replace standard_name = subinstr( standard_name, char(195)+char(162), "a", 30)
* e circumflex
replace standard_name = subinstr( standard_name, char(195)+char(170), "e", 30)
* i circumflex
replace standard_name = subinstr( standard_name, char(195)+char(174), "i", 30)
* e acute
replace standard_name = subinstr( standard_name, char(195)+char(169), "e", 30)
* E acute
replace standard_name = subinstr( standard_name, char(195)+char(137), "E", 30)
* e grave
replace standard_name = subinstr( standard_name, char(195)+char(168), "e", 30)
* c cedilla
replace standard_name = subinstr( standard_name, char(195)+char(167), "c", 30)
* E umlaut
replace standard_name = subinstr( standard_name, char(195)+char(139), "E", 30)
* a umlaut
replace standard_name = subinstr( standard_name, char(195)+char(164), "ae", 30)
* o umlaut
replace standard_name = subinstr( standard_name, char(195)+char(182), "oe", 30)
* u umlaut
replace standard_name = subinstr( standard_name, char(195)+char(188), "ue", 30)
* A umlaut 
replace standard_name = subinstr( standard_name, char(195)+char(132), "AE", 30)
* O umlaut
replace standard_name = subinstr( standard_name, char(195)+char(150), "OE", 30)
* U umlaut
replace standard_name = subinstr( standard_name, char(195)+char(156), "UE", 30)
* N tilde
replace standard_name = subinstr( standard_name, char(195)+char(145), "N", 30)
* n tilde
replace standard_name = subinstr( standard_name, char(195)+char(177), "n", 30)

/* SOME UNKNOWN ONES - VERY RARE
* ? italian "r??ta"
replace standard_name = subinstr( standard_name, char(195)+char(180), "?", 30)
* ? belgian VERY RARE
* £
replace standard_name = subinstr( standard_name, char(195)+char(163), "?", 30) 
* little raised o
replace standard_name = subinstr( standard_name, char(195)+char(186), "?", 30)
* >>
replace standard_name = subinstr( standard_name, char(195)+char(187), "?", 30)
* Dutch IndustriÃ«le - UNKNOWN and rare
replace standard_name = subinstr( standard_name, char(195)+char(171), "?", 30)
*/
}

** This section strips out all punctuation characters
** and replaces them with nulls
replace standard_name = subinstr( standard_name, "'",  "", 30)
replace standard_name = subinstr( standard_name, ";",  "", 30) if file~="USPTO" /* BHH - remove this for USPTO */
replace standard_name = subinstr( standard_name, "^",  "", 30)
replace standard_name = subinstr( standard_name, "<",  "", 30)
replace standard_name = subinstr( standard_name, ".",  "", 30)
replace standard_name = subinstr( standard_name, "`",  "", 30)
replace standard_name = subinstr( standard_name, "_",  "", 30)
replace standard_name = subinstr( standard_name, ">",  "", 30)
replace standard_name = subinstr( standard_name, "''", "", 30)
replace standard_name = subinstr( standard_name, "!",  "", 30)
replace standard_name = subinstr( standard_name, "+",  "", 30)
replace standard_name = subinstr( standard_name, "?",  "", 30)
replace standard_name = subinstr( standard_name, "(",  "", 30)
replace standard_name = subinstr( standard_name, "£",  "", 30)
replace standard_name = subinstr( standard_name, "{",  "", 30)
replace standard_name = subinstr( standard_name, "\",  "", 30)
replace standard_name = subinstr( standard_name, ")",  "", 30)
replace standard_name = subinstr( standard_name, "$",  "", 30)
replace standard_name = subinstr( standard_name, "}",  "", 30)
replace standard_name = subinstr( standard_name, "|",  "", 30)
replace standard_name = subinstr( standard_name, ",",  "", 30)
replace standard_name = subinstr( standard_name, "%",  "", 30)
replace standard_name = subinstr( standard_name, "[",  "", 30)
replace standard_name = subinstr( standard_name, "¦",  "", 30)
replace standard_name = subinstr( standard_name, "*",  "", 30)
replace standard_name = subinstr( standard_name, "]",  "", 30)
replace standard_name = subinstr( standard_name, "/",  " ", 30) /* BHH - need to keep names separate if joined by / */
replace standard_name = subinstr( standard_name, "@",  "", 30)
replace standard_name = subinstr( standard_name, ":",  "", 30)
replace standard_name = subinstr( standard_name, "~",  "", 30)
replace standard_name = subinstr( standard_name, "#",  "", 30)
replace standard_name = subinstr( standard_name, "-",  " ", 30) /* BHH -need to keep names separate if joined by - */

replace standard_name = subinstr( standard_name, "  ", " ", 30) /* BHH -recode double space to space   */
drop len
