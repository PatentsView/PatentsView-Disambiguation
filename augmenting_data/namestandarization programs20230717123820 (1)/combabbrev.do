* combabbrev.do

* 1/2007 JBessen
*
* combine single char sequences in standard_name
* this assumes name string begins and ends with space

gen str90 outname = " "

* remove quote characters
replace standard_name = subinstr( standard_name, `"""',  "", 30)

local i = 1
while `i' <= _N {
	
	* do next name
	local j = 1
	local len = wordcount(standard_name[`i']) 

	* do next word	
	while `j' <= `len' {
		
		local outword = word( standard_name[`i'], `j')
		if  strlen("`outword'")~=1 | strlen(word( standard_name[`i'],`j'+1))!=1  {
			local outword = "`outword'"+" "
		}
		qui replace outname = outname + "`outword'" in `i'

		local j = `j' + 1
	}
	local i = `i' + 1
}

replace standard_name = outname
drop outname
