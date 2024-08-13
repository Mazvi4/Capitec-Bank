SELECT
	dc.partyidentifiertypenumber AS ID_Number,
	dc.cifwithcheckdigit AS CIF_Number,
	TRIM(TRIM(dc.titlename) + ' ' + TRIM(dc.firstname) + 
		CASE 
			WHEN TRIM(dc.middlefirstname) = '' THEN '' ELSE ' ' + TRIM(dc.middlefirstname)
		END + 
		CASE 
			WHEN TRIM(dc.middlesecondname) = '' THEN '' ELSE ' ' + TRIM(dc.middlesecondname)
		END + 
		CASE 
			WHEN TRIM(dc.middlethirdname) = '' THEN '' ELSE ' ' + TRIM(dc.middlethirdname)
		END + 
		CASE 
			WHEN TRIM(dc.lastname) = '' THEN '' ELSE ' ' + TRIM(dc.lastname)
	END) AS Title_FirstNames_LastName,
	dc.mobilephonenumber AS Phone_Cell,
	dc.workphonenumber AS Phone_Work,
	dc.clientcreatedate::DATE AS Client_Create_Date,
	--fc.PrimaryDepositAccountNumber AS Primary_Deposit_Account,
	dac.accountnumberwithcheckdigit AS Primary_Deposit_Account, --will accountnumberwithcheckdigit from dimaccount with 10digits
	dac.accountopendate::DATE AS Account_Open_Date,
	fdadc.lastcusttrandatekey::TEXT::DATE AS Last_Cust_Tran_Date,
	fdadc.currentbalanceamount AS Account_Balance,
--	fladc.loanbalanceamount AS Loan_Balance,
--	fladc.loanarrearsammount AS Loan_Arrears,
--	CASE when fladc.loanbalanceamount >= 0 THEN 'No Loan' ELSE '[fladc.loanbalanceamount]' END AS Loan_Balance_2,
--	COALESCE(fladc.loanbalanceamount, 'No Loan') AS Loan,
--	dcps.clientdepositstatusname AS Client_Deposit_Status_Desc,
	CASE WHEN fdadc.stopsonaccountcount > 0 THEN 'Yes' ELSE 'No' END AS Account_Stopped,
	REPLACE(REPLACE(TRIM(dad.addressline1text) + ', ' + TRIM(dad.addressline2text) + ', ' + TRIM(dad.addressline3text) + ', ' + TRIM(dad.addresscityname),', , ',', '),', , ',', ') as Home_Address,
--	dad.addresscityname AS City,
	de.employername AS Employer_Name
FROM 
	edw_core_base.factclient fc
INNER JOIN 
	edw_core_base.dimclient dc ON dc.cifnumber = fc.cifnumber and dc.rowiscurrent = 1 AND fc.clientsnapshotdatekey = (select max(clientsnapshotdatekey) FROM edw_core_base.factclient)
INNER JOIN
	edw_core_base.dimaccount dac ON dac.accountnumber = fc.primarydepositaccountnumber AND dac.rowiscurrent = 1
INNER JOIN 
	edw_core_base.factdepositaccountdailycoverage fdadc ON fdadc.accountkey = dac.accountkey AND fdadc.snapshotdate = (select max(snapshotdate) FROM edw_core_base.factdepositaccountdailycoverage)
INNER JOIN
	edw_core_base.dimaddress dad ON dad.addresskey = fc.residentialaddresskey AND dad.rowiscurrent = 1
INNER JOIN 
	edw_core_base.dimemployer de ON de.employerkey = fc.employerkey AND de.rowiscurrent = 1
WHERE
	 fc.PrimaryDepositAccountNumber in ('148713635', '162370454', '152902882', '164642653')