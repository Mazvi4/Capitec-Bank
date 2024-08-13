SELECT
	da.accountnumberwithcheckdigit AS Account_Number_Check_Digit,
	fdadc.currentbalanceamount AS Dep_Bal,
	dc.cifwithcheckdigit AS CIF_Check_Digit,
	TRIM(TRIM(dc.firstname) + 
		CASE 
			WHEN TRIM(dc.middlefirstname) = '' THEN '' ELSE ' ' + TRIM(dc.middlefirstname)
		END + 
		CASE 
			WHEN TRIM(dc.middlesecondname) = '' THEN '' ELSE ' ' + TRIM(dc.middlesecondname)
		END + 
		CASE 
			WHEN TRIM(dc.middlethirdname) = '' THEN '' ELSE ' ' + TRIM(dc.middlethirdname)
	END) AS First_Name,
	dc.lastname AS Last_Name,
	dc.partyidentifiertypenumber AS ID_Number,
	dca.cardnumber AS Card_No,
	dca.embossedname AS Card_Name,
	dca.cardholdertypecode AS Card_Holder_Type,
	dca.cardexpirydate::DATE AS Expiry_Dates,
	dca.cardcreationdate::DATE AS Create_Date,
	dca.cardreplacetimestamp::DATE AS Replacement_Date,
	dca.cardstatusname AS Card_Status
FROM 
	edw_core_base.dimcard dca
INNER JOIN 
	edw_core_base.dimaccount da ON da.clientnumber = LEFT(dca.clientnumber, 8) AND da.rowiscurrent = 1
INNER JOIN
	edw_core_base.dimclient dc ON dc.cifwithcheckdigit = dca.clientnumber AND dc.rowiscurrent = 1
INNER JOIN
	edw_core_base.factdepositaccountdailycoverage fdadc ON fdadc.accountnumber = da.accountnumber AND snapshotdate = (select max(snapshotdate) FROM edw_core_base.factdepositaccountdailycoverage)
WHERE 
	dca.rowiscurrent = 1
AND 
--	da.accountnumberwithcheckdigit = '1361446488'
	dca.cardnumber = '179057551'
--	dc.CIF_Check_Digit = '188494383'