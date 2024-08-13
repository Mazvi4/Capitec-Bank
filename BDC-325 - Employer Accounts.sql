WITH var AS (
SELECT
	'5099773' AS employer_no_account_no
)
SELECT
	distinct
	da2.accountnumberwithcheckdigit AS Primary_Deposit_Account,
	ISNULL(ISNULL(db.branchname, db2.branchname), db3.branchname) AS Branch_Name,
	'' AS Branch_Street_Address,
	ISNULL(ISNULL(db.branchcode, db2.branchcode), db3.branchcode) AS Branch_Code,
	CASE
		WHEN dp.productgroupname NOT IN ('Loan', 'Credit Card') THEN da.accountnumberwithcheckdigit
	END AS Savings_Account,
	 	 CASE
		WHEN dp.productgroupname IN ('Loan', 'Credit Card') THEN da.accountnumberwithcheckdigit
	END AS Loan_Contract,
	CASE
		WHEN dp.productgroupname = 'Loan' THEN fca.advanceamount
		WHEN dp.productgroupname = 'Credit Card' THEN fccadc.creditlimitamount
	END AS Amount_Lost,
	 '' AS Amount_Prevented,
	 '' AS Forensics_Case_Nr,
	 '' AS SAPS_Case_Nr,
	 dc.titlename,
	 dc.initials,
	 dc.lastname,
	 dc.partyidentifiertypenumber AS id_number,
	 '' AS Postal_Address_1,
	 '' AS Postal_Address_2,
	 '' AS Postal_Address_3,
	 '' AS Postal_Code,
	 dad.addressline1text AS Home_Address_1,
	 dad.addressline2text AS Home_Address_2,
	 dad.addressline3text AS Home_Address_3,
	 dad.addresspostcode AS Post_Code,
	 '' AS Application_No,
	 CASE 
	 	  WHEN dp.productgroupname = 'Loan' THEN fladc.lastadvancedatekey::TEXT::DATE
	 	  WHEN dp.productgroupname = 'Credit Card' THEN fccadc.accountopendatekey::TEXT::DATE 
	  END AS Application_Date,
 	 'Capitec Bank' AS Application_Company,
 	 '' AS [Status],
 	 '' AS Decline_Reason_Code,
 	 '' AS If_07_Details,
 	 dc.homephonenumber AS Phone_Home,
 	 dc.mobilephonenumber AS Phone_Cell,
 	 de.employername AS Employer_Name,
 	 '' AS Gross_Salary,
 	 dc.workphonenumber AS Phone_Work,
 	 '' AS "E-mail",
 	 '' AS Ref_Title,
 	 '' AS Ref_Initials,
 	 '' AS Ref_Surname,
 	 '' AS Ref_Home_Telephone,
 	 '' AS Ref_Work_Telephone,
 	 '' AS Ref_Telephone,
 	 '' AS Fam_Title,
 	 '' AS Fam_Initials,
 	 '' AS Fam_Surname,
 	 '' AS Fam_Home_Telephone,
 	 '' AS Fam_Work_Telephone,
 	 '' AS Fam_Telephone,
 	 '' AS Credit_Card_Number,
 	 '' AS Bank_Name,
 	 '' AS Branch_Number,
 	 '' AS Bank_Account_number,
 	 '' AS Bank_Account_type,
 	 dc.clientcreatedate::DATE,
 	 trim(LEADING '0' FROM mdt.teller_no) + ' - ' + mdt.teller_name AS Create_Teller,
 	 --
 	 CASE
 	 	WHEN dp.productgroupname = 'Loan' THEN da.accountstatusname
 	 	WHEN dp.productgroupname = 'Credit Card' THEN da.accountstatusname
 	 	ELSE da.accountstatusname
 	 END AS Account_Status,
 	 dcc.clientstatusname AS Client_Handed_Over_Status,
	 CASE 
		  WHEN dp.productgroupname <> 'Loan' THEN da.accountopendate
		  ELSE NULL 
	END AS Savings_Account_Open_Date 
FROM
	edw_core_base.factclient fc
INNER JOIN edw_core_base.dimclient dc ON
	dc.clientkey = fc.clientkey
	AND dc.rowiscurrent = 1
INNER JOIN edw_ingest_syncserver.datastore_mis_dbo_d_client mdc ON mdc.cif = dc.cifnumber
INNER JOIN edw_ingest_syncserver.datastore_mis_dbo_d_teller mdt ON mdc.teller_key = mdt.teller_key
INNER JOIN edw_core_base.dimaddress dad ON dad.addresskey = fc.residentialaddresskey AND dad.rowiscurrent = 1
INNER JOIN edw_core_base.dimemployer de ON
	de.employerkey = fc.employerkey
	AND de.rowiscurrent = 1
INNER JOIN edw_core_base.dimaccount da ON
	da.clientnumber = dc.cifnumber
	AND da.rowiscurrent = 1
INNER JOIN edw_core_base.dimaccount da2 ON
	da2.accountnumber = fc.primarydepositaccountnumber
	AND da2.rowiscurrent = 1
INNER JOIN edw_core_base.dimclientcollections dcc ON dcc.clientcollectionskey = fc.clientcollectionskey AND dcc.rowiscurrent = 1
LEFT OUTER JOIN edw_core_base.factdepositaccountdailycoverage fdadc ON
	fdadc.accountkey = da.accountkey
	AND fdadc.coveragedatekey = (
	SELECT
		max(coveragedatekey)
	FROM
		edw_core_base.factdepositaccountdailycoverage)
LEFT OUTER JOIN edw_core_base.dimbranch db2 ON
	db2.branchkey = fdadc.homebranchkey
INNER JOIN edw_core_base.dimproduct dp ON
	dp.productcode = da.accounttypecode + '|' + da.accountcategorycode
LEFT OUTER JOIN (edw_core_base.factloanaccountdailycoverage fladc
INNER JOIN edw_core_base.factcapout fca ON
	fladc.accountkey = fca.accountkey) ON
	fladc.accountkey = da.accountkey
LEFT OUTER JOIN edw_core_base.factclientdailysummary fcds ON fcds.clientkey = fladc.clientkey
LEFT OUTER JOIN edw_core_base.dimbranch db ON db.branchkey = fcds.lastloanbranchkey
LEFT OUTER JOIN edw_core_base.factcreditcardaccountdailycoverage fccadc ON
	fccadc.accountkey = da.accountkey
LEFT OUTER JOIN edw_core_base.dimbranch db3 ON db3.branchkey = fccadc.openingbranchkey AND db3.rowiscurrent = 1
--LEFT OUTER JOIN edw_core_base.dimteller dt2 ON dt2.tellerkey = fca.transactiontellerkey
--LEFT OUTER JOIN edw_core_base.dimteller dt3 ON dt3.tellerkey = fccadc.
WHERE
	fc.clientsnapshotdatekey = (
	SELECT
		max(clientsnapshotdatekey)
	FROM
		edw_core_base.factclient)
	AND de.employernumberwithcheckdigit = (
	SELECT
		employer_no_account_no
	FROM
		var)
	OR da.accountnumberwithcheckdigit = (
	SELECT
		employer_no_account_no
	FROM
		var)

