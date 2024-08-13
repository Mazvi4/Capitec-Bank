
with accouns_with_tellers as ( --- filters acounts and gets create tellers from edw tables
	with client as (
		select 
			f.primarydepositaccountnumber 
			,da.accountnumberwithcheckdigit
			,d.cifnumber 
			,d.cifwithcheckdigit 
		from edw_core_base.factclient as f
		inner join edw_core_base.dimclient as d on d.clientkey = f.clientkey 
		inner join edw_core_base.dimaccount as da on da.accountnumber = f.primarydepositaccountnumber 
		where da.accountnumberwithcheckdigit in ('1612002992','1372014002','1400109564')    
		and f.clientsnapshotdatekey = (select max(clientsnapshotdatekey) from edw_core_base.factclient) 
		and da.rowiscurrent = 1
		and d.rowiscurrent = 1
	),
	first_card_teller as (
		select 
			c.clientnumber 
			,c.cardactivatedtellernumber 
			,dt.tellerkey
			,row_number() over(partition by clientnumber order by cardactivetimestamp) as row_num
		from edw_core_base.dimcard as c
		inner join edw_core_base.dimteller as dt on cast(dt.tellernumber as int) =  c.cardactivatedtellernumber
		where c.clientnumber in (select cifwithcheckdigit from client)
		and c.rowiscurrent = 1 
		and dt.rowiscurrent = 1
	)
	select 
		c.cifnumber as cif
		,ct.tellerkey as teller_key
		,c.accountnumberwithcheckdigit as primary_deposit_account
	from client as c
	inner join first_card_teller as ct on ct.clientnumber = c.cifwithcheckdigit
	and ct.row_num = 1 	
) 
SELECT
	DISTINCT
	dc.partyidentifiertypenumber AS ID_Number,
	dc.cifwithcheckdigit AS CIF_Check_Digit,
	dc.clientcreatedate AS Client_Create_Date,
	-- trim(LEADING '0' FROM mdt.teller_no) + ' - ' + mdt.teller_name AS Create_Teller,
	trim(LEADING '0' FROM dt.tellernumber) + ' - ' + dt.tellername AS Create_Teller,
	da2.accountnumberwithcheckdigit AS Primary_Deposit_Account,
	fdadc.currentbalanceamount AS Dep_Bal,
	CASE 
		WHEN fdadc.stopsonaccountcount > 0 THEN 'Yes' ELSE 'No' 
	END AS Account_Stopped,
	da.accountnumberwithcheckdigit AS Loan_Contract,
	fladc.lastadvancedatekey::TEXT::DATE AS Application_Date,
	cao.clienttype AS Client_Type,
	fladc.lastarrearsdatekey::TEXT::DATE AS Default_Date,
	fladc.arrearsinstallmentcount AS Num_Arrears_Installments,
	sma.loanpurpose AS Loan_Purpose,
	sma.retailername AS Retailer_Name,
	smr.privatesector as MIE,
	fladc.loanbalanceamount AS Loan_Bal, 
	fladc.loanarrearsamount AS Loan_Arrears,
	dp.productname AS Product_Desc,
	CASE 
		WHEN dp.productgroupname IN ('Loan', 'Facility', 'Credit Card') 
		THEN trim(LEADING '0' FROM dt2.tellernumber) + ' - ' + dt2.tellername 
		ELSE NULL 
	END AS Loan_Teller,
	db.branchname AS Branch_Name,
	db.provincename AS Province,
	dcc.clientstatusname AS Client_Handed_Over_Status, 
	de.employernumberwithcheckdigit AS Employer_No,
	de.employername AS Employer_Name,
	de.employerindustrytypename AS Industry,
--	de.insertdate,
	fca.advanceamount AS Capout,
	gd.score_mi AS Prism_Score,
	(round(fladc.loanarrearsamount/fladc.loanbalanceamount,2) * 100)::TEXT + ' %' AS Client_Arrears_Ratio
FROM edw_core_base.factloanaccountdailycoverage fladc
INNER JOIN edw_core_base.dimaccount da ON da.accountkey = fladc.accountkey AND da.rowiscurrent = 1
LEFT OUTER JOIN edw_core_base.factcapout fca ON fca.accountkey = fladc.accountkey -- AND fca.capoutdatekey = (select max(capoutdatekey) FROM edw_core_base.factcapout)
INNER JOIN edw_core_base.dimclient dc ON dc.clientkey = fladc.clientkey AND dc.rowiscurrent = 1
--INNER JOIN edw_ingest_syncserver.datastore_mis_dbo_d_client mdc ON mdc.cif = dc.cifnumber
--         INNER JOIN edw_ingest_syncserver.datastore_mis_dbo_d_teller mdt ON mdc.teller_key = mdt.teller_key
INNER JOIN accouns_with_tellers awt on awt.cif = dc.cifnumber
INNER JOIN edw_core_base.factclient fcl ON fcl.clientkey = dc.clientkey AND fcl.clientsnapshotdatekey = (SELECT max(clientsnapshotdatekey) FROM edw_core_base.factclient)
INNER JOIN edw_core_base.dimemployer de ON de.employerkey = fcl.employerkey AND de.rowiscurrent = 1
INNER JOIN edw_core_base.dimproduct dp ON dp.productkey = fladc.productkey AND dp.rowiscurrent = 1
INNER JOIN edw_core_base.dimteller dt ON dt.tellerkey = awt.teller_key AND dt.rowiscurrent = 1
INNER JOIN edw_core_base.dimaccount da2 ON da2.accountnumber = fcl.primarydepositaccountnumber AND da2.rowiscurrent = 1
INNER JOIN edw_core_base.factdepositaccountdailycoverage fdadc ON fdadc.accountkey = da2.accountkey AND fdadc.coveragedatekey = (select max(coveragedatekey) FROM edw_core_base.factdepositaccountdailycoverage)
LEFT OUTER JOIN edw_core_base.dimbranch db ON db.branchkey = fca.transactionbranchkey-- AND db.rowiscurrent = 1
LEFT OUTER JOIN edw_core_base.dimteller dt2 ON dt2.tellerkey = fladc.loanofficertellerkey AND dt2.rowiscurrent = 1
LEFT OUTER JOIN edw_core_base.dimclientcollections dcc ON dcc.clientcollectionskey = fladc.clientcollectionskey AND dcc.rowiscurrent = 1
INNER JOIN edw_noncore_base.creditapplicationoutcome cao ON cao.cifwithcheckdigit = dc.cifwithcheckdigit 
	AND cao.clienttype IN ('a.NO Prev G1 and No PrevLoan','b.Existing G1 and No PrevLoan')
	AND cao.lrid_date BETWEEN '2023/04/04' AND '2023/06/04'
LEFT JOIN ingest_interface_vmax_refined.dbo_smapplication sma ON sma.logicalrecordid = cao.logical_record_id
LEFT JOIN ingest_interface_vmax_refined.dbo_smsmresults smr ON smr.logicalrecordid = cao.logical_record_id
LEFT JOIN ingest_interface_vmax_refined.dbo_GDCreditPolicyStrategy gd ON gd.logicalrecordid = cao.logical_record_id
WHERE fladc.coveragedatekey = (select max(coveragedatekey) FROM edw_core_base.factloanaccountdailycoverage)
AND Application_Date BETWEEN '2023/04/04' AND '2023/06/04'
--AND dc.cifwithcheckdigit IN ('383852986', '282233555', '293432023')
--AND Default_Date BETWEEN '2022/08/25' AND '2022/10/25'
--AND db.branchcode = '4377' 
--AND Product_Desc <> 'Multi Loan' 
--ORDER BY Application_Date DESC
AND Primary_Deposit_Account IN (select primary_deposit_account from accouns_with_tellers) --('1612002992','1372014002','1400109564')
--AND Loan_Arrears > '5000'
--AND Num_Arrears_Installments >= '2'
GROUP BY
	dc.partyidentifiertypenumber,
	dc.cifwithcheckdigit,
	Client_Create_Date,
	Create_Teller,
	Dep_Bal,
	da2.accountnumberwithcheckdigit,
	Account_Stopped,
	Loan_Contract,
	Application_Date,
	cao.clienttype,
	Default_Date,
	Num_Arrears_Installments,
	Loan_Purpose,
	Retailer_Name,
	MIE,
	Loan_Bal,
	Loan_Arrears,
	Product_Desc,
	Loan_Teller,
	Branch_Name,
	Province,
	Client_Handed_Over_Status,
	Employer_No,
	Employer_Name,
	Industry,
	Capout,
	Prism_Score,
	Client_Arrears_Ratio
ORDER BY 
	dc.partyidentifiertypenumber DESC ;