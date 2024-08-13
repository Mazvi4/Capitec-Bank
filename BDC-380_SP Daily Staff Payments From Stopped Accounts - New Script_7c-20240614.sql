WITH vwF_Transaction AS ( --main praimary virtual table add timekey value, add transaction_code and filter with dates
      SELECT
                f.Accountkey,
                ts.transactioncode,
                f.ClientKey,
                f.Transactionamount,
                f.Transactiondatekey,
                di.fulldate,
				f.TRANSACTIONINDICATORSKEY,
				f.postedtransactiondatekey,
				f.postedtransactiontimekey,
				ti.Timekey,
                f.JOURNALNUMBER
                --CAST(f.postedtransactiondatekey AS varchar) :: date AS Tran_Date,
            FROM 
                edw_core_base.FactTransaction AS f
            INNER JOIN 
                      edw_core_base.vwdimtransactionclassification AS ts ON ts.transactionclassificationkey = f.transactionclassificationkey 
            INNER JOIN 
                      edw_core_base.DIMTIME AS ti ON f.postedtransactiontimekey = ti.timekey
            INNER JOIN 
                      edw_core_base.DIMDATE AS di ON f.postedtransactiondatekey = di.datekey
            WHERE     
                 EXTRACT(YEAR FROM f.postedtransactiondatekey::varchar::date) >= EXTRACT(YEAR FROM CURRENT_DATE-30) 
              AND
                 EXTRACT(MONTH FROM f.postedtransactiondatekey::varchar::date) >= EXTRACT(MONTH FROM CURRENT_DATE-30)
)--select * from vwF_Transaction LIMIT 5 --##test results set here
,
-------------------------------------------------------------------------------------------------------------------------------------------
F_Transaction_Description AS (
    SELECT
	     DISTINCT ftd.Accountkey
	    ,ftd.TRANSACTIONINDICATORSKEY
	FROM edw_core_base.vwFactTransaction AS ftd
        INNER JOIN edw_core_base.vwdimtransactionclassification as fk 
		        ON ftd.transactionclassificationkey = fk.transactionclassificationkey
    WHERE ftd.TRANSACTIONINDICATORSKEY IN (50,73) 
)--select * from F_Transaction_Description LIMIT 5 --##test results set here
,
-------------------------------------------------------------------------------------------------------------------------------------------
F_Transaction_additional AS ( --use results from join to pass records to prepjoin subsection while filtering and aligning  accountkey values
    SELECT 
        ft2.Transactionamount,
        ft2.JOURNALNUMBER,
        ft2.Transactiondatekey AS Datekey,
        ft2.Timekey,
		ft2.TRANSACTIONINDICATORSKEY,
        ft2.transactioncode,
        ft2.Accountkey,
        ft2.ClientKey
    FROM
        vwF_Transaction as ft2
    INNER JOIN
        F_Transaction_Description as fd ON fd.Accountkey = ft2.Accountkey
)--select * from F_Transaction_additional LIMIT 5 --##test results set here
,
-------------------------------------------------------------------------------------------------------------------------------------------
PrepJoin AS ( --all filtered records results join and prepare for final query whil filtering amount
    SELECT
        ft.Accountkey AS vwF_Transaction_Accountkey,
        ft.transactioncode AS vwF_Transaction_transactioncode,
        ft.ClientKey AS vwF_Transaction_Client_Key,
        ft.Transactionamount AS Main_Amount,
        ft2.TRANSACTIONINDICATORSKEY,
        ft2.Transactionamount AS Balance_Amount,
        ft2.JOURNALNUMBER,
        ft2.Datekey,
        ft2.Timekey,
        ft2.transactioncode,
        ft2.Accountkey,
        ft2.ClientKey
    FROM
        vwF_Transaction ft
    INNER JOIN
        F_Transaction_additional ft2 ON ft2.Datekey = ft.Transactiondatekey 
                                       AND ft2.Timekey = ft.Timekey 
                                       AND ft2.JOURNALNUMBER = ft.JOURNALNUMBER
                                       AND ft2.Transactionamount = ft.Transactionamount * -1 
                                       AND ft2.Accountkey <> ft.Accountkey 
                                       AND ft2.ClientKey <> ft.ClientKey
    WHERE
        ft.Transactionamount > 0
)--select * from PrepJoin WHERE Journalnumber in('539953') LIMIT 20 --##test results set here
,
-------------------------------------------------------------------------------------------------------------------------------------------
FinalSet AS ( --prepare all subset results, combine and join all records together in this section
    SELECT DISTINCT 
        dc.CIF_Check_Digit,
        dc.Main_First_Name,
        dc.Main_Last_Name,
        dc.Main_Employer_Name,
        da.Staff_Account,
        dtc.Main_Tran_Code,
        dtc.Main_Tran_Desc,
        a.Main_Amount,
        da2.Stopped_Account,
        dc2.Balance_First_Name,
        dc2.Balance_Last_Name,
        de2.Balance_Employer_Name,
        dd.Tran_Date AS Tran_Date,
        dt.Tran_Time AS Tran_Time,
        dtc2.Balance_Tran_Code,
        --a.TRANSACTIONINDICATORSKEY AS Transactionindicator,
        dtc2.Balance_Tran_Desc,
        a.Balance_Amount,
        a.JOURNALNUMBER,
        ROW_NUMBER () OVER (PARTITION BY dc.CIF_Check_Digit ORDER BY dd.Tran_Date DESC,dt.Tran_Time DESC) AS rn
	FROM
        PrepJoin as a
    INNER JOIN --Extract CIF_Check_Digit, Main_First_Name, Main_Last_Name, Main_Employer_Name
	        (SELECT 
                  cd.clientkey,
				  ed.employerkey,
				  ed.employername AS Main_Employer_Name,
				  cd.cifwithcheckdigit AS  CIF_Check_Digit,
				  cd.firstname || ' ' || cd.middlefirstname || ' ' || cd.middlesecondname || ' ' || cd.middlethirdname AS Main_First_Name,
				  cd.lastname AS Main_Last_Name
             FROM edw_core_base.factclient AS cf
                  INNER JOIN
                            edw_core_base.DIMCLIENT AS cd ON cf.clientkey = cd.clientkey
				  INNER JOIN
				            edw_core_base.dimemployer AS ed ON cf.employerkey = ed.employerkey
				         AND
				            cf.clientsnapshotdatekey = (SELECT MAX(clientsnapshotdatekey) FROM edw_core_base.factclient)
		    ) as dc ON dc.ClientKey = a.vwF_Transaction_Client_Key
	INNER JOIN --Extract Staff_Account
	        (SELECT
			      Accountkey,
				  ACCOUNTNUMBERWITHCHECKDIGIT AS Staff_Account
			FROM edw_core_base.DIMACCOUNT
			WHERE
			     rowiscurrent = 1
			)as da ON da.Accountkey = a.vwF_Transaction_Accountkey
	INNER JOIN --Extract Main_Tran_Code & Main_Tran_Desc
	        (SELECT 
			      transactioncode AS Main_Tran_Code,
				  Transactionclassificationname AS Main_Tran_Desc
			 FROM edw_core_base.vwdimtransactionclassification
			) as dtc ON dtc.Main_Tran_Code = a.vwF_Transaction_transactioncode 
    INNER JOIN --Extract Stopped_Account
	        (SELECT
			      Accountkey,
				  ACCOUNTNUMBERWITHCHECKDIGIT AS Stopped_Account
			FROM edw_core_base.DIMACCOUNT
			) as da2 ON da2.Accountkey = a.Accountkey
    INNER JOIN --Extract Tran_Date
	        (SELECT
			      DateKey,
				  FULLDATE AS Tran_Date
             FROM edw_core_base.DIMDATE
             )as dd ON dd.DateKey = a.Datekey 
    INNER JOIN --Extract Tran_Time
	        (SELECT
			      TimeKey,
				  CAST(EXTRACT(HOUR FROM FULLTIME) AS VARCHAR) || 'h' || CAST(EXTRACT(MINUTE FROM FULLTIME) AS VARCHAR) AS Tran_Time
		    FROM edw_core_base.DIMTIME 
		    ) as dt ON dt.TimeKey = a.timekey 
    INNER JOIN --Extract Balance_Tran_Code & Balance_Tran_Desc
	        (SELECT
			      transactioncode AS Balance_Tran_Code,
				  Transactionclassificationname AS Balance_Tran_Desc
            FROM edw_core_base.vwdimtransactionclassification
            WHERE
                 rowiscurrent = 1
			) as dtc2 ON dtc2.Balance_Tran_Code = a.transactioncode
    INNER JOIN --Extract Balance_First_Name, Balance_Last_Name
	        (SELECT
			      ClientKey,
			      firstname || ' ' || middlefirstname || ' ' || middlesecondname || ' ' || middlethirdname AS Balance_First_Name,
			      LastName AS Balance_Last_Name
            FROM edw_core_base.DIMCLIENT
			) as dc2 ON dc2.ClientKey = a.ClientKey 
    INNER JOIN --Extract Balance_Employer_Name
	        (SELECT
			      EmployerKey,
			      EMPLOYERNAME AS Balance_Employer_Name
			FROM edw_core_base.dimemployer
			WHERE
			    rowiscurrent = 1
			) as de2 ON de2.EmployerKey = dc.EmployerKey
) 
SELECT * FROM FinalSet 
WHERE Balance_Employer_Name LIKE ('%CAPITEC%')
  AND 
     rn = 1
  AND
     CIF_Check_Digit in ('100044794') --SEARCH INPUT HERE
--LIMIT 20;

