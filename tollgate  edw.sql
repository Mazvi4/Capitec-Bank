
WITH Terminal_Type AS (
	SELECT '00' AS POS_Terminal_Type, 'NAD' AS Device_Type
	UNION ALL
	SELECT '01' AS POS_Terminal_Type, 'POS' AS Device_Type 
)
SELECT 
DISTINCT 
     datetime_req AS Tran_Date 
    ,Cast(datetime_req  AS Date ) AS [Date]
    ,tt.Device_Type        		         AS Device_Type
    ,CAST(p.pan AS VARCHAR)                AS Card_No
    , CAST (dcrdc.CardStopTimestamp  AS Date) AS Card_Stop_Date
    ,d.accountnumberwithcheckdigit        AS Account
    ,holdresponsecode                      AS Hold_Response_Key
    ,qualifycreditlimitamount              AS Initial_Cred_Lim
    ,sub.creditlimitamount                 AS Current_Cred_Lim
    ,sub.Currentbalanceamount                  AS Credit_Used
    ,arrearsamount                         AS Arrears_Amount
    ,delinquentlevelnumber                AS Delinquent_Level
    ,daysinarrearsnumber                   AS  Aging_Days_Key           
    ,sub.Currentbalanceamount - sub.creditlimitamount *-1    AS Credit_Available
    ,CASE
	    WHEN p.tran_amount_req = 0 OR LEN(p.tran_amount_req) = 0 THEN 0::NUMERIC(23,2)
		ELSE p.tran_amount_req::NUMERIC / 100
	 END 	AS tran_amount_req
    ,CASE
	    WHEN p.settle_amount_req = '0' OR LEN(p.settle_amount_req) = 0 THEN 0::NUMERIC(23,2)
		ELSE p.settle_amount_req::NUMERIC / 100
	 END 	AS settle_amount_req
    ,tp.cardtransactiontypecode              AS Tran_Type
    ,tp.cardtransactiontypename              AS Tran_Type_Desc
    ,rsp.cardresponsecode                    AS Response_Code
    ,rsp.cardresponsename                    AS Response_Code_Desc    
    ,TRIM(p.card_acceptor_id_code)           AS Merchant_Id 
    ,p.card_acceptor_name_loc                AS Merchant_Name    
    ,p.merchant_type                         AS Merchant_Type
    ,mcc.merchantcategoryname                AS Merchant_Type_Desc
    ,p.terminal_id                           AS Terminal_ID
    ,pem.pointofserviceentrymodecode         AS POS_Entry_Mode
    ,mt.messagetypecode                      AS Message_Type
    ,mt.messagetypename                      AS Message_Type_Desc
    ,p.acquiring_inst_id_code                AS Acquiring_Inst_Id_Code
    ,p.pos_card_data_input_ability           AS Pos_Card_Data_Input_Ability 
    ,p.pos_card_data_input_mode              AS Pos_Card_Data_Input_Mode
    ,p.pos_cardholder_present                AS Pos_Cardholder_Present          
FROM ingest_interface_postilion_token_refined.vwpost_tran_leg_internal p
    LEFT  JOIN (SELECT cardtransactiontypecode,cardtransactiontypename
        FROM EDW_CORE_BASE.dimcardtransactionindicators 
        WHERE rowiscurrent = 1
        GROUP BY 1,2) tp ON p.tran_type = tp.cardtransactiontypecode
    LEFT   JOIN (SELECT extendedtransactiontypecode,extendedtransactiontypename
        FROM EDW_CORE_BASE.dimcardtransactionindicators 
        WHERE rowiscurrent = 1
        GROUP BY 1,2) etc ON p.extended_tran_type = etc.extendedtransactiontypecode
    LEFT   JOIN (SELECT messagereasoncode,messagereasonname,cardresponsename
        FROM EDW_CORE_BASE.dimcardtransactionindicators 
        WHERE rowiscurrent = 1
        GROUP BY 1,2,3) mrc ON p.message_reason_code = mrc.messagereasoncode
    LEFT  JOIN (SELECT cardresponsecode,cardresponsename
        FROM EDW_CORE_BASE.dimcardtransactionindicators 
        WHERE rowiscurrent = 1
        GROUP BY 1,2) rsp ON p.rsp_code_req = rsp.cardresponsecode
    LEFT  JOIN (SELECT pointofserviceentrymodecode,pointofserviceentrymodename
        FROM EDW_CORE_BASE.dimcardtransactionindicators 
        WHERE rowiscurrent = 1
        GROUP BY 1,2) pem ON p.pos_entry_mode = pem.pointofserviceentrymodecode
    LEFT JOIN (SELECT pointofserviceconditioncode,pointofserviceconditionname
        FROM EDW_CORE_BASE.dimcardtransactionindicators 
        WHERE rowiscurrent = 1
        GROUP BY 1,2) pcc ON p.pos_condition_code = pcc.pointofserviceconditioncode
    LEFT JOIN (SELECT MessageTypeCode,MessageTypename
        FROM EDW_CORE_BASE.dimcardtransactionindicators 
        WHERE rowiscurrent = 1
        GROUP BY 1,2) mt ON p.message_type = mt.MessageTypeCode
    LEFT  JOIN (SELECT currencyalphacode, currencynumericcode
    	FROM EDW_CORE_BASE.vwdimcurrency 
    	WHERE rowiscurrent = 1
    	GROUP BY 1,2) alc ON p.tran_currency_code = alc.currencynumericcode
    LEFT JOIN (SELECT merchantcategoryname, merchantcategorycode
    	FROM EDW_CORE_BASE.dimmerchantcategory
    	WHERE rowiscurrent = 1
    	GROUP BY 1,2) mcc ON p.merchant_type = mcc.merchantcategorycode 
    LEFT JOIN edw_core_base.dimdate  dd ON dd.fulldate  = CAST (p.datetime_tran_local AS Date) AND dd.rowiscurrent =1 
    LEFT JOIN   edw_core_base.dimcard AS dcrdc ON dcrdc.cardnumber  = p.card_no_id  
    AND dcrdc.rowiscurrent =1 
    LEFT JOIN ( SELECT
        fccadc.clientkey , 
        fccadc.accountkey , 
        fccadc.creditlimitamount,
        fccadc.currentbalanceamount,
        fccadc.arrearsamount,
        fccadc.coveragedatekey,
        fccadc.coveragedate,
        fccadc.accountnumber,
        fccadc.qualifycreditlimitamount
        fccadc.delinquencytext ,
        fccadc.stopsonaccountcount,
        dcl.cifwithcheckdigit,
        fccadc.delinquentlevelnumber,
        fccadc.daysinarrearsnumber,
        row_number() over (partition by fccadc.accountkey order by fccadc.coveragedatekey desc) as RowRank
    FROM edw_core_base.factcreditcardaccountdailycoverage AS fccadc
    LEFT JOIN edw_core_base.dimclient AS dcl ON dcl.clientkey = fccadc.clientkey  
    WHERE dcl.rowiscurrent =1 
   ) AS  sub ON sub.cifwithcheckdigit =  dcrdc.clientnumber 
   LEFT   JOIN edw_core_base.dimaccount d ON d.accountkey =sub.accountkey AND d.rowiscurrent =1
   LEFT JOIN edw_core_base.dimdate d2  ON d2.fulldate  = CAST(dcrdc.cardstoptimestamp AS Date)  AND d2.rowiscurrent =1  
   LEFT JOIN Terminal_Type tt ON p.pos_terminal_type = tt.pos_terminal_type 
WHERE p.TRAN_POSTILION_ORIGINATED = 0
AND RowRank =1 
--AND p.TRAN_REVERSED = 0
AND   hold_response_key <> '114'
AND   p.merchant_type ='4784'
AND CAST(p.datetime_tran_local AS Date) >= CAST (dcrdc.CardStopTimestamp  AS Date) 
  -- AND p.datetime_tran_local BETWEEN '2024-04-17'::TIMESTAMP AND '2023-04-17 23:59:59.000'
AND d.accountnumberwithcheckdigit 
IN ('6000328264','6000522923','6007904287','6008726161','6008555251','6006676590','6009114747')
AND p.rowiscurrent = 1
--AND p.card_acceptor_id_code = '2815215'
ORDER BY p.datetime_tran_local
--LIMIT 50
;

-
