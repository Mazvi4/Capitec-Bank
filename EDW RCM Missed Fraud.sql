SELECT
dcl.clientkey
,i.incidentnumber AS CSI_Incident_number
,utd.incidentnumber as incidentnumber_UTD
,td.incidentnumber as incidentnumber_TD
,'' as STrack_Ref
,utd.cardnumber AS Card_No
,LEFT(utd.cardnumber, 6) AS BIN
,utd.trancode AS Tran_Code
,utd.trantype AS Tran_Desc
,i.createddatetime AS Incident_Date
,td.createddatetime as createddatetime_TD
,td.lastmoddatetime as lastmoddatetime_TD
,'' AS tran_id
,utd.additionaldescription AS Description
,utd.amount AS Amount
,utd.createddatetime AS CSI_Log_Date
,LEFT(td.fraudtype, 1) AS Fruad_Code
,td.fraudtype AS Fruad_Description
,td.resolution AS Summary_Resolution

FROM (
	SELECT
	amount
	,journalnumber
	,incxid
	,createddatetime
	,cardnumber
	,incidentnumber
	,trancode
	,trantype
	,additionaldescription
	,parentlink_recid
	,rowiscurrent
	,fraud
	,disputed
FROM ingest_interface_strack_refined.dbo_utdaccounttransactions 
WHERE rowiscurrent = 1) utd

INNER JOIN (
	SELECT
	fraudtype
	,createddatetime
	,lastmoddatetime
	,incidentnumber
	,resolution
	,recid
	,incidentlink_recid
	,rowiscurrent
	 FROM ingest_interface_strack_refined.dbo_unathorisedtransactiondisputes) td
ON utd.parentlink_recid = td.recid
AND utd.rowiscurrent = 1
AND td.rowiscurrent = 1

INNER JOIN(
	SELECT recid,rowiscurrent,incidentnumber,createddatetime::date,status,clientcif
	FROM ingest_interface_strack_refined.dbo_incident) AS i
ON td.incidentlink_recid = i.recid
AND i.rowiscurrent = 1

INNER JOIN(
	SELECT clientkey,cifwithcheckdigit,rowiscurrent
	FROM edw_core_base.dimclient) dcl
ON dcl.cifwithcheckdigit = i.clientcif
AND dcl.rowiscurrent = 1

where i.rowiscurrent = 1
AND LOWER(i.status) = 'closed'
AND utd.createddatetime::date = '2023-09-30'
AND utd.amount < 0
AND LOWER(utd.additionaldescription) NOT LIKE '%fee%'
AND LOWER(utd.trantype) NOT LIKE '%fee%'
AND LOWER(utd.disputed) = 'yes'
AND utd.incidentnumber IS NOT NULL

ORDER BY 
utd.createddatetime,
utd.amount
