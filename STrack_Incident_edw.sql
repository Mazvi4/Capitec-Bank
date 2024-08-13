WITH CTE AS (
    SELECT
        ec.INCIDENTNUMBER AS Incident_number,
        fc.CIFWITHCHECKDIGIT AS cif_check_digit,
        fc.PARTYIDENTIFIERTYPENUMBER AS id_number,
        fc.FIRSTNAME AS first_name,
        fc.LASTNAME AS last_name,
        ac.PRIMARYDEPOSITACCOUNTNUMBER AS primary_deposit_account,
        ec.noncbclientidnumber AS non_cb_client_id_number,
        cast(ec.CREATEDDATETIME AS date) AS incident_create_date,
        ec.profilefullname AS owner_full_name,
        ec.actualcategory AS actual_category,
        ec.actualsubcategory AS actual_sub_category,
        ec.actualissue AS actual_issue,
        ec.subject,
        ec.symptom,
        ec.resolution,
        ROW_NUMBER() OVER (PARTITION BY ec.INCIDENTNUMBER ORDER BY ec.CREATEDDATETIME DESC) AS rn
    FROM
        edw_core_base.factclient AS ac
    INNER JOIN
        edw_core_base.DIMCLIENT AS fc ON ac.CLIENTKEY = fc.clientkey
    INNER JOIN
        ingest_interface_strack_refined.dbo_incident AS ec ON fc.CIFWITHCHECKDIGIT = ec.CLIENTCIF
    WHERE
        ac.clientsnapshotdatekey = (
            SELECT MAX(clientsnapshotdatekey)
            FROM edw_core_base.factclient
        )
        AND fc.rowiscurrent = 1
        )
SELECT *
FROM CTE
WHERE rn = 1;