SELECT
    case_id,
    case_creation_date,
    case_client_surname,
    case_id_passport_no,
    case_client_first_name,
    serving_consultant_cp_no
FROM ingest_interface_aps_syncserver.datastore_mis_dbo_f_documentum_case_folder
WHERE case_creation_date > '2023-02-15 06:00:00.000' AND case_id_passport_no = '1007186126086';

