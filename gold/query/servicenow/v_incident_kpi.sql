-- ==============================================================================
-- Gold Mart Query: v_incident_kpi.sql
-- Source System: servicenow
-- Hierarchy: s3://<bucket>/gold/query/servicenow/v_incident_kpi.sql
-- Produces:
--   - Athena Mandatory View : v_incident_kpi
--   - Downstream MySQL Mart : gold_tbl_incident_kpi (View: v_incident_kpi)
-- S3 Storage: s3://<bucket>/gold/data/servicenow/incident_kpi/
-- ==============================================================================

SELECT
    COALESCE(priority, 'Unassigned')                         AS priority_level,
    COALESCE(incident_state, 'Unknown')                      AS incident_state,
    COALESCE(category, 'General')                            AS incident_category,
    COUNT(DISTINCT sys_id)                                   AS total_incidents,
    SUM(CASE WHEN _is_deleted = 'Y' THEN 1 ELSE 0 END)       AS soft_deleted_incidents,
    SUM(CASE WHEN incident_state IN ('Closed', 'Resolved', '7', '6') THEN 1 ELSE 0 END) AS resolved_incidents,
    SUM(CASE WHEN incident_state NOT IN ('Closed', 'Resolved', '7', '6') THEN 1 ELSE 0 END) AS active_open_incidents,
    CURRENT_TIMESTAMP                                        AS _data_as_of
FROM
    tbl_incident
GROUP BY
    COALESCE(priority, 'Unassigned'),
    COALESCE(incident_state, 'Unknown'),
    COALESCE(category, 'General');
