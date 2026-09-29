-- ==============================================================================
-- Gold Mart Query: incident_kpi.sql
-- Source System: servicenow
-- Hierarchy: s3://<bucket>/gold/query/servicenow/incident_kpi.sql
-- Produces:
--   - Athena Physical Table : gold_servicenow_incident_kpi (Iceberg Table)
--   - Downstream MySQL Mart : gold_servicenow_incident_kpi (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/servicenow/incident_kpi/
-- ==============================================================================

SELECT
    CAST(COALESCE(priority, 'Unassigned') AS STRING)                         AS priority_level,
    CAST(COALESCE(incident_state, 'Unknown') AS STRING)                      AS incident_state,
    CAST(COALESCE(category, 'General') AS STRING)                            AS incident_category,
    CAST(COUNT(DISTINCT sys_id) AS STRING)                                   AS total_incidents,
    CAST(SUM(CASE WHEN _is_deleted = 'Y' THEN 1 ELSE 0 END) AS STRING)       AS soft_deleted_incidents,
    CAST(SUM(CASE WHEN incident_state IN ('Closed', 'Resolved', '7', '6') THEN 1 ELSE 0 END) AS STRING) AS resolved_incidents,
    CAST(SUM(CASE WHEN incident_state NOT IN ('Closed', 'Resolved', '7', '6') THEN 1 ELSE 0 END) AS STRING) AS active_open_incidents,
    CURRENT_TIMESTAMP                                                        AS _data_as_of
FROM
    tbl_incident
GROUP BY
    COALESCE(priority, 'Unassigned'),
    COALESCE(incident_state, 'Unknown'),
    COALESCE(category, 'General');
