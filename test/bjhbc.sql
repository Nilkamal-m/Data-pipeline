WITH 
-- 1. Aggregate unique topics/entities per conversation
conversation_topics AS (
    SELECT 
        conversation_id,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT detail_entity), ', ') AS conversation_topic
    FROM interactions
    WHERE detail_entity IS NOT NULL AND detail_entity <> ''
    GROUP BY conversation_id
),

-- 2. Aggregate plugin call states per interaction
plugin_aggregates AS (
    SELECT 
        interaction_id,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT CASE WHEN NOT served AND NOT used THEN plugin_name END), ', ') AS unsuccessful_plugins,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT CASE WHEN served THEN plugin_name END), ', ') AS plugin_served,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT CASE WHEN served AND used THEN plugin_name END), ', ') AS plugin_used
    FROM plugin_calls
    GROUP BY interaction_id
),

-- 3. Aggregate resource info, citations, and ticket metadata per interaction
resource_aggregates AS (
    SELECT 
        interaction_id,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT detail_domain), ', ') AS resource_domain,
        COUNT(DISTINCT resource_id) AS no_of_citations,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT detail_name), ', ') AS content_item_name,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT detail_external_resource_id), ', ') AS content_item_id,
        MAX(CASE WHEN type = 'RESOURCE_TYPE_TICKET' THEN 'user initiated ticket' ELSE '' END) AS ticket_type,
        ARRAY_JOIN(ARRAY_AGG(DISTINCT CASE WHEN type = 'RESOURCE_TYPE_TICKET' THEN detail_external_resource_id END), ', ') AS ticket_id
    FROM plugin_resources
    GROUP BY interaction_id
)

-- 4. Final join and assembly matching raw_interactions_table
SELECT 
    -- Base Interaction Details
    ui.created_time                             AS "Timestamp",
    ui.conversation_id                          AS "Conversation ID",
    ui.id                                       AS "Interaction ID",
    ui.type                                     AS "Interaction Type",
    COALESCE(c.primary_domain, '')              AS "Conversation Domain",
    COALESCE(ct.conversation_topic, '')         AS "Conversation Topic",
    COALESCE(ui.detail_content, '')             AS "Interaction Content",
    COALESCE(bot.detail_content, '')            AS "Bot Response",

    -- Plugin Details
    COALESCE(pa.unsuccessful_plugins, '')       AS "Unsuccessful Plugins",
    COALESCE(pa.plugin_served, '')              AS "Plugin Served",
    COALESCE(pa.plugin_used, '')                AS "Plugin Used",

    -- Resource & Citation Details
    COALESCE(ra.resource_domain, '')            AS "Resource Domain",
    COALESCE(ra.no_of_citations, 0)             AS "No of Citations",
    COALESCE(ra.content_item_name, '')          AS "Content Item Name",
    COALESCE(ra.content_item_id, '')            AS "Content Item ID",
    COALESCE(ra.ticket_type, '')                AS "Ticket Type",
    COALESCE(ra.ticket_id, '')                  AS "Ticket ID",

    -- Metadata & User Info
    COALESCE(ui.platform, '')                   AS "Interaction Surface",
    COALESCE(u.user_preferred_language, '')     AS "User Preferred Language",
    
    -- External System Placeholders
    CAST(NULL AS VARCHAR)                       AS "User Department",
    CAST(NULL AS VARCHAR)                       AS "User Location",
    CAST(NULL AS VARCHAR)                       AS "User Country"

FROM interactions ui

-- Get domain from conversations
LEFT JOIN conversations c 
    ON ui.conversation_id = c.id

-- Get conversation topics (aggregated)
LEFT JOIN conversation_topics ct 
    ON ui.conversation_id = ct.conversation_id

-- Get bot response (self-join on parent_interaction_id)
LEFT JOIN interactions bot 
    ON bot.parent_interaction_id = ui.id 
   AND bot.actor = 'bot'

-- Get plugin details
LEFT JOIN plugin_aggregates pa 
    ON ui.id = pa.interaction_id

-- Get resource details
LEFT JOIN resource_aggregates ra 
    ON ui.id = ra.interaction_id

-- Get user metadata
LEFT JOIN users u 
    ON ui.user_id = u.id

WHERE ui.actor = 'user'
  -- Iceberg Timestamp Filtering (Athena Trino format):
  AND ui.last_updated_time >= TIMESTAMP '2025-07-15 00:00:00.000 UTC'
  AND ui.last_updated_time <= TIMESTAMP '2025-07-15 23:59:59.999 UTC'
ORDER BY ui.id DESC;
