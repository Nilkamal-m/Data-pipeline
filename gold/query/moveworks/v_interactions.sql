-- ==============================================================================
-- Gold Mart Query: v_interactions.sql
-- Source System: moveworks
-- Hierarchy: s3://<bucket>/gold/query/moveworks/v_interactions.sql
-- Produces:
--   - Athena Mandatory View : v_interactions
--   - Downstream MySQL Mart : gold_tbl_interactions (View: v_interactions)
-- S3 Storage: s3://<bucket>/gold/data/moveworks/interactions/
--
-- Description:
--   Aggregates user-led Moveworks interactions with:
--   - Parent bot responses
--   - Conversation-level topics/entities & primary domain
--   - Plugin invocation status (unsuccessful, served, used)
--   - Plugin resources, citations, content items, and ticket generation
--   - User language preferences and identity placeholders
-- ==============================================================================

WITH
    conversation_topics AS (
        -- Aggregate distinct entity topics across all interactions in each conversation
        SELECT
            conversation_id,
            array_join (
                array_agg (DISTINCT trim(detail_entity)),
                ', '
            ) AS conversation_topic
        FROM tbl_interactions
        WHERE
            _is_current = 'Y'
            AND _is_deleted = 'N'
            AND detail_entity IS NOT NULL
            AND trim(detail_entity) != ''
            AND lower(trim(detail_entity)) != 'nan'
        GROUP BY
            conversation_id
    ),
    bot_responses AS (
        -- Extract the first bot response interaction replying to a parent user interaction
        SELECT
            parent_interaction_id,
            detail_content AS bot_response
        FROM (
                SELECT
                    parent_interaction_id, detail_content, ROW_NUMBER() OVER (
                        PARTITION BY
                            parent_interaction_id
                        ORDER BY id ASC -- mirrors pandas iloc[0] based on record order
                    ) AS rn
                FROM tbl_interactions
                WHERE
                    _is_current = 'Y'
                    AND _is_deleted = 'N'
                    AND lower(actor) = 'bot'
                    AND parent_interaction_id IS NOT NULL
                    AND detail_content IS NOT NULL
                    AND trim(detail_content) != ''
            ) sub
        WHERE
            rn = 1
    ),
    plugin_aggregates AS (
        -- Aggregate plugin execution metrics per interaction
        SELECT
            interaction_id,
            array_join (
                array_agg (
                    DISTINCT CASE
                        WHEN (
                            served = false
                            OR served IS NULL
                        )
                        AND (
                            used = false
                            OR used IS NULL
                        ) THEN plugin_name
                    END
                ),
                ', '
            ) AS unsuccessful_plugins,
            array_join (
                array_agg (
                    DISTINCT CASE
                        WHEN served = true THEN plugin_name
                    END
                ),
                ', '
            ) AS plugin_served,
            array_join (
                array_agg (
                    DISTINCT CASE
                        WHEN served = true
                        AND used = true THEN plugin_name
                    END
                ),
                ', '
            ) AS plugin_used
        FROM tbl_plugin_calls
        WHERE
            _is_current = 'Y'
            AND _is_deleted = 'N'
            AND interaction_id IS NOT NULL
            AND plugin_name IS NOT NULL
            AND trim(plugin_name) != ''
        GROUP BY
            interaction_id
    ),
    resource_aggregates AS (
        -- Aggregate knowledge citations, content items, and generated tickets per interaction
        SELECT
            interaction_id,
            array_join (
                array_agg (
                    DISTINCT CASE
                        WHEN detail_domain IS NOT NULL
                        AND trim(detail_domain) != '' THEN detail_domain
                    END
                ),
                ', '
            ) AS resource_domain,
            count(DISTINCT resource_id) AS no_of_citations,
            array_join (
                array_agg (
                    DISTINCT CASE
                        WHEN detail_name IS NOT NULL
                        AND trim(detail_name) != '' THEN detail_name
                    END
                ),
                ', '
            ) AS content_item_name,
            array_join (
                array_agg (
                    DISTINCT CASE
                        WHEN detail_external_resource_id IS NOT NULL
                        AND trim(detail_external_resource_id) != '' THEN detail_external_resource_id
                    END
                ),
                ', '
            ) AS content_item_id,
            max(
                CASE
                    WHEN type = 'RESOURCE_TYPE_TICKET' THEN 'user initiated ticket'
                    ELSE NULL
                END
            ) AS ticket_type,
            array_join (
                array_agg (
                    DISTINCT CASE
                        WHEN type = 'RESOURCE_TYPE_TICKET'
                        AND detail_external_resource_id IS NOT NULL
                        AND trim(detail_external_resource_id) != '' THEN detail_external_resource_id
                    END
                ),
                ', '
            ) AS ticket_id
        FROM tbl_plugin_resources
        WHERE
            _is_current = 'Y'
            AND _is_deleted = 'N'
            AND interaction_id IS NOT NULL
        GROUP BY
            interaction_id
    ),
    ranked_users AS (
        -- Rank users based on id and take the 1st record to ensure 1:1 join
        SELECT *
        FROM (
                SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY
                            id
                        ORDER BY COALESCE(_updated_at, _inserted_at) DESC
                    ) AS rnk
                FROM tbl_users
                WHERE
                    _is_current = 'Y'
                    AND _is_deleted = 'N'
                    AND id IS NOT NULL
                    AND trim(id) != ''
            ) AS u_sub
        WHERE
            u_sub.rnk = 1
    )
SELECT
    -- Base Interaction Attributes
    ui.created_time AS timestamp,
    ui.conversation_id AS conversation_id,
    ui.id AS interaction_id,
    COALESCE(ui.type, 'UNKNOWN') AS interaction_type,

-- Conversation Details
COALESCE(c.primary_domain, '') AS conversation_domain,
COALESCE(ct.conversation_topic, '') AS conversation_topic,

-- Content & Bot Response
COALESCE(ui.detail_content, '') AS interaction_content,
COALESCE(br.bot_response, '') AS bot_response,

-- Plugin Activity
COALESCE(pa.unsuccessful_plugins, '') AS unsuccessful_plugins,
COALESCE(pa.plugin_served, '') AS plugin_served,
COALESCE(pa.plugin_used, '') AS plugin_used,

-- Resource & Citation Details
COALESCE(ra.resource_domain, '') AS resource_domain,
COALESCE(ra.no_of_citations, 0) AS no_of_citations,
COALESCE(ra.content_item_name, '') AS content_item_name,
COALESCE(ra.content_item_id, '') AS content_item_id,
COALESCE(ra.ticket_type, '') AS ticket_type,
COALESCE(ra.ticket_id, '') AS ticket_id,

-- Surface Platform & User Identity
COALESCE(ui.platform, '') AS interaction_surface,
COALESCE(u.user_preferred_language, '') AS user_preferred_language,

-- Placeholder Dimensions for External HR/Identity Enrichment
CAST(NULL AS VARCHAR) AS user_department,
CAST(NULL AS VARCHAR) AS user_location,
CAST(NULL AS VARCHAR) AS user_country,

-- Audit Timestamp
CURRENT_TIMESTAMP AS _data_as_of
FROM tbl_interactions ui

-- Join conversation domain from conversations table (active records only)
LEFT JOIN tbl_conversations c ON ui.conversation_id = c.id
AND c._is_current = 'Y'
AND c._is_deleted = 'N'

-- Join aggregated conversation topics/entities
LEFT JOIN conversation_topics ct ON ui.conversation_id = ct.conversation_id

-- Join bot response interaction
LEFT JOIN bot_responses br ON ui.id = br.parent_interaction_id

-- Join aggregated plugin calls
LEFT JOIN plugin_aggregates pa ON ui.id = pa.interaction_id

-- Join aggregated plugin resources & citations
LEFT JOIN resource_aggregates ra ON ui.id = ra.interaction_id

-- Join user identity & preferred language from 1st-ranked user record
LEFT JOIN ranked_users u ON ui.user_id = u.id
WHERE
    -- Filter strictly for active, non-deleted user-led interactions
    ui._is_current = 'Y'
    AND ui._is_deleted = 'N'
    AND lower(ui.actor) = 'user';