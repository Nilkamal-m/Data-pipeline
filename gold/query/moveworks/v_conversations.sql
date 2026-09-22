-- ==============================================================================
-- Gold Mart Query: v_conversations.sql
-- Source System: moveworks
-- Hierarchy: s3://<bucket>/gold/query/moveworks/v_conversations.sql
-- Produces:
--   - Athena Mandatory View : v_conversations
--   - Downstream MySQL Mart : gold_tbl_conversations (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/moveworks/conversations/
--
-- Description:
--   Aggregates Moveworks conversations at one row per conversation.
--   Constructed directly from v_interactions:
--   - Grain: One row per conversation (all conversations, escalated and non-escalated alike)
--   - Conversation Start / End: MIN / MAX timestamp across all rows in the conversation
--   - Interaction Content: Concatenation of user prompts in timestamp order ("Prompt 1: xxx; Prompt 2: xxx; ...")
--   - Bot Response: Concatenation of bot responses in timestamp order ("Response 1: xxx; Response 2: xxx; ...")
--   - Escalated: 1 if ANY row within the Conversation ID has Plugin Used containing "Start Live Agent Chat", else 0
--   - HR vs. IT Agent: Classification logic applied when Escalated = 1, else blank
-- ==============================================================================

WITH
    prompts_aggregated AS (
        SELECT
            conversation_id,
            array_join(
                array_agg(concat('Prompt ', cast(prompt_seq as varchar(20)), ': ', trim(interaction_content))),
                '; '
            ) AS interaction_content
        FROM (
            SELECT
                conversation_id,
                interaction_content,
                ROW_NUMBER() OVER (
                    PARTITION BY conversation_id
                    ORDER BY timestamp ASC, interaction_id ASC
                ) AS prompt_seq
            FROM v_interactions
            WHERE conversation_id IS NOT NULL
              AND trim(conversation_id) != ''
              AND interaction_content IS NOT NULL
              AND trim(interaction_content) != ''
        )
        GROUP BY conversation_id
    ),
    responses_aggregated AS (
        SELECT
            conversation_id,
            array_join(
                array_agg(concat('Response ', cast(response_seq as varchar(20)), ': ', trim(bot_response))),
                '; '
            ) AS bot_response
        FROM (
            SELECT
                conversation_id,
                bot_response,
                ROW_NUMBER() OVER (
                    PARTITION BY conversation_id
                    ORDER BY timestamp ASC, interaction_id ASC
                ) AS response_seq
            FROM v_interactions
            WHERE conversation_id IS NOT NULL
              AND trim(conversation_id) != ''
              AND bot_response IS NOT NULL
              AND trim(bot_response) != ''
        )
        GROUP BY conversation_id
    ),
    conversation_bounds AS (
        SELECT
            conversation_id,
            min(timestamp) AS conversation_start,
            max(timestamp) AS conversation_end,
            max(
                CASE
                    WHEN lower(coalesce(plugin_used, '')) LIKE '%start live agent chat%' THEN 1
                    ELSE 0
                END
            ) AS escalated
        FROM v_interactions
        WHERE conversation_id IS NOT NULL AND trim(conversation_id) != ''
        GROUP BY conversation_id
    )
SELECT
    -- Output Fields
    cb.conversation_id AS conversation_id,
    cb.conversation_start AS conversation_start,
    cb.conversation_end AS conversation_end,
    COALESCE(pa.interaction_content, '') AS interaction_content,
    COALESCE(ra.bot_response, '') AS bot_response,
    cb.escalated AS escalated,
    CASE
        WHEN cb.escalated = 1 THEN
            CASE
                WHEN lower(coalesce(ra.bot_response, '')) LIKE '%hr%' THEN 'HR'
                WHEN lower(coalesce(ra.bot_response, '')) LIKE '%live%' THEN 'IT'
                WHEN lower(coalesce(pa.interaction_content, '')) LIKE '%it%' THEN 'IT'
                ELSE 'HR'
            END
        ELSE ''
    END AS hr_vs_it_agent,
    CURRENT_TIMESTAMP AS _data_as_of
FROM conversation_bounds cb
LEFT JOIN prompts_aggregated pa ON cb.conversation_id = pa.conversation_id
LEFT JOIN responses_aggregated ra ON cb.conversation_id = ra.conversation_id
