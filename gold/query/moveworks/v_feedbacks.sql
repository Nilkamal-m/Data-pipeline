-- ==============================================================================
-- Gold Mart Query: v_feedbacks.sql
-- Source System: moveworks
-- Hierarchy: s3://<bucket>/gold/query/moveworks/v_feedbacks.sql
-- Produces:
--   - Athena Mandatory View : v_feedbacks
--   - Downstream MySQL Mart : gold_tbl_feedbacks (Zero-Downtime Physical Table)
-- S3 Storage: s3://<bucket>/gold/data/moveworks/feedbacks/
--
-- Description:
--   Aggregates Moveworks feedback events at one row per feedback event.
--   Constructed directly from v_interactions:
--   - Identification logic: interaction_type = 'link_click' AND interaction_content IN ('helpful', 'not helpful')
--   - Construction logic:
--       * Walk backward within the same Conversation ID to nearest preceding bot response row -> 'What Bot Said'
--       * Walk backward from that row to nearest preceding user prompt row -> 'What User Said'
--       * Take feedback row's interaction_content as 'Rating' (Helpful / Not Helpful)
--       * If a free-text feedback row immediately follows (same Conversation ID), pull as 'Feedback Text', else blank
-- ==============================================================================

WITH
    interactions_sequenced AS (
        SELECT
            conversation_id,
            timestamp,
            interaction_id,
            interaction_type,
            interaction_content,
            bot_response,
            CASE
                WHEN lower(coalesce(interaction_type, '')) = 'link_click'
                     AND lower(trim(coalesce(interaction_content, ''))) IN ('helpful', 'not helpful')
                THEN 1
                ELSE 0
            END AS is_feedback,
            ROW_NUMBER() OVER (
                PARTITION BY conversation_id
                ORDER BY timestamp ASC, interaction_id ASC
            ) AS seq,
            -- Look ahead to the immediately following row in the same conversation
            LEAD(lower(coalesce(interaction_type, ''))) OVER (
                PARTITION BY conversation_id
                ORDER BY timestamp ASC, interaction_id ASC
            ) AS next_type,
            LEAD(trim(coalesce(interaction_content, ''))) OVER (
                PARTITION BY conversation_id
                ORDER BY timestamp ASC, interaction_id ASC
            ) AS next_content,
            LEAD(trim(coalesce(bot_response, ''))) OVER (
                PARTITION BY conversation_id
                ORDER BY timestamp ASC, interaction_id ASC
            ) AS next_bot_response
        FROM v_interactions
    ),
    feedback_events AS (
        SELECT
            conversation_id,
            timestamp,
            interaction_id,
            interaction_content,
            seq,
            -- Free-text comment immediately follows if not a link_click and has no bot response
            CASE
                WHEN next_type IS NOT NULL
                     AND next_type != 'link_click'
                     AND (next_bot_response IS NULL OR next_bot_response = '')
                     AND next_content IS NOT NULL
                     AND next_content != ''
                     AND lower(next_content) NOT IN ('helpful', 'not helpful')
                THEN next_content
                ELSE ''
            END AS feedback_text
        FROM interactions_sequenced
        WHERE is_feedback = 1
    ),
    ranked_preceding_prompts AS (
        SELECT
            fb.conversation_id,
            fb.interaction_id,
            p.interaction_content AS what_user_said,
            p.bot_response AS what_bot_said,
            ROW_NUMBER() OVER (
                PARTITION BY fb.conversation_id, fb.interaction_id
                ORDER BY p.seq DESC
            ) AS rn
        FROM feedback_events fb
        JOIN interactions_sequenced p
            ON fb.conversation_id = p.conversation_id
            AND p.seq < fb.seq
            AND p.is_feedback = 0
            AND p.bot_response IS NOT NULL
            AND trim(p.bot_response) != ''
    )
SELECT
    -- Output Fields
    fb.conversation_id AS conversation_id,
    fb.timestamp AS timestamp,
    COALESCE(rp.what_user_said, '') AS what_user_said,
    COALESCE(rp.what_bot_said, '') AS what_bot_said,
    CASE
        WHEN lower(trim(fb.interaction_content)) = 'helpful' THEN 'Helpful'
        WHEN lower(trim(fb.interaction_content)) = 'not helpful' THEN 'Not Helpful'
        ELSE fb.interaction_content
    END AS rating,
    COALESCE(fb.feedback_text, '') AS feedback_text,
    CURRENT_TIMESTAMP AS _data_as_of
FROM feedback_events fb
LEFT JOIN ranked_preceding_prompts rp
    ON fb.conversation_id = rp.conversation_id
    AND fb.interaction_id = rp.interaction_id
    AND rp.rn = 1
