-- ==========================================
-- Model: dim_users
-- Purpose: Dimension table for users
--          Provides cleaned and standardized user information
-- Source: stg_inat__users (staging model)
-- ==========================================

select
    user_id,    -- Unique identifier for each user
    username    -- User's login name or display name, standardized for analysis
from {{ ref('stg_inat__users') }}

