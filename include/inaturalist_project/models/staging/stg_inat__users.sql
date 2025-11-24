-- ==========================================
-- Model: users
-- Purpose: Stage the iNaturalist users table
--          for consistent joins with fact tables
-- Source: inaturalist_raw.users
-- ==========================================
select
    user_id,            -- Unique identifier for each user
    login as username   -- User's login name, renamed for clarity in downstream models
from {{ source('inaturalist_raw', 'users') }}

