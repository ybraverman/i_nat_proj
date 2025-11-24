-- ==========================================
-- Model: fct_observations
-- Purpose: Fact table for iNaturalist observations
--          Selects core observation data from staging for analytics
-- Source: stg_inat__observations (staging model)
-- ==========================================

-- Step 1: Base CTE selecting all columns from staging
with obs as (
    select *
    from {{ ref('stg_inat__observations') }}
)

-- Step 2: Select relevant columns for the fact table
select
    o.observation_id,   -- Unique identifier for the observation
    o.user_id,          -- ID of the user who submitted the observation
    o.taxon_id,         -- ID of the observed taxon
    o.observed_at,      -- Timestamp of the observation
    o.latitude,         -- Latitude of observation
    o.longitude         -- Longitude of observation
from obs o

