-- ==========================================
-- Model: observations_clean
-- Purpose: Cast and normalize key columns from raw iNaturalist observations
--          for downstream analysis
-- Source: inaturalist_raw.observations
-- ==========================================

-- Step 1: Base CTE to select and cast raw data
with base as (
    select
        observation_id,                        -- Unique identifier for the observation
        user_id,                               -- User who submitted the observation
        taxon_id::int as taxon_id,             -- Cast taxon_id to integer for consistency
        observed_on::timestamp as observed_at, -- Convert observed_on to timestamp for analysis
        latitude,                              -- Latitude of observation
        longitude                              -- Longitude of observation
    from {{ source('inaturalist_raw', 'observations') }}
)

-- Step 2: Output the base dataset
select * from base



