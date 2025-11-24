-- ==========================================
-- Model: fct_observation_features
-- Purpose: Fact table with user-level rolling counts of key taxa
--          Includes classification flags and 30-day rolling sums for analysis
-- Source: stg_inat__observations (staging) and dim_taxa (dimension)
-- ==========================================

-- Step 1: Base observations with iconic taxon names
with obs as (
    select 
        o.observation_id,      -- Unique identifier for each observation
        o.user_id,             -- User who submitted the observation
        o.taxon_id,            -- Taxon ID observed
        o.observed_at,         -- Observation timestamp
        t.iconic_taxon_name    -- Broad taxonomic category (e.g., Aves, Insecta)
    from {{ ref('stg_inat__observations') }} o
    left join {{ ref('dim_taxa') }} t 
        on o.taxon_id = t.taxon_id
),

-- Step 2: Classify observations by major taxa
classified as (
    select
        *,
        case when iconic_taxon_name = 'Insecta' then 1 else 0 end as is_insecta,
        case when iconic_taxon_name = 'Aves' then 1 else 0 end as is_aves,
        case when iconic_taxon_name = 'Plantae' then 1 else 0 end as is_plants,
        case when iconic_taxon_name = 'Fungi' then 1 else 0 end as is_fungi,
        case when iconic_taxon_name = 'Mammalia' then 1 else 0 end as is_mammals,
        case when iconic_taxon_name = 'Reptilia' then 1 else 0 end as is_reptiles,
        case when iconic_taxon_name = 'Amphibia' then 1 else 0 end as is_amphibians
    from obs
),

-- Step 3: Compute 30-day rolling counts per user for Insecta and Aves
rolling as (
    select
        *,
        sum(is_insecta) over (
            partition by user_id 
            order by observed_at 
            rows between 30 preceding and current row
        ) as rolling_insecta_30,
        sum(is_aves) over (
            partition by user_id 
            order by observed_at 
            rows between 30 preceding and current row
        ) as rolling_aves_30
    from classified
)

-- Final output: all columns including classification flags and rolling counts
select * from rolling

