-- ==========================================
-- Model: dim_taxa
-- Purpose: Dimension table for taxa
--          Provides cleaned and standardized taxon information
-- Source: stg_inat__taxa (staging model)
-- ==========================================

select
    taxon_id,            -- Unique identifier for each taxon
    taxon_name,          -- Human-readable taxon name
    rank,                -- Taxonomic rank (e.g., species, genus, family)
    iconic_taxon_name    -- Broad category (e.g., Aves, Insecta) for grouping and analysis
from {{ ref('stg_inat__taxa') }}

