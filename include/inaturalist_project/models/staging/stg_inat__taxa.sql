-- ==========================================
-- Model: taxa_clean
-- Purpose: Clean and normalize the iNaturalist taxa table
--          to match types expected in fact tables
-- Source: inaturalist_raw.taxa
-- ==========================================

select
    taxon_id::int as taxon_id,       -- Cast taxon_id to integer for consistency with fact tables
    name as taxon_name,              -- Human-readable taxon name
    rank,                            -- Taxonomic rank (e.g., species, genus, family)
    iconic_taxon_name                -- Broad category (e.g., Aves, Insecta) for grouping
from {{ source('inaturalist_raw', 'taxa') }}

