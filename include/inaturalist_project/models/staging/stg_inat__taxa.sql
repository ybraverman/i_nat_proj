select
    taxon_id::int as taxon_id,  -- Explicitly cast to match your fact table
    name as taxon_name,
    rank,
    iconic_taxon_name
from {{ source('inaturalist_raw', 'taxa') }}
