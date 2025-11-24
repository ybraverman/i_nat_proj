select
    taxon_id,
    taxon_name,
    rank,
    iconic_taxon_name
from {{ ref('stg_inat__taxa') }}
