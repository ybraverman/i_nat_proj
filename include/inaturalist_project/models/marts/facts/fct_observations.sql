with obs as (
    select *
    from {{ ref('stg_inat__observations') }}
)

select
    o.observation_id,
    o.user_id,
    o.taxon_id,
    o.observed_at,
    o.latitude,
    o.longitude
from obs o
