with base as (
    select
        observation_id,
        user_id,
        taxon_id::int as taxon_id,
        observed_on::timestamp as observed_at,
        latitude,
        longitude
    from {{ source('inaturalist_raw', 'observations') }}
)

select * from base




