with obs as (
    select 
        o.observation_id,
        o.user_id,
        o.taxon_id,
        o.observed_at,
        t.iconic_taxon_name
    from {{ ref('stg_inat__observations') }} o
    left join {{ ref('dim_taxa') }} t 
        on o.taxon_id = t.taxon_id
),

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

rolling as (
    select
        *,
        sum(is_insecta) over (partition by user_id order by observed_at rows between 30 preceding and current row) as rolling_insecta_30,
        sum(is_aves)    over (partition by user_id order by observed_at rows between 30 preceding and current row) as rolling_aves_30
    from classified
)

select * from rolling
