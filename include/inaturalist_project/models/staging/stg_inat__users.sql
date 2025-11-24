select
    user_id,
    login as username
from {{ source('inaturalist_raw', 'users') }}
