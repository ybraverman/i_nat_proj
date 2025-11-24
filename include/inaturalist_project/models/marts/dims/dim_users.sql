select
    user_id,
    username
from {{ ref('stg_inat__users') }}
