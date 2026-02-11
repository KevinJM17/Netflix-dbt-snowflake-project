{{ config(materialized='incremental') }}

select 
    userId as user_id,
    movieId as movie_id,
    tag,
    to_timestamp(timestamp) as tag_timestamp
from {{ source('raw', 'raw_tags') }}

{% if is_incremental() %}
    where to_timestamp(timestamp) > (select coalesce(max(tag_timestamp), to_timestamp(0)) from {{ this }})
{% endif %}