{{ config(materialized='incremental') }}

select 
    movieId as movie_id,
    userId as user_id,
    rating,
    to_timestamp(timestamp) as rating_timestamp
from {{ source('raw', 'raw_ratings') }}

{% if is_incremental() %}
    where to_timestamp(timestamp) > (select coalesce(max(rating_timestamp), to_timestamp(0)) from {{ this }})
{% endif %}