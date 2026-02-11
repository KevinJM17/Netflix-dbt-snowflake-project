{{ config(materialized='incremental') }}

select 
    movieId as movie_id,
    title,
    genres
from {{ source('raw', 'raw_movies') }}

{% if is_incremental() %}
    where movieId > (select coalesce(max(movie_id), 0) from {{ this }})
{% endif %}