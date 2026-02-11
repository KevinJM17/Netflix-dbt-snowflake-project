{{ config(materialized='incremental') }}

select 
    movieId as movie_id,
    imdbId as imdb_id,
    tmdbId as tmdb_id
from {{ source('raw', 'raw_links') }}

{% if is_incremental() %}
    where movieId > (select coalesce(max(movie_id), 0) from {{ this }})
{% endif %}