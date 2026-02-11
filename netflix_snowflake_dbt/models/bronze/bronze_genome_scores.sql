{{ config(materialized='incremental') }}

select 
    movieId as movie_id,
    tagId as tag_id,
    relevance
from {{ source('raw', 'raw_genome_scores') }}

{% if is_incremental() %}
    where movieId > (select coalesce(max(movie_id), 0) from {{ this }})
{% endif %}