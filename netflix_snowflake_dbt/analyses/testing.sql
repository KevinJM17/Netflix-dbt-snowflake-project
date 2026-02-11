{% set incremental = 1 %}
{% set incremental_col = 'movieId' %}

select 
    movieId as movie_id,
    tagId as tag_id,
    relevance
from {{ source('raw', 'raw_genome_scores') }}
{% if incremental == 1 %}
    where {{ incremental_col }} > (select max({{ incremental_col }}) from {{ this }})
{% endif %}