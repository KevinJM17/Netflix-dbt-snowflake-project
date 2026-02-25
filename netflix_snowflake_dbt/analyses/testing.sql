select
    movie_id,
    title,
    split(genres, '|') as genres
from {{ ref('stg_movies') }}