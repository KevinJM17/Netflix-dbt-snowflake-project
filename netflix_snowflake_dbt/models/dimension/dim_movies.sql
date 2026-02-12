select 
    movie_id,
    title,
    split(genres, '|') as genres
from {{ ref('bronze_movies') }} 