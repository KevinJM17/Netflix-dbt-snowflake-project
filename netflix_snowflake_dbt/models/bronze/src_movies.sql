select 
    movieId as movie_id,
    title,
    genres
from {{ source('raw', 'raw_movies') }}