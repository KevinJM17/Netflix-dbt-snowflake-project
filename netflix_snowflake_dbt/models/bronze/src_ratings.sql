select 
    movieId as movie_id,
    userId as user_id,
    rating,
    timestamp as rating_timestamp
from {{ source('raw', 'raw_ratings') }}