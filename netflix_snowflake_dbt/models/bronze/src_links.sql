select 
    movieId as movie_id,
    imdbId as imdb_id,
    tmdbId as tmdb_id
from {{ source('raw', 'raw_links') }}