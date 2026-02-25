with stg_links as (
    select 
        movieId as movie_id,
        imdbId as imdb_id,
        tmdbId as tmdb_id,
        current_timestamp() as _loaded_at
    from {{ source('raw', 'raw_links') }}
    where movie_id is not null 
)

select * from stg_links