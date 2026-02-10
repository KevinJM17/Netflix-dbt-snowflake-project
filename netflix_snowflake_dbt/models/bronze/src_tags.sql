select 
    userId as user_id,
    movieId as movie_id,
    tag,
    to_timestamp(timestamp) as tag_timestamp
from {{ source('raw', 'raw_tags') }}