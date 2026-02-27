with latest_ratings as (
    select *
    from {{ ref('stg_ratings') }}
    qualify row_number() over (partition by user_id, movie_id order by rating_timestamp) = 1
), movie_stats as (
    select
        movie_id,
        count(*) as users_rated,
        round(avg(rating), 4) as avg_movie_rating,
        min(rating) as min_movie_rating,
        max(rating) as max_movie_rating
    from latest_ratings
    group by movie_id
)

select * from movie_stats