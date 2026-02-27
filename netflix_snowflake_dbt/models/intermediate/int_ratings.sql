with latest_ratings as (
    select *
    from {{ ref('stg_ratings') }}
    qualify row_number() over (
        partition by user_id, movie_id
        order by rating_timestamp desc
    ) = 1
),

movie_avg as (
    select
        movie_id,
        avg(rating) as avg_movie_rating
    from latest_ratings
    group by movie_id
),

movie_stats as (
    select
        lr.movie_id,
        count(*) as users_rated,
        round(ma.avg_movie_rating, 4) as avg_movie_rating,
        min(lr.rating) as min_movie_rating,
        max(lr.rating) as max_movie_rating,
        count(case when lr.rating >= ma.avg_movie_rating then 1 end) as users_high_rating,
        count(case when lr.rating < ma.avg_movie_rating then 1 end) as users_low_rating
    from latest_ratings lr
    join movie_avg ma using (movie_id)
    group by movie_id, ma.avg_movie_rating
)

select * from movie_stats