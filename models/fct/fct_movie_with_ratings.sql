{{
    config(
        materialized='table',
        cluster_by=['genres', 'popularity_tier'],
        partition_by={
            'field': 'created_at',
            'data_type': 'date'
        }
    )
}}

-- Comprehensive movie analytics model integrating ratings, tags, genome data, and movie metadata
-- Optimized for recommendation systems and genre-based segmentation

with 

-- Base staging models with null-safe filtering
ratings_base as (
    select 
        user_id,
        movie_id,
        rating,
        rating_timestamp
    from {{ ref('src_ratings') }}
    where movie_id is not null 
        and user_id is not null 
        and rating is not null
),

movies_base as (
    select 
        movie_id,
        title,
        genres
    from {{ ref('src_movies') }}
    where movie_id is not null 
        and title is not null
        and genres is not null
),

tags_base as (
    select 
        user_id,
        movie_id,
        tag,
        tag_timestamp
    from {{ ref('src_tags') }}
    where movie_id is not null 
        and user_id is not null 
        and tag is not null
),

genome_scores_base as (
    select 
        movie_id,
        tag_id,
        relevance
    from {{ ref('src_genome_scores') }}
    where movie_id is not null 
        and tag_id is not null 
        and relevance is not null
        and relevance > 0
),

genome_tags_base as (
    select 
        tag_id,
        tag as tag_name
    from {{ ref('src_genome_tags') }}
    where tag_id is not null 
        and tag is not null
),

-- Calculate tag frequency threshold (minimum 10 occurrences)
tag_frequency as (
    select 
        tag,
        count(*) as tag_count
    from tags_base
    group by tag
    having count(*) >= 10
),

-- Filter tags by frequency threshold
filtered_tags as (
    select 
        t.user_id,
        t.movie_id,
        t.tag,
        t.tag_timestamp
    from tags_base t
    inner join tag_frequency tf on t.tag = tf.tag
),

-- Create weighted genome tag importance scores with normalization
genome_weighted_scores as (
    select 
        gs.movie_id,
        gt.tag_name,
        gs.relevance,
        -- Normalize relevance scores using min-max scaling within each movie
        (gs.relevance - min(gs.relevance) over (partition by gs.movie_id)) / 
        nullif(max(gs.relevance) over (partition by gs.movie_id) - min(gs.relevance) over (partition by gs.movie_id), 0) as normalized_relevance,
        -- Calculate weighted importance score
        gs.relevance * 
        (row_number() over (partition by gs.movie_id order by gs.relevance desc) / 
         count(*) over (partition by gs.movie_id)) as weighted_importance_score
    from genome_scores_base gs
    inner join genome_tags_base gt on gs.tag_id = gt.tag_id
),

-- Aggregate movie ratings with comprehensive metrics
movie_rating_metrics as (
    select 
        movie_id,
        count(rating) as rating_count,
        avg(rating) as average_rating,
        stddev(rating) as rating_stddev,
        min(rating) as min_rating,
        max(rating) as max_rating,
        percentile_cont(0.5) within group (order by rating) as median_rating,
        count(distinct user_id) as unique_raters
    from ratings_base
    group by movie_id
),

-- Aggregate user-generated tags per movie
movie_tag_metrics as (
    select 
        movie_id,
        count(distinct tag) as unique_tag_count,
        count(tag) as total_tag_count,
        count(distinct user_id) as unique_taggers,
        listagg(distinct tag, ', ') within group (order by tag) as user_tags_list
    from filtered_tags
    group by movie_id
),

-- Aggregate genome tag importance per movie
movie_genome_metrics as (
    select 
        movie_id,
        count(distinct tag_name) as genome_tag_count,
        avg(relevance) as avg_genome_relevance,
        sum(weighted_importance_score) as total_weighted_importance,
        max(weighted_importance_score) as max_weighted_importance,
        listagg(tag_name, ', ') within group (order by weighted_importance_score desc) as top_genome_tags
    from genome_weighted_scores
    group by movie_id
),

-- Split genres for individual genre analysis
genre_split as (
    select 
        movie_id,
        title,
        genres,
        trim(value) as individual_genre
    from movies_base,
    lateral split_to_table(genres, '|')
    where trim(value) != ''
),

-- Main integration with null-safe joins
integrated_movie_data as (
    select 
        m.movie_id,
        m.title as movie_title,
        m.genres,
        gs.individual_genre,
        
        -- Rating metrics (null-safe)
        coalesce(rm.rating_count, 0) as rating_count,
        coalesce(rm.average_rating, 0) as average_rating,
        coalesce(rm.rating_stddev, 0) as rating_stddev,
        coalesce(rm.median_rating, 0) as median_rating,
        coalesce(rm.unique_raters, 0) as unique_raters,
        
        -- Tag metrics (null-safe)
        coalesce(tm.unique_tag_count, 0) as user_tag_count,
        coalesce(tm.total_tag_count, 0) as total_user_tags,
        coalesce(tm.unique_taggers, 0) as unique_taggers,
        coalesce(tm.user_tags_list, 'No tags') as user_tags_list,
        
        -- Genome metrics (null-safe)
        coalesce(gm.genome_tag_count, 0) as genome_tag_count,
        coalesce(gm.avg_genome_relevance, 0) as avg_genome_relevance,
        coalesce(gm.total_weighted_importance, 0) as total_weighted_importance,
        coalesce(gm.max_weighted_importance, 0) as max_weighted_importance,
        coalesce(gm.top_genome_tags, 'No genome tags') as top_genome_tags,
        
        current_timestamp() as created_at
        
    from movies_base m
    left join genre_split gs on m.movie_id = gs.movie_id
    left join movie_rating_metrics rm on m.movie_id = rm.movie_id
    left join movie_tag_metrics tm on m.movie_id = tm.movie_id
    left join movie_genome_metrics gm on m.movie_id = gm.movie_id
),

-- Apply window functions for genre-based ranking
genre_rankings as (
    select 
        *,
        -- Rank movies by average rating within each genre
        row_number() over (
            partition by individual_genre 
            order by average_rating desc, rating_count desc
        ) as genre_rating_rank,
        
        -- Rank movies by tag importance within each genre
        row_number() over (
            partition by individual_genre 
            order by total_weighted_importance desc, genome_tag_count desc
        ) as genre_importance_rank,
        
        -- Combined ranking score for recommendation systems
        row_number() over (
            partition by individual_genre 
            order by 
                (average_rating * 0.4) + 
                (total_weighted_importance * 0.3) + 
                (log(rating_count + 1) * 0.2) + 
                (user_tag_count * 0.1) desc
        ) as genre_combined_rank
        
    from integrated_movie_data
    where individual_genre is not null
),

-- Final model with popularity tiers and comprehensive analytics
final_movie_analytics as (
    select 
        movie_id,
        movie_title,
        genres,
        individual_genre,
        
        -- Rating metrics
        rating_count,
        average_rating,
        rating_stddev,
        median_rating,
        unique_raters,
        
        -- Tag metrics
        user_tag_count,
        total_user_tags,
        unique_taggers,
        user_tags_list,
        
        -- Genome metrics
        genome_tag_count,
        avg_genome_relevance,
        total_weighted_importance,
        max_weighted_importance,
        top_genome_tags,
        
        -- Rankings
        genre_rating_rank,
        genre_importance_rank,
        genre_combined_rank,
        
        -- Popularity tier classification based on rating count and average score
        case 
            when rating_count >= 1000 and average_rating >= 4.0 then 'Blockbuster'
            when rating_count >= 500 and average_rating >= 3.5 then 'Popular'
            when rating_count >= 100 and average_rating >= 3.0 then 'Well-Known'
            when rating_count >= 50 then 'Moderate'
            when rating_count >= 10 then 'Niche'
            else 'Limited'
        end as popularity_tier,
        
        -- Recommendation score for downstream analytics
        round(
            (average_rating * 0.35) + 
            (total_weighted_importance * 0.25) + 
            (log(rating_count + 1) * 0.20) + 
            (user_tag_count * 0.10) + 
            (genome_tag_count * 0.10), 
            4
        ) as recommendation_score,
        
        -- Metadata
        created_at
        
    from genre_rankings
)

select * from final_movie_analytics
order by recommendation_score desc, average_rating desc