{{
    config(
        materialized='table',
        cluster_by=['genres', 'popularity_tier'],
        tags=['analytics', 'recommendation_system']
    )
}}

-- Comprehensive movie analytics model integrating ratings, tags, and genome data
-- Optimized for recommendation systems and genre-based segmentation

with 

-- Base ratings data with aggregations
ratings_base as (
    select 
        movie_id,
        count(*) as rating_count,
        avg(rating) as avg_rating,
        stddev(rating) as rating_stddev,
        min(rating) as min_rating,
        max(rating) as max_rating
    from {{ ref('src_ratings') }}
    where movie_id is not null 
        and rating is not null
    group by movie_id
),

-- User-generated tags with frequency filtering
tags_filtered as (
    select 
        movie_id,
        tag,
        count(*) as tag_frequency
    from {{ ref('src_tags') }}
    where movie_id is not null 
        and tag is not null
        and trim(tag) != ''
    group by movie_id, tag
    having count(*) >= 3  -- Minimum frequency threshold for tag relevance
),

-- Genome scores with normalization
genome_scores_normalized as (
    select 
        movie_id,
        tag_id,
        relevance,
        -- Normalize relevance scores within each movie
        relevance / max(relevance) over (partition by movie_id) as normalized_relevance
    from {{ ref('src_genome_scores') }}
    where movie_id is not null 
        and tag_id is not null
        and relevance > 0
),

-- Genome tags lookup
genome_tags_clean as (
    select 
        tag_id,
        initcap(trim(tag)) as genome_tag_name
    from {{ ref('src_genome_tags') }}
    where tag_id is not null 
        and tag is not null
        and trim(tag) != ''
),

-- Movies base data
movies_base as (
    select 
        movie_id,
        initcap(trim(title)) as movie_title,
        genres,
        -- Extract primary genre for ranking
        split_part(genres, '|', 1) as primary_genre
    from {{ ref('src_movies') }}
    where movie_id is not null 
        and title is not null
        and genres is not null
),

-- Weighted genome tag importance per movie
genome_importance as (
    select 
        gs.movie_id,
        gt.genome_tag_name,
        gs.normalized_relevance as tag_importance_score,
        -- Rank tags by importance within each movie
        row_number() over (
            partition by gs.movie_id 
            order by gs.normalized_relevance desc
        ) as tag_rank
    from genome_scores_normalized gs
    inner join genome_tags_clean gt 
        on gs.tag_id = gt.tag_id
),

-- Top genome tags per movie (limit to top 5 for performance)
top_genome_tags as (
    select 
        movie_id,
        listagg(genome_tag_name, ', ') within group (order by tag_rank) as top_genome_tags,
        avg(tag_importance_score) as avg_genome_importance
    from genome_importance
    where tag_rank <= 5
    group by movie_id
),

-- User tags aggregated per movie
user_tags_agg as (
    select 
        movie_id,
        count(distinct tag) as unique_user_tags,
        sum(tag_frequency) as total_tag_mentions,
        listagg(tag, ', ') within group (order by tag_frequency desc) as popular_user_tags
    from tags_filtered
    group by movie_id
),

-- Core movie data with enrichments
enriched_movies as (
    select 
        m.movie_id,
        m.movie_title,
        m.genres,
        m.primary_genre,
        
        -- Rating metrics
        coalesce(r.rating_count, 0) as rating_count,
        coalesce(r.avg_rating, 0) as avg_rating,
        coalesce(r.rating_stddev, 0) as rating_stddev,
        
        -- Tag metrics
        coalesce(ut.unique_user_tags, 0) as unique_user_tags,
        coalesce(ut.total_tag_mentions, 0) as total_tag_mentions,
        coalesce(ut.popular_user_tags, 'No user tags') as popular_user_tags,
        
        -- Genome tag metrics
        coalesce(gt.top_genome_tags, 'No genome tags') as top_genome_tags,
        coalesce(gt.avg_genome_importance, 0) as avg_genome_importance
        
    from movies_base m
    left join ratings_base r 
        on m.movie_id = r.movie_id
    left join user_tags_agg ut 
        on m.movie_id = ut.movie_id
    left join top_genome_tags gt 
        on m.movie_id = gt.movie_id
),

-- Genre-based rankings and analytics
genre_rankings as (
    select 
        *,
        -- Rank movies by average rating within each primary genre
        row_number() over (
            partition by primary_genre 
            order by avg_rating desc, rating_count desc
        ) as genre_rating_rank,
        
        -- Rank movies by genome tag importance within each primary genre
        row_number() over (
            partition by primary_genre 
            order by avg_genome_importance desc, rating_count desc
        ) as genre_importance_rank,
        
        -- Combined ranking score (weighted average)
        (avg_rating * 0.6 + avg_genome_importance * 0.4) as combined_score
        
    from enriched_movies
    where rating_count > 0  -- Filter out movies with no ratings
),

-- Final model with popularity tiers and comprehensive metrics
final_model as (
    select 
        movie_id,
        movie_title,
        genres,
        primary_genre,
        
        -- Rating metrics
        rating_count,
        round(avg_rating, 2) as avg_rating,
        round(rating_stddev, 3) as rating_stddev,
        
        -- Tag metrics
        unique_user_tags,
        total_tag_mentions,
        popular_user_tags,
        top_genome_tags,
        round(avg_genome_importance, 4) as avg_genome_importance,
        
        -- Rankings
        genre_rating_rank,
        genre_importance_rank,
        round(combined_score, 3) as combined_score,
        
        -- Popularity tier classification
        case 
            when rating_count >= 1000 and avg_rating >= 4.0 then 'Blockbuster'
            when rating_count >= 500 and avg_rating >= 3.5 then 'Popular'
            when rating_count >= 100 and avg_rating >= 3.0 then 'Well-Known'
            when rating_count >= 50 then 'Moderate'
            else 'Niche'
        end as popularity_tier,
        
        -- Recommendation flags for downstream analytics
        case 
            when genre_rating_rank <= 10 and avg_genome_importance > 0.5 then true
            else false
        end as is_top_genre_pick,
        
        case 
            when combined_score >= 3.5 and rating_count >= 100 then true
            else false
        end as is_recommended,
        
        -- Metadata for analytics
        current_timestamp() as model_updated_at
        
    from genre_rankings
)

select * from final_model
order by combined_score desc, rating_count desc