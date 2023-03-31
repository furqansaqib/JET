-- jet.v_product_reviews source

CREATE OR REPLACE VIEW jet.v_product_reviews
AS SELECT dt.date,
    dt.day,
    dt.month,
    dt.year,
    dt.week,
    dt.quarter,
    dt.holiday,
    pr.asin,
    pr.description,
    pr.title,
    pr.image_url,
    pr.also_viewed,
    pr.also_bought,
    pr.bought_together,
    pr.buy_after_viewing,
    pr.viewed_and_bought,
    pr.is_viewed_and_bought,
    pc.product_category,
    rvr.reviewerid,
    rvr.reviewer_name,
    rv.review_summary,
    rv.review_text,
    pb.bucket_range,
    fact.fact_key,
    fact.price,
    fact.sales_rank,
    fact.helpful_votes,
    fact.non_helpful_votes,
    fact.total_votes,
    fact.overall_score
   FROM jet.fact_product_reviews fact
     JOIN jet.dim_date dt ON fact.review_date = dt.date
     LEFT JOIN jet.dim_product pr ON fact.asin = pr.asin
     JOIN jet.dim_product_category pc ON pr.product_category_key = pc.product_category_key
     LEFT JOIN jet.dim_review rv ON fact.review_key = rv.review_key
     LEFT JOIN jet.dim_reviewer rvr ON fact.reviewer_key = rvr.reviewer_key
     LEFT JOIN jet.dim_price_bucket pb ON fact.price_bucket_key = pb.price_bucket_key;