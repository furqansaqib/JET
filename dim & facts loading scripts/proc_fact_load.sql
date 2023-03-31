CREATE OR REPLACE PROCEDURE jet.proc_fact_load(IN p_startdate date)
 LANGUAGE plpgsql
AS $procedure$

BEGIN
	
INSERT INTO jet.fact_product_reviews(review_date, price_bucket_key, reviewer_key, review_key, asin, 
price, sales_rank, helpful_votes, non_helpful_votes, total_votes, overall_score)
SELECT  
p_startdate as review_date,
dpb.price_bucket_key as price_bucket_key,
dreviewer.reviewer_key, 
CASE WHEN drar.review_key='915d220aba4527d1e33010bdfcbc6855' THEN NULL ELSE drar.review_key END as review_key,
dp.ASIN, 
dp_stg.price,
dp_stg.sales_rank,
drar.helpful_votes,
drar.non_helpful_votes,
drar.total_votes,
drar.overall as overall_score
FROM jet.dim_product dp
INNER JOIN jet.dim_product_stg dp_stg
ON dp.ASIN = dp_stg.ASIN
LEFT JOIN jet.dim_price_bucket dpb 
ON dp_stg.price >= dpb.min_price and  dp_stg.price<dpb.max_price 
LEFT JOIN jet.dim_review_and_reviewer_stg as drar
ON dp.ASIN = drar.ASIN and drar.review_date = p_startdate
LEFT JOIN jet.dim_reviewer dreviewer
ON drar.reviewerid =dreviewer.reviewerid 
and drar.review_date >=dreviewer.startdate and drar.review_date <dreviewer.enddate;



END;
$procedure$
;
