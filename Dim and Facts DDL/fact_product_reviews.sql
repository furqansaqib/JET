DROP TABLE jet.fact_product_reviews;
CREATE TABLE jet.fact_product_reviews (
	fact_key BIGINT GENERATED BY DEFAULT AS IDENTITY,
	review_date date NOT NULL,
	price_bucket_key int4 NULL,
	reviewer_key int8 NULL,
	review_key bpchar(32) NULL,
	asin bpchar(10) NULL,
	price float8 NULL,
	sales_rank int4 NULL,
	helpful_votes int4 NULL,
	non_helpful_votes int4 NULL,
	total_votes int4 NULL,
	overall_score int4 NULL,
	CONSTRAINT fact_reviews_ukey UNIQUE (review_date, asin, reviewer_key, price_bucket_key, review_key),
	CONSTRAINT fact_reviews_pkey Primary Key (review_date, fact_key)
	)
PARTITION BY RANGE (review_date);
CREATE INDEX idx_fact ON ONLY jet.fact_product_reviews USING btree (review_date,asin, reviewer_key, price_bucket_key,reviewer_key);


-- jet.fact_product_reviews foreign keys

ALTER TABLE jet.fact_product_reviews ADD CONSTRAINT fk_asin FOREIGN KEY (asin) REFERENCES jet.dim_product(asin);
ALTER TABLE jet.fact_product_reviews ADD CONSTRAINT fk_price_bucket_key FOREIGN KEY (price_bucket_key) REFERENCES jet.dim_price_bucket(price_bucket_key);
ALTER TABLE jet.fact_product_reviews ADD CONSTRAINT fk_review_date FOREIGN KEY (review_date) REFERENCES jet.dim_date("date");
ALTER TABLE jet.fact_product_reviews ADD CONSTRAINT fk_review_key FOREIGN KEY (review_key) REFERENCES jet.dim_review(review_key);
ALTER TABLE jet.fact_product_reviews ADD CONSTRAINT fk_reviewer_key FOREIGN KEY (reviewer_key) REFERENCES jet.dim_reviewer(reviewer_key);

SELECT jet.generate_monthly_partitions('1996-01-01','2015-01-01','jet.fact_product_reviews');