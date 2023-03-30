-- jet.dim_review definition

-- Drop table

-- DROP TABLE jet.dim_review;

CREATE TABLE jet.dim_review (
	review_key bpchar(32) NOT NULL,
	review_summary text NOT NULL,
	review_text text NOT NULL,
	CONSTRAINT dim_review_pkey PRIMARY KEY (review_key)
);