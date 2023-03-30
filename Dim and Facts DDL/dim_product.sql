-- jet.dim_product definition

-- Drop table

-- DROP TABLE jet.dim_product;

CREATE TABLE jet.dim_product (
	asin bpchar(10) NOT NULL,
	description text NOT NULL,
	title text NOT NULL,
	image_url text NOT NULL,
	also_viewed text NOT NULL,
	also_bought text NOT NULL,
	bought_together text NOT NULL,
	buy_after_viewing text NOT NULL,
	viewed_and_bought text NOT NULL,
	product_category_key bpchar(32) NULL,
	CONSTRAINT dim_product_pkey PRIMARY KEY (asin)
);
CREATE INDEX ix_product_category_key ON jet.dim_product USING btree (product_category_key);


-- jet.dim_product foreign keys

ALTER TABLE jet.dim_product ADD CONSTRAINT fk_product_category_key FOREIGN KEY (product_category_key) REFERENCES jet.dim_product_category(product_category_key);