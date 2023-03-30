-- jet.dim_product_category definition

-- Drop table

-- DROP TABLE jet.dim_product_category;

CREATE TABLE jet.dim_product_category (
	product_category_key bpchar(32) NOT NULL,
	product_category text NOT NULL,
	CONSTRAINT dim_product_category_pkey PRIMARY KEY (product_category_key)
);
CREATE INDEX dim_product_category_key_idx ON jet.dim_product_category USING btree (product_category);