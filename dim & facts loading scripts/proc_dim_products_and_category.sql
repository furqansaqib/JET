CREATE OR REPLACE PROCEDURE jet.proc_dim_product_and_category()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

DROP TABLE IF EXISTS jet.dim_product_stg;
CREATE TABLE jet.dim_product_stg
(
	ASIN bpchar(10) Primary key,
	description text NOT NULL,
	title text NOT NULL,
	image_url text NOT NULL,
	also_viewed text NOT NULL,
	also_bought text NOT NULL,
	bought_together text NOT NULL,
	buy_after_viewing text NOT NULL,
	viewed_and_bought text NOT NULL,
	is_viewed_and_bought smallint NOT NULL,
	product_category_key bpchar(32) NOT NULL,
	price float NOT NULL,
	category text NOT NULL,
	sales_rank bigint NOT NULL,
	rn bigint NOT NULL,
	status text NOT NULL
);

INSERT INTO jet.dim_product_stg
SELECT 
ASIN, 
coalesce(Description, 'Unknown') as description, 
coalesce(title, 'Unknown') as title, 
coalesce(im_url, 'Unknown') as image_url,
coalesce(also_viewed, 'Unknown') as also_viewed,
coalesce(also_bought, 'Unknown') as also_bought,
coalesce(bought_together, 'Unknown') as bought_together,
coalesce(buy_after_viewing, 'Unknown') as buy_after_viewing,
coalesce(jet.find_matching_ids(also_viewed,also_bought), 'Unknown') as viewed_and_bought,
CASE WHEN jet.find_matching_ids(also_viewed,also_bought) ='[]' THEN 0 ELSE 1 END as is_viewed_and_bought,
MD5(COALESCE(jet.extract_text_in_quotes(categories), 'Unknown')) as product_category_key,
COALESCE(price::float, 0) as price,
COALESCE(jet.extract_text_in_quotes(categories), 'Unknown') as category,
case when sales_rank ~ '\d+' then COALESCE(regexp_replace(sales_rank, '[^0-9]', '', 'g')::int,0)
else 0 END AS sales_rank,
ROW_NUMBER()OVER(Partition by ASIN) as rn,
'Pending' as status
FROM jet.products_stg ps 
WHERE ps.record_status = 'pending';


-- Delete duplicated products
update jet.dim_product_stg 
set status ='Rejected' 
WHERE rn>1;

DROP TABLE IF EXISTS jet.dim_product_category_stg;
CREATE TABLE jet.dim_product_category_stg
(

	product_category_key bpchar(32) primary key,
	category text NOT NULL
);

INSERT INTO jet.dim_product_category_stg
SELECT distinct stg.product_category_key, stg.category
FROM jet.dim_product_stg stg;

-- Insert New Categories if Available
INSERT INTO jet.dim_product_category 
SELECT distinct stg.product_category_key, stg.category
FROM jet.dim_product_category_stg stg
LEFT JOIN jet.dim_product_category dim
ON stg.product_category_key = dim.product_category_key
WHERE dim.product_category_key IS NULL;

-- Insert new products if available
INSERT INTO jet.dim_product
SELECT stg.ASIN, 
stg.description, 
stg.title, 
stg.image_url,
stg.also_viewed,
stg.also_bought,
stg.bought_together,
stg.buy_after_viewing,
stg.viewed_and_bought,
stg.product_category_key
FROM dim_product_stg stg
LEFT JOIN jet.dim_product dim
ON stg.ASIN = dim.ASIN
WHERE dim.ASIN IS NULL;

END;
$procedure$
;
