CREATE OR REPLACE PROCEDURE jet.proc_dim_review_and_reviewer(IN p_startdate date, IN p_enddate date)
 LANGUAGE plpgsql
AS $procedure$
BEGIN

-- Records where we don't have review ID or ASIN marking those as rejected
UPDATE jet.reviews_stg rs 
SET record_status = 'rejected'
WHERE review_date>=p_startdate AND review_date<p_enddate
AND (reviewerID is NULL OR ASIN is NULL);

    
    
DROP TABLE IF EXISTS jet.dim_review_and_reviewer_stg;
CREATE TABLE jet.dim_review_and_reviewer_stg
(
	-- columns for review dimension
	review_key BPCHAR(32),
	review_summary TEXT NULL,
	review_text TEXT NULL,
	-- coloumns for reviewer dimension
	reviewer_name TEXT NULL,
	reviewerID TEXT NOT NULL,
	review_date date NOT NULL,
	overall float NULL,
	helpful text NOT NULL,
	helpful_votes INT NOT NULL, 
	non_helpful_votes INT NOT NULL,
	total_votes INT NOT NULL,
	ASIN bpchar(10) NOT NULL,
	row_number int4 NOT NULL,
	status text NOT NULL
);



INSERT INTO jet.dim_review_and_reviewer_stg
SELECT 
MD5(COALESCE(summary,'Unknown') || COALESCE(reviewtext,'Unknown')) as review_key,
coalesce(summary, 'Unknown') as review_summary, 
coalesce(reviewtext, 'Unknown') as review_text, 
-- Cleaning reviewername as it contains addit texts
coalesce(LOWER(trim(regexp_replace(replace(reviewername,'%amp;',''), '\".*', ''))), 'Unknown') as reviewer_name,
coalesce(reviewerID, 'Unknown') as reviewerID,
review_date as review_date,
overall::float, 
COALESCE(helpful,'Unknown') as helpful,
COALESCE((helpful::json->>0)::integer,0) as helpful_votes,
COALESCE(((helpful::json->>1)::integer - (helpful::json->>0)::integer),0) as non_helpful_votes,
COALESCE((helpful::json->>1)::integer,0) as total_votes,
COALESCE(ASIN, 'Unknown') as ASIN,
ROW_NUMBER()OVER(PARTITION BY reviewerID,ASIN),
'pending' as status
FROM jet.reviews_stg rs 
WHERE review_date>=p_startdate AND review_date<p_enddate
AND reviewerID is not NULL and ASIN is not NULL
AND rs.record_status = 'pending';

DROP TABLE IF EXISTS jet.dim_review_stg;
CREATE TABLE jet.dim_review_stg
(
	review_key BPCHAR(32) Primary Key,
	review_summary TEXT NULL,
	review_text TEXT NULL
);

INSERT INTO jet.dim_review_stg
SELECT distinct review_key, review_summary, review_text
FROM jet.dim_review_and_reviewer_stg;

-- Inserting records into Review Dimension
INSERT INTO jet.dim_review
SELECT stg.* FROM jet.dim_review_stg stg
LEFT JOIN jet.dim_review dim
ON stg.review_key = dim.review_key
WHERE dim.review_key IS NULL;


 -- Delete Duplicate reviews given to single product by a single reviewer
UPDATE  jet.dim_review_and_reviewer_stg
SET Status ='rejected'
WHERE row_number >1;


DROP TABLE IF EXISTS temp_reviewers;
CREATE TEMP TABLE temp_reviewers
(
	reviewerID TEXT NOT NULL,
	reviewer_name TEXT NOT NULL,
	review_date date NOT NULL
);
create index idx_temp_reviewers_name_id on temp_reviewers(reviewerID, reviewer_name);

INSERT INTO temp_reviewers
SELECT reviewerID, string_agg(distinct reviewer_name, '; '), max(review_date) as review_date 
FROM jet.dim_review_and_reviewer_stg
WHERE reviewer_name!='Unknown' 
AND reviewer_name!='none' 
AND reviewer_name not like '%customer%'
GROUP BY 1;


DROP TABLE IF EXISTS temp_active_reviewers;
CREATE TEMP TABLE temp_active_reviewers
(
	-- columns for review dimension
	reviewer_key int8 primary key,
	reviewerID TEXT NOT NULL,
	reviewer_name TEXT NOT NULL,
	startdate date NOT NULL
);
create index idx_temp_active_reviewer_name_id on temp_active_reviewers(reviewerid, reviewer_name);

-- Pick Active Reviewers
insert into temp_active_reviewers
SELECT reviewer_key, reviewerID, reviewer_name, startdate 
FROM jet.dim_reviewer
WHERE enddate ='9999-09-09';


DROP TABLE IF EXISTS temp_records_to_insert;
CREATE TEMP TABLE temp_records_to_insert AS 
SELECT r.reviewerID, r.reviewer_name, r.review_date as startdate
FROM temp_reviewers r
INNER JOIN temp_active_reviewers a
ON a.reviewerID = r.reviewerID and a.reviewer_name !=r.reviewer_name;

-- SELECT * FROM temp_reviewers where reviewerid='A4IK6MVD15DSY'
-- SELECT * FROM temp_active_reviewers where reviewerid='A4IK6MVD15DSY'

INSERT INTO temp_records_to_insert
SELECT r.reviewerID, r.reviewer_name, r.review_date as startdate
FROM temp_reviewers r
LEFT JOIN temp_active_reviewers a
ON a.reviewerID = r.reviewerID
WHERE a.reviewerID is NULL;

-- Marking end date for changed records
UPDATE jet.dim_reviewer a
SET enddate = b.startdate
FROM temp_records_to_insert b
WHERE a.reviewerID = b.reviewerID
AND a.enddate='9999-09-09';

INSERT INTO jet.dim_reviewer (reviewerID, reviewer_name, startdate, enddate)
SELECT 
reviewerID, 
reviewer_name, 
startdate, 
'9999-09-09'::date as enddate 
FROM temp_records_to_insert;



DROP TABLE IF EXISTS temp_reviewers;
DROP TABLE IF EXISTS temp_active_reviewers;
DROP TABLE IF EXISTS temp_records_to_insert;


END;
$procedure$
;
