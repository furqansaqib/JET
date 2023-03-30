-- jet.dim_reviewer definition

-- Drop table

-- DROP TABLE jet.dim_reviewer;

CREATE TABLE jet.dim_reviewer (
	reviewer_key bigserial NOT NULL,
	reviewerid text NOT NULL,
	reviewer_name text NOT NULL,
	startdate date NOT NULL,
	enddate date NOT NULL,
	CONSTRAINT dim_reviewer_pkey PRIMARY KEY (reviewer_key)
);
CREATE INDEX idx_dim_reviewer_enddate ON jet.dim_reviewer USING btree (enddate);
CREATE INDEX idx_dim_reviewer_id ON jet.dim_reviewer USING btree (reviewerid, startdate, enddate);
CREATE INDEX idx_dim_reviewer_key ON jet.dim_reviewer USING btree (reviewer_key, startdate, enddate);