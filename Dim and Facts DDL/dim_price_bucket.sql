CREATE TABLE jet.dim_price_bucket (
  price_bucket_key serial PRIMARY KEY,
  min_price float8,
  max_price float8,
  bucket_range CHAR(10) NOT NULL
);

-- Insert data into the dim_price_bucket table
INSERT INTO jet.dim_price_bucket ( min_price, max_price, bucket_range)
VALUES
  (0, 100,   '0-100'),
  (100, 200, '100-200'),
  (200, 300, '200-300'), 
  (300, 400, '300-400'),
  (400, 500, '400-500'),
  (500, 600, '500-600'),
  (600, 700, '600-700'),
  (700, 800, '700-800'),
  (800, 900, '800-900'),
  (900, 1000, '900-1000'),
  (1000, 1100, '1000-1100'),
  (1100, 1200, '1100-1200'),
  (1200, 1300, '1200-1300'),
  (1300, 1400, '1300-1400'),
  (1400, 1500, '1400-1500'),
  (1500, 1600, '1500-1600'),
  (1600, 1700, '1600-1700'),
  (1700, 1800, '1700-1800'),
  (1800, 1900, '1800-1900'),
  (1900, 2000, '1900-2000'),
  (2000, 9999, '>2000');

-- Create an index on the price_bucket_key column for faster lookups
CREATE INDEX idx_min_max_price ON jet.dim_price_bucket (min_price, max_price);