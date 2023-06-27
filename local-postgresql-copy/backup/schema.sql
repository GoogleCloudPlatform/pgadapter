CREATE TABLE albums (
  id character varying(36) NOT NULL,
  title character varying(200),
  marketing_budget numeric,
  release_date date,
  cover_picture bytea,
  singer_id character varying(36) NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id)
);
CREATE TABLE tracks (
  id character varying(36) NOT NULL,
  track_number bigint NOT NULL,
  title character varying NOT NULL,
  sample_rate double precision NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id, track_number)
) INTERLEAVE IN PARENT albums ON DELETE CASCADE;
CREATE TABLE concerts (
  shard_id bigint GENERATED ALWAYS AS (mod(id, '2048'::bigint)) STORED NOT NULL,
  id bigint NOT NULL,
  venue_shard_id bigint GENERATED ALWAYS AS (mod(venue_id, '2048'::bigint)) STORED NOT NULL,
  venue_id bigint NOT NULL,
  singer_id character varying NOT NULL,
  name character varying NOT NULL,
  start_time timestamp with time zone NOT NULL,
  end_time timestamp with time zone NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(shard_id, id),
  CONSTRAINT chk_end_time_after_start_time CHECK((end_time > start_time))
);
CREATE TABLE databasechangelog (
  id character varying(255) NOT NULL,
  author character varying(255) NOT NULL,
  filename character varying(255) NOT NULL,
  dateexecuted timestamp with time zone NOT NULL,
  orderexecuted bigint NOT NULL,
  exectype character varying(10) NOT NULL,
  md5sum character varying(35),
  description character varying(255),
  comments character varying(255),
  tag character varying(255),
  liquibase character varying(20),
  contexts character varying(255),
  labels character varying(255),
  deployment_id character varying(10),
  PRIMARY KEY(id)
);
CREATE TABLE databasechangeloglock (
  id bigint NOT NULL,
  locked boolean NOT NULL,
  lockgranted timestamp with time zone,
  lockedby character varying(255),
  PRIMARY KEY(id)
);
CREATE TABLE person (
  id bigint NOT NULL,
  age bigint NOT NULL,
  covid_info character varying(255),
  date character varying(255),
  name character varying(255),
  surname character varying(255),
  PRIMARY KEY(id)
);
CREATE TABLE seq_ids (
  seq_id character varying NOT NULL,
  seq_value bigint NOT NULL,
  PRIMARY KEY(seq_id)
);
CREATE TABLE singers (
  id character varying(36) NOT NULL,
  first_name character varying(100),
  last_name character varying(200),
  full_name character varying(300) GENERATED ALWAYS AS (CASE WHEN (first_name IS NULL) THEN last_name WHEN (last_name IS NULL) THEN first_name ELSE ((first_name || ' '::text) || last_name) END) STORED,
  active boolean,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id)
);
ALTER TABLE albums ADD CONSTRAINT fk_albums_singers FOREIGN KEY (singer_id) REFERENCES singers(id);
ALTER TABLE concerts ADD CONSTRAINT fk_concerts_singers FOREIGN KEY (singer_id) REFERENCES singers(id);
CREATE TABLE vendor_color_products (
  vendor_color_products_uuid character varying(36),
  color_id character varying NOT NULL,
  color_name character varying,
  brand_id bigint,
  brand_name character varying,
  vendor_id bigint NOT NULL,
  vendor_name character varying,
  product_id character varying,
  usage_id character varying,
  sheen_id bigint,
  sheen_category_id character varying,
  short_sheen_description character varying,
  sheen_description character varying,
  base_id character varying,
  base_description character varying,
  upc_code character varying,
  manual_fill_container_capacity double precision,
  container_capacity double precision,
  container_base_volume double precision,
  container_size_code bigint,
  db_id character varying,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(vendor_id, color_id)
);
CREATE TABLE venues (
  shard_id bigint GENERATED ALWAYS AS (mod(id, '2048'::bigint)) STORED NOT NULL,
  id bigint NOT NULL,
  name character varying NOT NULL,
  description jsonb NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(shard_id, id)
);
ALTER TABLE concerts ADD CONSTRAINT fk_concerts_venues FOREIGN KEY (venue_shard_id, venue_id) REFERENCES venues(shard_id, id);
