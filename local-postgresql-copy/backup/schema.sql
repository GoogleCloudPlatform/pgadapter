CREATE TABLE albums (
  id character varying NOT NULL,
  title character varying NOT NULL,
  marketing_budget numeric,
  release_date date,
  cover_picture bytea,
  singer_id character varying NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id)
);
CREATE TABLE tracks (
  id character varying NOT NULL,
  track_number bigint NOT NULL,
  title character varying NOT NULL,
  sample_rate double precision NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id, track_number)
) INTERLEAVE IN PARENT albums ON DELETE CASCADE;
CREATE TABLE all_array_types (
  id bigint NOT NULL,
  col_array_bytea bytea[],
  col_array_date date[],
  col_array_numeric numeric[],
  col_array_jsonb jsonb[],
  col_array_bigint bigint[],
  col_array_bool boolean[],
  col_array_float8 double precision[],
  col_array_int8 bigint[],
  col_array_timestamptz timestamp with time zone[],
  col_array_varchar character varying[],
  col_array_text character varying[],
  col_array_int bigint[],
  PRIMARY KEY(id)
);
CREATE TABLE all_types (
  col_bigint bigint NOT NULL,
  col_bool boolean,
  col_bytea bytea,
  col_float8 double precision,
  col_int bigint,
  col_numeric numeric,
  col_timestamptz timestamp with time zone,
  col_date date,
  col_varchar character varying(100),
  col_jsonb jsonb,
  PRIMARY KEY(col_bigint)
);
CREATE TABLE concerts (
  id character varying NOT NULL,
  venue_id character varying NOT NULL,
  singer_id character varying NOT NULL,
  name character varying NOT NULL,
  start_time timestamp with time zone NOT NULL,
  end_time timestamp with time zone NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id),
  CONSTRAINT chk_end_time_after_start_time CHECK((end_time > start_time))
);
CREATE TABLE latency_test (
  col_bigint bigint NOT NULL,
  col_bool boolean,
  col_bytea bytea,
  col_float8 double precision,
  col_int bigint,
  col_numeric numeric,
  col_timestamptz timestamp with time zone,
  col_date date,
  col_varchar character varying(100),
  col_jsonb jsonb,
  PRIMARY KEY(col_bigint)
);
CREATE TABLE numbers (
  num bigint NOT NULL,
  name character varying,
  extra_value character varying,
  PRIMARY KEY(num)
);
CREATE INDEX idx_numbers ON numbers (name) INCLUDE (extra_value) WHERE (name IS NOT NULL);
CREATE TABLE pgbench_accounts (
  aid bigint NOT NULL,
  bid bigint,
  abalance bigint,
  filler character varying(84),
  PRIMARY KEY(aid)
);
CREATE TABLE pgbench_branches (
  bid bigint NOT NULL,
  bbalance bigint,
  filler character varying(88),
  PRIMARY KEY(bid)
);
CREATE TABLE pgbench_history (
  tid bigint DEFAULT '-1'::bigint NOT NULL,
  bid bigint DEFAULT '-1'::bigint NOT NULL,
  aid bigint DEFAULT '-1'::bigint NOT NULL,
  delta bigint,
  mtime timestamp with time zone,
  filler character varying(22),
  PRIMARY KEY(tid, bid, aid)
);
CREATE TABLE pgbench_tellers (
  tid bigint NOT NULL,
  bid bigint,
  tbalance bigint,
  filler character varying(84),
  PRIMARY KEY(tid)
);
CREATE TABLE singers (
  id character varying NOT NULL,
  first_name character varying,
  last_name character varying NOT NULL,
  active boolean,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  full_name character varying GENERATED ALWAYS AS (CASE WHEN (first_name IS NULL) THEN last_name WHEN (last_name IS NULL) THEN first_name ELSE ((first_name || ' '::text) || last_name) END) STORED,
  PRIMARY KEY(id)
);
ALTER TABLE albums ADD CONSTRAINT fk_albums_singers FOREIGN KEY (singer_id) REFERENCES singers(id);
ALTER TABLE concerts ADD CONSTRAINT fk_concerts_singers FOREIGN KEY (singer_id) REFERENCES singers(id);
CREATE TABLE test (
  id bigint NOT NULL,
  value boolean[],
  j jsonb[],
  bi bigint[],
  ba bytea[],
  PRIMARY KEY(id)
);
CREATE TABLE test2 (
  id bigint NOT NULL,
  value character varying,
  PRIMARY KEY(id)
);
CREATE TABLE track_history (
  id bigint NOT NULL,
  foo character varying,
  album_id character varying(36),
  track_id bigint,
  PRIMARY KEY(id),
  CONSTRAINT fk_track_history_tracks FOREIGN KEY (album_id, track_id) REFERENCES tracks(id, track_number)
);
CREATE TABLE ts_test (
  id bigint NOT NULL,
  value character varying,
  created_at_milliseconds bigint,
  date timestamp with time zone GENERATED ALWAYS AS (to_timestamp(created_at_milliseconds)) STORED,
  PRIMARY KEY(id)
);
CREATE TABLE venues (
  id character varying NOT NULL,
  name character varying NOT NULL,
  description character varying NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id)
);
ALTER TABLE concerts ADD CONSTRAINT fk_concerts_venues FOREIGN KEY (venue_id) REFERENCES venues(id);
CREATE VIEW v_test SQL SECURITY INVOKER AS SELECT CASE count('1'::bigint) WHEN '1'::bigint THEN ((CURRENT_TIMESTAMP)::character varying)::boolean ELSE false END AS empty FROM (SELECT '1'::bigint FROM test LIMIT '1'::bigint) t;
CREATE VIEW v_test2 SQL SECURITY INVOKER AS SELECT id, value FROM test2;
