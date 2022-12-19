-- Executing the schema creation in a batch will improve execution speed.
start batch ddl;

CREATE TABLE IF NOT EXISTS singers (
     id character varying NOT NULL,
     first_name character varying,
     last_name character varying NOT NULL,
     full_name character varying NOT NULL,
     active boolean,
     created_at timestamp with time zone,
     updated_at timestamp with time zone,
     PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS albums (
    id character varying NOT NULL,
    title character varying NOT NULL,
    marketing_budget numeric,
    release_date date,
    cover_picture bytea,
    singer_id character varying NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    PRIMARY KEY(id),
    CONSTRAINT fk_albums_singers FOREIGN KEY (singer_id) REFERENCES singers(id)
);

CREATE TABLE IF NOT EXISTS tracks (
    track_id character varying NOT NULL,
    id character varying NOT NULL,
    track_number bigint NOT NULL,
    title character varying NOT NULL,
    sample_rate double precision NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    PRIMARY KEY(id, track_number)
) INTERLEAVE IN PARENT albums ON DELETE CASCADE;

CREATE UNIQUE INDEX unique_idx_id ON tracks(track_id);

CREATE TABLE IF NOT EXISTS venues (
    id character varying NOT NULL,
    name character varying NOT NULL,
    description character varying NOT NULL,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    PRIMARY KEY(id)
);

CREATE TABLE IF NOT EXISTS concerts (
  id character varying NOT NULL,
  venue_id character varying NOT NULL,
  singer_id character varying NOT NULL,
  name character varying NOT NULL,
  start_time timestamp with time zone NOT NULL,
  end_time timestamp with time zone NOT NULL,
  created_at timestamp with time zone,
  updated_at timestamp with time zone,
  PRIMARY KEY(id),
  CONSTRAINT chk_end_time_after_start_time CHECK((end_time > start_time)),
  CONSTRAINT fk_concerts_singers FOREIGN KEY (singer_id) REFERENCES singers(id),
  CONSTRAINT fk_concerts_venues FOREIGN KEY (venue_id) REFERENCES venues(id)
);

run batch;