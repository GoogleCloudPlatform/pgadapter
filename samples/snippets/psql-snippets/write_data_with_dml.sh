#!/bin/bash

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-example-db}"

psql -c "INSERT INTO singers (singer_id, first_name, last_name) VALUES
                             (12, 'Melissa', 'Garcia'),
                             (13, 'Russel', 'Morales'),
                             (14, 'Jacqueline', 'Long'),
                             (15, 'Dylan', 'Shaw')"
