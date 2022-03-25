module pgadapter_pgx_tests

go 1.17

require (
	github.com/jackc/pgtype v1.10.0
	github.com/jackc/pgx/v4 v4.15.0
)

require (
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgconn v1.11.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.2.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20200714003250-2b9c44734f2b // indirect
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/text v0.3.6 // indirect
)

replace github.com/jackc/pgx/v4 => github.com/olavloite/pgx/v4 v4.15.1
