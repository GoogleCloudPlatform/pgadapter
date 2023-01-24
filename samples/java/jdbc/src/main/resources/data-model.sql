create table Singers (
                         id         String(255) not null,
                         first_name String(1000),
                         last_name  String(1000) not null,
                         full_name  String(1000),
                         active     bool,
                         created_at timestamp,
                         updated_at timestamp
)  primary key(id);

create table Albums (
                        id               String(255) not null,
                        title            String(1000) not null,
                        marketing_budget NUMERIC,
                        release_date     date,
                        cover_picture    BYTES(1000),
                        singer_id        String(255) not null,
                        created_at       timestamp,
                        updated_at       timestamp,
                        constraint fk_albums_singers foreign key (singer_id) references Singers (id)
)  primary key(id);

create table Tracks (
                        id           String(255) not null,
                        track_number INT64 not null,
                        title        String(1000) not null,
                        sample_rate  FLOAT64 not null,
                        created_at   timestamp,
                        updated_at   timestamp
) primary key (id, track_number);

create table Venues (
                        id          String(255) not null,
                        name        String(1000) not null,
                        description String(1000) not null,
                        created_at  timestamp,
                        updated_at  timestamp
)  primary key(id);

create table Concerts (
                          id          String(255) not null,
                          venue_id    String(255) not null,
                          singer_id   String(255) not null,
                          name        String(1000) not null,
                          start_time  timestamp not null,
                          end_time    timestamp not null,
                          created_at  timestamp,
                          updated_at  timestamp,
                          constraint fk_concerts_venues  foreign key (venue_id)  references Venues  (id),
                          constraint fk_concerts_singers foreign key (singer_id) references Singers (id),
                          constraint chk_end_time_after_start_time check (end_time > start_time)
)  primary key(id);