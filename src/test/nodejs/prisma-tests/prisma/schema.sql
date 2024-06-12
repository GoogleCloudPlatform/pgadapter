start batch ddl;

-- model User {
--   id    String  @id
--   email String  @unique
--   name  String?
--   posts Post[]
-- }

create table "User" (
    id varchar(36) not null primary key,
    email varchar not null,
    name varchar
);

create unique index idx_user_email on "User" (email);

-- model Post {
--   id        String  @id
--   title     String
--   content   String?
--   published Boolean @default(false)
--   author    User?   @relation(fields:  [authorId], references: [id])
--   authorId  String?
-- }

create table "Post" (
    id varchar(36) not null primary key,
    title varchar not null,
    content text,
    published bool default false,
    "authorId" varchar(36),
    constraint fk_post_user foreign key ("authorId") references "User" (id)
);

run batch;
