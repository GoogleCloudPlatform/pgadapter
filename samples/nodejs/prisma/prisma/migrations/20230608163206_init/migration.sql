-- CreateTable
CREATE TABLE "Singer" (
    "id" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "firstName" TEXT NOT NULL,
    "lastName" TEXT NOT NULL,
    "fullName" TEXT generated always as (CASE WHEN "firstName" IS NULL THEN "lastName"
                                              WHEN "lastName"  IS NULL THEN "firstName"
                                              ELSE "firstName" || ' ' || "lastName" END) stored,
    "active" BOOLEAN NOT NULL,

    CONSTRAINT "Singer_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Album" (
    "id" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "title" TEXT NOT NULL,
    "marketingBudget" DECIMAL(65,30),
    "releaseDate" DATE,
    "coverPicture" BYTEA,
    "singerId" TEXT NOT NULL,

    CONSTRAINT "Album_pkey" PRIMARY KEY ("id")
);

-- CreateTable
-- Note that the definition of this table has been manually modified so it is interleaved in the
-- table Album. This is a Cloud Spanner-specific extension to open source PostgreSQL.
CREATE TABLE "Track" (
    "id" TEXT NOT NULL,
    "trackNumber" BIGINT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "title" TEXT NOT NULL,
    "sampleRate" DOUBLE PRECISION,

    CONSTRAINT "Track_pkey" PRIMARY KEY ("id","trackNumber")
) INTERLEAVE IN PARENT "Album" ON DELETE CASCADE;

-- CreateTable
CREATE TABLE "Venue" (
    "id" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "name" TEXT NOT NULL,
    "description" JSONB NOT NULL,

    CONSTRAINT "Venue_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Concert" (
    "id" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "name" TEXT NOT NULL,
    "venueId" TEXT NOT NULL,
    "singerId" TEXT NOT NULL,
    "startTime" TIMESTAMPTZ NOT NULL,
    "endTime" TIMESTAMPTZ NOT NULL,

    CONSTRAINT "Concert_pkey" PRIMARY KEY ("id"),
    constraint chk_end_time_after_start_time check ("endTime" > "startTime")
);

-- CreateIndex
CREATE INDEX "Singer_lastName_idx" ON "Singer"("lastName");

-- CreateIndex
CREATE UNIQUE INDEX "Album_id_title_idx" ON "Track"("id", "title");

-- CreateIndex
CREATE UNIQUE INDEX "Track_id_title_idx" ON "Track"("id", "title");

-- CreateIndex
CREATE INDEX "Concert_startTime_idx" ON "Concert"("startTime" DESC);

-- AddForeignKey
ALTER TABLE "Album" ADD CONSTRAINT "Album_singerId_fkey" FOREIGN KEY ("singerId") REFERENCES "Singer"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Concert" ADD CONSTRAINT "Concert_venueId_fkey" FOREIGN KEY ("venueId") REFERENCES "Venue"("id") ON DELETE RESTRICT ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Concert" ADD CONSTRAINT "Concert_singerId_fkey" FOREIGN KEY ("singerId") REFERENCES "Singer"("id") ON DELETE RESTRICT ON UPDATE CASCADE;
