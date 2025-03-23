-- CreateTable
CREATE TABLE "TicketSale" (
    "id" SERIAL PRIMARY KEY,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,
    "concertId" TEXT NOT NULL,
    "customerName" TEXT NOT NULL,
    "price" NUMERIC,
    "seats" TEXT[] NOT NULL,
    CONSTRAINT "TicketSale_concertId_fkey" FOREIGN KEY ("concertId") REFERENCES "Concert"("id") ON DELETE RESTRICT ON UPDATE CASCADE
);
