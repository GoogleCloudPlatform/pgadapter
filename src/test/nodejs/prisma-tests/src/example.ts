import { PrismaClient } from '@prisma/client'

process.env.DATABASE_URL = 'postgresql://localhost:5433/knut-test-db';
const prisma = new PrismaClient();

helloWorld(prisma)
  .then(async () => {
    await prisma.$disconnect();
  })
  .catch(async (e) => {
    console.error(e);
    await prisma.$disconnect();
    process.exit(1);
  });

async function helloWorld(prisma: PrismaClient) {
  const result = await prisma.$queryRaw`SELECT 'Hello world!' as hello`;
  console.log(result)
}
