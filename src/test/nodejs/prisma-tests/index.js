const { PrismaClient } = require('@prisma/client')
const prisma = new PrismaClient();

(async () => {
  const result = await prisma.$queryRaw`SELECT 'Hello World!'`
  console.log(result)
})()
