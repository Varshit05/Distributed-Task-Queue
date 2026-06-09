import dotenv from 'dotenv';
import { connectDb } from './config/database.js';
import { Scheduler } from './core/Scheduler.js';

dotenv.config();

async function bootstrap() {
  // 1. Connect to PostgreSQL
  await connectDb();

  // 2. Initialize Scheduler
  const scheduler = new Scheduler();

  // 3. Start Scheduler loops
  await scheduler.start();

  // 4. Handle Graceful Shutdown
  const shutdown = async (signal: string) => {
    console.log(`\nReceived ${signal}. Shutting down scheduler...`);
    await scheduler.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

bootstrap().catch((error) => {
  console.error('Failed to start scheduler runner:', error);
  process.exit(1);
});
