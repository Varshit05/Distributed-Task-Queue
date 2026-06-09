import dotenv from 'dotenv';
import { connectDb } from './config/database.js';
import { Worker } from './core/Worker.js';
import { taskRegistry } from './workers/handlers.js';

dotenv.config();

async function bootstrap() {
  // 1. Connect to PostgreSQL
  await connectDb();

  // 2. Get queue name from command line arguments (e.g., npm run dev:worker -- high)
  const queueName = process.argv[2] || 'default';

  // 3. Initialize Worker
  const worker = new Worker(queueName);

  // 4. Register all handlers
  for (const [name, handler] of Object.entries(taskRegistry)) {
    worker.registerHandler(name, handler);
  }

  // 5. Start Worker
  await worker.start();

  // 6. Handle Graceful Shutdown
  const shutdown = async (signal: string) => {
    console.log(`\nReceived ${signal}. Shutting down worker...`);
    await worker.stop();
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

bootstrap().catch((error) => {
  console.error('Failed to start worker runner:', error);
  process.exit(1);
});
