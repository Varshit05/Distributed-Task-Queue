import { connectDb, db } from '../config/database.js';
import { redis } from '../config/redis.js';
import { Producer } from '../core/Producer.js';
import { Worker } from '../core/Worker.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTest() {
  console.log('=== RATE LIMITING TEST ===\n');
  await connectDb();

  // Reset database state and delete old tasks
  console.log('[Setup] Cleaning up tasks and rate limits...');
  await db.taskLog.deleteMany({});
  await db.task.deleteMany({});
  const limitKeys = await redis.keys('ratelimit:*');
  if (limitKeys.length > 0) {
    await redis.del(...limitKeys);
  }
  console.log('[Setup] Done.\n');

  // --- PART 1: Queue-level Rate Limiting (Max 3 executions per 2000ms) ---
  console.log('[Test 1] Testing Queue-level Rate Limiting (Limit: 3 tasks per 2000ms)...');

  // Submit 8 tasks
  console.log('Submitting 8 tasks to the "default" queue...');
  for (let i = 0; i < 8; i++) {
    await Producer.submitTask('send_email', { index: i }, { queue: 'default' });
  }

  // Create worker with queue limit
  const queueWorker = new Worker('default', {
    queueRateLimits: {
      default: { maxExecutions: 3, windowMs: 2000 }
    }
  });

  const queueTimestamps: number[] = [];
  queueWorker.registerHandler('send_email', async (payload) => {
    const now = Date.now();
    queueTimestamps.push(now);
    console.log(`[QueueWorker] Executed task ${payload.index} at ${new Date(now).toISOString()}`);
    return { success: true };
  });

  console.log('Starting QueueWorker...');
  await queueWorker.start();

  // Wait for all 8 tasks to complete
  console.log('Waiting for tasks to complete...');
  let completedCount = 0;
  const startTime = Date.now();
  while (completedCount < 8 && Date.now() - startTime < 15000) {
    completedCount = await db.task.count({ where: { status: 'completed' } });
    await delay(500);
  }

  console.log(`Stopping QueueWorker (Completed ${completedCount}/8 tasks)...`);
  await queueWorker.stop();

  if (completedCount < 8) {
    throw new Error(`Queue rate limit test timed out. Completed only ${completedCount} tasks.`);
  }

  // Validate Queue Rate Limit Timestamps
  console.log('\nValidating Queue Rate Limiting timestamps...');
  queueTimestamps.sort((a, b) => a - b);
  for (let i = 0; i < queueTimestamps.length; i++) {
    const t = queueTimestamps[i];
    // Count how many executions occurred within [t, t + 2000ms)
    const count = queueTimestamps.filter(ts => ts >= t && ts < t + 2000).length;
    console.log(`At ${new Date(t).toISOString()}, 2-second sliding window execution count: ${count}`);
    if (count > 3) {
      throw new Error(`Queue rate limit violated! Found ${count} executions within 2 seconds starting at ${new Date(t).toISOString()}`);
    }
  }
  console.log('Success: Queue-level rate limiting validated successfully.\n');

  // Clean up tasks for Part 2
  await db.taskLog.deleteMany({});
  await db.task.deleteMany({});
  if (limitKeys.length > 0) {
    await redis.del(...limitKeys);
  }

  // --- PART 2: Worker-level Rate Limiting (Max 2 executions per 1500ms) ---
  console.log('[Test 2] Testing Worker-level Rate Limiting (Limit: 2 tasks per 1500ms)...');

  // Submit 6 tasks
  console.log('Submitting 6 tasks...');
  for (let i = 0; i < 6; i++) {
    await Producer.submitTask('send_email', { index: i + 8 }, { queue: 'default' });
  }

  // Create worker with worker-level limits
  const workerRateLimit = new Worker('default', {
    workerMaxExecutions: 2,
    workerWindowMs: 1500
  });

  const workerTimestamps: number[] = [];
  workerRateLimit.registerHandler('send_email', async (payload) => {
    const now = Date.now();
    workerTimestamps.push(now);
    console.log(`[WorkerRateLimit] Executed task ${payload.index} at ${new Date(now).toISOString()}`);
    return { success: true };
  });

  console.log('Starting WorkerRateLimit...');
  await workerRateLimit.start();

  // Wait for all 6 tasks to complete
  console.log('Waiting for tasks to complete...');
  completedCount = 0;
  const workerStartTime = Date.now();
  while (completedCount < 6 && Date.now() - workerStartTime < 12000) {
    completedCount = await db.task.count({ where: { status: 'completed' } });
    await delay(500);
  }

  console.log(`Stopping WorkerRateLimit (Completed ${completedCount}/6 tasks)...`);
  await workerRateLimit.stop();

  if (completedCount < 6) {
    throw new Error(`Worker rate limit test timed out. Completed only ${completedCount} tasks.`);
  }

  // Validate Worker Rate Limit Timestamps
  console.log('\nValidating Worker Rate Limiting timestamps...');
  workerTimestamps.sort((a, b) => a - b);
  for (let i = 0; i < workerTimestamps.length; i++) {
    const t = workerTimestamps[i];
    // Count how many executions occurred within [t, t + 1500ms)
    const count = workerTimestamps.filter(ts => ts >= t && ts < t + 1500).length;
    console.log(`At ${new Date(t).toISOString()}, 1.5-second sliding window execution count: ${count}`);
    if (count > 2) {
      throw new Error(`Worker rate limit violated! Found ${count} executions within 1.5 seconds starting at ${new Date(t).toISOString()}`);
    }
  }
  console.log('Success: Worker-level rate limiting validated successfully.\n');

  console.log('=== ALL RATE LIMITING TESTS PASSED ===');
}

runTest()
  .catch((err) => {
    console.error('\nERROR: Test execution failed:', err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await db.$disconnect();
    await redis.quit();
    console.log('Disconnected.');
  });
