import { spawn } from 'child_process';
import { db, connectDb } from '../config/database.js';
import { redis } from '../config/redis.js';
import { Scheduler } from '../core/Scheduler.js';
import { Worker } from '../core/Worker.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTest() {
  console.log('=== DISTRIBUTED QUEUE RELIABILITY & MAINTENANCE TEST ===\n');
  await connectDb();

  // 1. Reset Database and Redis
  console.log('[Setup] Resetting database and Redis states...');
  await db.taskLog.deleteMany();
  await db.task.deleteMany();
  await db.cronJob.deleteMany();
  await redis.del('delayed_tasks');
  await redis.del('task_stream:default');
  await redis.del('task_stream:dlq');
  console.log('[Setup] Cleaned task queue, logs, and DLQ stream.\n');

  // 2. Start the API Server as a child process
  console.log('[API] Starting server.ts as a child process...');
  const serverProcess = spawn('npx', ['tsx', 'src/server.ts'], {
    stdio: 'pipe',
    shell: true,
  });

  serverProcess.stdout.on('data', (data) => {
    console.log(`[Server stdout] ${data.toString().trim()}`);
  });
  serverProcess.stderr.on('data', (data) => {
    console.error(`[Server stderr] ${data.toString().trim()}`);
  });

  await delay(4000);

  // 3. Start Scheduler and Worker
  console.log('\n[Core] Starting Scheduler and Worker in-process...');
  const scheduler = new Scheduler();
  const worker = new Worker('default');

  // Register a mock handler with dynamic duration support to test timeouts and retries
  let runCount = 0;
  worker.registerHandler('send_email', async (payload, log) => {
    runCount++;
    // First two runs will take 4 seconds (exceeding timeout), subsequent runs (like manual retry) will take 500ms
    const duration = runCount <= 2 ? 4000 : 500;
    await log(`Running mock send_email handler. Attempt #${runCount}. Simulating duration: ${duration}ms...`);
    await delay(duration);
    await log(`mock send_email completed successfully`);
    return { success: true };
  });

  await scheduler.start();
  await worker.start();

  const baseUrl = 'http://localhost:3000/api';

  try {
    // 4. Submit task with a short timeout and maxRetries = 1 (total 2 attempts)
    console.log('\n[Test] 1. Submitting task with 1.5s timeout (handler takes 4s first run)...');
    const submitResponse = await fetch(`${baseUrl}/tasks`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'send_email',
        payload: { to: 'cron.test@example.com', subject: 'Timeout Test' },
        options: {
          timeoutMs: 1500,
          maxRetries: 1, // Attempt 0 + 1 retry = 2 attempts total
        },
      }),
    });

    const submitResult = await submitResponse.json() as any;
    console.log('Submit Response status:', submitResponse.status);
    const taskId = submitResult.data.id;
    console.log('Task ID submitted:', taskId);

    // 5. Wait for the task to time out twice and fail permanently
    console.log('\n[Test] 2. Waiting for task execution and retries to exhaust...');
    
    // We expect:
    // Attempt 0: Starts, times out in 1.5s, schedules retry with exponential backoff (5s)
    // Attempt 1: Triggered 5s later, starts, times out in 1.5s, retries exhausted -> marks failed, routes to DLQ
    // Total wait: 1.5s (timeout) + 5s (backoff) + 1.5s (timeout) + 3s (safety margin) = 11 seconds
    for (let i = 1; i <= 6; i++) {
      await delay(2000);
      const taskInDb = await db.task.findUnique({ where: { id: taskId } });
      console.log(`[Poll ${i}/6] Task Status in DB: ${taskInDb?.status.toUpperCase()} (Retries: ${taskInDb?.retryCount}/${taskInDb?.maxRetries})`);
      if (taskInDb?.status === 'failed') {
        console.log(`Task permanently failed with error: "${taskInDb.errorMessage}"`);
        break;
      }
    }

    // 6. Verify that it was routed to the DLQ Redis Stream
    console.log('\n[Test] 3. Reading Dead Letter Queue (DLQ) Redis Stream...');
    const dlqMessages = await redis.xread('STREAMS', 'task_stream:dlq', '0-0') as any;
    if (dlqMessages && dlqMessages.length > 0) {
      const messages = dlqMessages[0][1];
      console.log(`Found ${messages.length} messages in DLQ:`);
      messages.forEach((m: any) => {
        const fields = m[1];
        const msgMap: Record<string, string> = {};
        for (let j = 0; j < fields.length; j += 2) {
          msgMap[fields[j]] = fields[j + 1];
        }
        console.log(`  - Msg ID: ${m[0]}, Task ID: ${msgMap.id}, Queue: ${msgMap.originalQueue}, Error: "${msgMap.error}"`);
      });
    } else {
      console.error('ERROR: No messages found in DLQ stream!');
    }

    // 7. Test API: Manual Retry
    console.log('\n[Test] 4. Triggering manual retry endpoint for the failed task...');
    const retryResponse = await fetch(`${baseUrl}/tasks/${taskId}/retry`, {
      method: 'POST',
    });
    const retryResult = await retryResponse.json() as any;
    console.log('Retry Response status:', retryResponse.status);
    console.log('Retry Response:', JSON.stringify(retryResult));

    // Wait 3 seconds for the retry execution (since runCount = 2, it will take 500ms and succeed)
    console.log('\n[Test] 5. Waiting for manual retry execution to finish...');
    await delay(3000);
    const finalTaskInDb = await db.task.findUnique({
      where: { id: taskId },
      include: { logs: true },
    });
    console.log('Final Task Status in DB:', finalTaskInDb?.status.toUpperCase());
    console.log('Task Log Audit Trail:');
    finalTaskInDb?.logs.forEach((log) => {
      console.log(`  [${log.level.toUpperCase()}] ${log.message}`);
    });

    if (finalTaskInDb?.status !== 'completed') {
      console.error('ERROR: Task did not complete after retry!');
    } else {
      console.log('Success: Task completed successfully after retry.');
    }

    // 8. Test Database History Pruning
    console.log('\n[Test] 6. Verifying database history pruning...');
    // Create an old mock task manually
    const tenDaysAgo = new Date(Date.now() - 10 * 24 * 60 * 60 * 1000);
    const oldTask = await db.task.create({
      data: {
        name: 'send_email',
        queue: 'default',
        status: 'completed',
        payload: { old: true },
        finishedAt: tenDaysAgo,
      },
    });
    console.log(`Created old task in DB: ID = ${oldTask.id}, FinishedAt = ${tenDaysAgo.toISOString()}`);

    const countBefore = await db.task.count();
    console.log(`Total tasks count before pruning: ${countBefore}`);

    // Call pruneOldTasks(7) programmatically
    const prunedCount = await scheduler.pruneOldTasks(7);
    console.log(`Pruned tasks count: ${prunedCount} (Expected: 1)`);

    const countAfter = await db.task.count();
    console.log(`Total tasks count after pruning: ${countAfter}`);

    const oldTaskCheck = await db.task.findUnique({ where: { id: oldTask.id } });
    if (!oldTaskCheck) {
      console.log('Success: Old task was successfully pruned from database.');
    } else {
      console.error('ERROR: Old task was not pruned!');
    }

  } catch (err) {
    console.error('Error during integration tests:', err);
  } finally {
    // Clean up
    console.log('\n[Cleanup] Stopping Scheduler and Worker...');
    await scheduler.stop();
    await worker.stop();

    console.log('[Cleanup] Stopping API Server...');
    serverProcess.kill('SIGTERM');

    await db.$disconnect();
    await redis.quit();
    console.log('\n=== TEST RUN FINISHED ===');
    process.exit(0);
  }
}

runTest().catch((err) => {
  console.error('Fatal test error:', err);
  process.exit(1);
});
