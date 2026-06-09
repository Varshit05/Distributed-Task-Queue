import { db, connectDb } from '../config/database.js';
import { redis } from '../config/redis.js';
import { Producer } from '../core/Producer.js';

// Helper to wait
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTest() {
  console.log('=== DISTRIBUTED TASK QUEUE INTEGRATION TEST ===');
  await connectDb();

  // 1. Reset Database and Redis for a clean test run
  console.log('\n[Setup] Cleaning database and Redis state...');
  try {
    await db.taskLog.deleteMany();
    await db.task.deleteMany();

    // Clean up Redis keys
    await redis.del('delayed_tasks');
    await redis.del('task_stream:default');
    await redis.del('task_stream:high');

    // Also remove consumer groups if they exist
    try {
      await redis.xgroup('DESTROY', 'task_stream:default', 'worker_group');
      await redis.xgroup('DESTROY', 'task_stream:high', 'worker_group');
    } catch (_) { }

    console.log('[Setup] Done. System is reset to clean slate.');
  } catch (err: any) {
    console.error('[Setup] Error resetting state. Is PostgreSQL running?', err.message);
    process.exit(1);
  }

  // 2. Submit Tasks
  console.log('\n[Producer] Submitting test tasks to queue...');

  // Task A: Immediate Successful Task (Email)
  const taskA = await Producer.submitTask('send_email', {
    to: 'john.doe@example.com',
    subject: 'Welcome to our platform!',
    body: 'We are excited to have you.'
  });

  // Task B: Immediate Successful Task (Image processing)
  const taskB = await Producer.submitTask('process_image', {
    imageId: 'img_x98765',
    format: 'webp',
    width: 800,
    height: 600
  });

  // Task C: Delayed Successful Task (Report Generation)
  // Scheduled to execute in 10 seconds
  const taskC = await Producer.submitTask('generate_report', {
    userId: 'usr_101',
    reportType: 'annual_financial'
  }, { delayMs: 10000 });

  // Task D: Immediate Retryable Task (Report Generation - Fails twice, succeeds on 3rd attempt)
  // Max retries = 3 (Attempt 0, Attempt 1 (Retry 1), Attempt 2 (Retry 2) - succeeds)
  const taskD = await Producer.submitTask('generate_report', {
    userId: 'usr_202',
    reportType: 'monthly_invoice',
    failAttempts: 2 // Fail first 2 attempts
  }, { maxRetries: 3 });

  // Task E: Immediate Terminally Failing Task (Email - Fails with no retries left)
  const taskE = await Producer.submitTask('send_email', {
    to: 'invalid-email-server',
    subject: 'Alert',
    body: 'Urgent notification',
    shouldFail: true
  }, { maxRetries: 1 });

  console.log('\nTasks Submitted:');
  console.log(`- Task A (Immediate Email): ID = ${taskA.id}`);
  console.log(`- Task B (Immediate Image): ID = ${taskB.id}`);
  console.log(`- Task C (Delayed Report - 10s delay): ID = ${taskC.id}`);
  console.log(`- Task D (Retry & Succeed Report): ID = ${taskD.id}`);
  console.log(`- Task E (Permanent Failure Email): ID = ${taskE.id}`);

  console.log('\n[Monitor] Starting live monitoring. Please run the worker and scheduler processes now!');
  console.log('Polling database every 3 seconds for status changes...\n');

  // 3. Monitor Loop
  const taskIds = [taskA.id, taskB.id, taskC.id, taskD.id, taskE.id];
  const maxPolls = 15; // Poll for 45 seconds total

  for (let poll = 1; poll <= maxPolls; poll++) {
    const tasks = await db.task.findMany({
      where: { id: { in: taskIds } },
    });

    console.log(`--- Poll #${poll}/${maxPolls} (${new Date().toLocaleTimeString()}) ---`);
    console.table(
      tasks.map((t) => ({
        ID: `${t.id.substring(0, 8)}...`,
        Name: t.name,
        Status: t.status.toUpperCase(),
        'Retry Ct': `${t.retryCount}/${t.maxRetries}`,
        'Scheduled RunAt': t.runAt.toLocaleTimeString(),
        Error: t.errorMessage ? t.errorMessage.substring(0, 45) + '...' : 'None'
      }))
    );

    // Check if all tasks have reached a terminal state (completed or failed)
    const allFinished = tasks.every(t => t.status === 'completed' || t.status === 'failed');
    if (allFinished) {
      console.log('\n[Monitor] All tasks reached final state. Verification completed.');
      break;
    }

    await delay(3000);
  }

  // 4. Print logs of the retry and failure tasks to inspect details
  console.log('\n=== LOG AUDIT TRAIL FOR TASK D (Retry task) ===');
  const logsD = await db.taskLog.findMany({ where: { taskId: taskD.id }, orderBy: { createdAt: 'asc' } });
  logsD.forEach(l => console.log(`[${l.createdAt.toLocaleTimeString()}] [${l.level.toUpperCase()}] ${l.message}`));

  console.log('\n=== LOG AUDIT TRAIL FOR TASK E (Terminal fail task) ===');
  const logsE = await db.taskLog.findMany({ where: { taskId: taskE.id }, orderBy: { createdAt: 'asc' } });
  logsE.forEach(l => console.log(`[${l.createdAt.toLocaleTimeString()}] [${l.level.toUpperCase()}] ${l.message}`));

  // Clean up connections
  await db.$disconnect();
  await redis.quit();
  process.exit(0);
}

runTest().catch((error) => {
  console.error('Error running integration test:', error);
  process.exit(1);
});
