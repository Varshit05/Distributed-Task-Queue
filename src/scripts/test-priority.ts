import { db, connectDb } from '../config/database.js';
import { redis } from '../config/redis.js';
import { Producer } from '../core/Producer.js';
import { Worker } from '../core/Worker.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTest() {
  console.log('=== PRIORITY QUEUE VERIFICATION TEST ===\n');
  await connectDb();

  // 1. Reset database and Redis
  console.log('[Setup] Cleaning database and Redis state...');
  await db.taskLog.deleteMany();
  await db.task.deleteMany();

  await redis.del('task_stream:low');
  await redis.del('task_stream:default');
  await redis.del('task_stream:high');

  try {
    await redis.xgroup('DESTROY', 'task_stream:low', 'worker_group');
    await redis.xgroup('DESTROY', 'task_stream:default', 'worker_group');
    await redis.xgroup('DESTROY', 'task_stream:high', 'worker_group');
  } catch (_) {}
  console.log('[Setup] Done.\n');

  // 2. Submit tasks in reverse priority order: low, then default, then high
  // Submit low-priority task first
  console.log('[Producer] Submitting Low priority task...');
  const taskLow = await Producer.submitTask('send_email', {
    to: 'low.priority@example.com',
    subject: 'Low Priority Task',
  }, { queue: 'low' });

  // Submit default-priority task
  console.log('[Producer] Submitting Default priority task...');
  const taskDefault = await Producer.submitTask('send_email', {
    to: 'default.priority@example.com',
    subject: 'Default Priority Task',
  }, { queue: 'default' });

  // Submit high-priority task last
  console.log('[Producer] Submitting High priority task...');
  const taskHigh = await Producer.submitTask('send_email', {
    to: 'high.priority@example.com',
    subject: 'High Priority Task',
  }, { queue: 'high' });

  console.log('\nTasks submitted and waiting in streams:');
  console.log(`- Low Priority Task: ID = ${taskLow.id}, Queue = ${taskLow.queue}`);
  console.log(`- Default Priority Task: ID = ${taskDefault.id}, Queue = ${taskDefault.queue}`);
  console.log(`- High Priority Task: ID = ${taskHigh.id}, Queue = ${taskHigh.queue}`);

  // 3. Initialize Worker with priority order: high,default,low
  console.log('\n[Worker] Initializing Worker with queues: "high,default,low"');
  const worker = new Worker('high,default,low');

  const executionOrder: string[] = [];

  // Register a mock handler to record the execution sequence
  worker.registerHandler('send_email', async (payload, log, context) => {
    const { to } = payload;
    console.log(`[Worker] Started processing task to ${to}`);
    executionOrder.push(to);
    // Delay slightly to simulate work
    await delay(1000);
    return { success: true };
  });

  console.log('[Worker] Starting worker to process the tasks...');
  await worker.start();

  // Wait for all 3 tasks to finish
  console.log('\n[Monitor] Waiting for tasks to complete...');
  let allCompleted = false;
  for (let attempt = 1; attempt <= 10; attempt++) {
    await delay(1000);
    const tasks = await db.task.findMany({
      where: { id: { in: [taskLow.id, taskDefault.id, taskHigh.id] } }
    });
    allCompleted = tasks.every(t => t.status === 'completed');
    if (allCompleted) {
      break;
    }
  }

  // 4. Verification and Shutdown
  console.log('\n[Cleanup] Stopping worker...');
  await worker.stop();

  console.log('\n=== RESULT VERIFICATION ===');
  console.log('Task processing start order:', executionOrder);

  const expectedOrder = [
    'high.priority@example.com',
    'default.priority@example.com',
    'low.priority@example.com'
  ];

  const matchesExpected = JSON.stringify(executionOrder) === JSON.stringify(expectedOrder);
  if (matchesExpected) {
    console.log('SUCCESS: Priority queue polling worked perfectly!');
    console.log('Tasks were processed in the exact priority order (high -> default -> low).');
  } else {
    console.error('ERROR: Priority queue polling failed.');
    console.error(`Expected: ${JSON.stringify(expectedOrder)}`);
    console.error(`Actual:   ${JSON.stringify(executionOrder)}`);
  }

  await db.$disconnect();
  await redis.quit();

  if (matchesExpected) {
    process.exit(0);
  } else {
    process.exit(1);
  }
}

runTest().catch((error) => {
  console.error('Fatal error in priority queue test:', error);
  process.exit(1);
});
