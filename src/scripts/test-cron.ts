import { spawn } from 'child_process';
import { db, connectDb } from '../config/database.js';
import { redis } from '../config/redis.js';
import { Scheduler } from '../core/Scheduler.js';
import { Worker } from '../core/Worker.js';
import { taskRegistry } from '../workers/handlers.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTest() {
  console.log('=== DISTRIBUTED CRON SCHEDULING INTEGRATION TEST ===\n');
  await connectDb();

  // 1. Reset Database and Redis
  console.log('[Setup] Resetting database and Redis states...');
  await db.taskLog.deleteMany();
  await db.task.deleteMany();
  await db.cronJob.deleteMany();
  await redis.del('delayed_tasks');
  await redis.del('task_stream:default');
  console.log('[Setup] Cleaned task queue and cron tables.\n');

  // 2. Start the API Server as a child process
  console.log('[API] Starting server.ts as a child process...');
  const serverProcess = spawn('npx', ['tsx', 'src/server.ts'], {
    stdio: 'pipe',
    shell: true,
  });

  // Log server output to console prefixed with [Server Output]
  serverProcess.stdout.on('data', (data) => {
    console.log(`[Server stdout] ${data.toString().trim()}`);
  });
  serverProcess.stderr.on('data', (data) => {
    console.error(`[Server stderr] ${data.toString().trim()}`);
  });

  // Wait 4 seconds for server to start up and connect to db
  await delay(4000);

  // 3. Initialize & Start Scheduler and Worker
  console.log('\n[Core] Starting Scheduler and Worker in-process...');
  const scheduler = new Scheduler();
  const worker = new Worker('default');

  // Register handlers on worker
  for (const [name, handler] of Object.entries(taskRegistry)) {
    worker.registerHandler(name, handler);
  }

  await scheduler.start();
  await worker.start();

  const baseUrl = 'http://localhost:3000/api';

  try {
    // 4. Test API: Register a Cron Job
    console.log('\n[Test] 1. Registering a cron job running every 10 seconds via API...');
    const registerResponse = await fetch(`${baseUrl}/cron`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        name: 'test-cron-job',
        taskName: 'send_email',
        expression: '*/10 * * * * *', // Every 10 seconds
        payload: {
          to: 'cron.test@example.com',
          subject: 'Cron Test Email',
        },
      }),
    });

    const registerResult = await registerResponse.json() as any;
    console.log('Register Response Status:', registerResponse.status);
    console.log('Register Response:', JSON.stringify(registerResult));

    if (!registerResult.success) {
      throw new Error(`Failed to register cron job: ${registerResult.error}`);
    }

    const cronJobId = registerResult.data.id;

    // 5. Test API: Get Cron Job list
    console.log('\n[Test] 2. Fetching all cron jobs...');
    const listResponse = await fetch(`${baseUrl}/cron`);
    const listResult = await listResponse.json() as any;
    console.log('List count:', listResult.data?.length);

    // 6. Test API: Get Cron Job by name
    console.log('\n[Test] 3. Fetching cron job by name...');
    const getResponse = await fetch(`${baseUrl}/cron/test-cron-job`);
    const getResult = await getResponse.json() as any;
    console.log('Get by name success:', getResult.success, 'Name:', getResult.data?.name);

    // 7. Wait and observe execution (2 cycles, ~22 seconds)
    console.log('\n[Test] 4. Waiting 22 seconds to observe cron triggering twice...');
    for (let i = 1; i <= 7; i++) {
      await delay(3000);
      const jobInDb = await db.cronJob.findUnique({ where: { id: cronJobId } });
      const completedTasks = await db.task.findMany({
        where: { name: 'send_email' },
        include: { logs: true },
      });
      console.log(`[Poll ${i}/7] Next scheduled run: ${jobInDb?.nextRunAt.toISOString()}. Executed tasks count: ${completedTasks.length}`);
      completedTasks.forEach((t) => {
        console.log(`  - Task ID ${t.id.substring(0, 8)} status: ${t.status}`);
      });
    }

    // 8. Test API: Toggle/Pause the Cron Job
    console.log('\n[Test] 5. Pausing the cron job...');
    const pauseResponse = await fetch(`${baseUrl}/cron/${cronJobId}/toggle`, {
      method: 'PATCH',
    });
    const pauseResult = await pauseResponse.json() as any;
    console.log('Pause status:', pauseResult.data?.isActive ? 'Active' : 'Paused');

    // Wait 12 seconds to verify it is NOT triggering anymore
    console.log('\n[Test] 6. Waiting 12 seconds to verify no new executions occur while paused...');
    const countBefore = await db.task.count({ where: { name: 'send_email' } });
    await delay(12000);
    const countAfter = await db.task.count({ where: { name: 'send_email' } });
    console.log(`Task count before: ${countBefore}, After: ${countAfter} (Expected: equal)`);
    if (countAfter > countBefore) {
      console.error('WARNING: Cron job was triggered while paused!');
    } else {
      console.log('Success: Cron job did not trigger while paused.');
    }

    // 9. Test API: Manually trigger the Cron Job
    console.log('\n[Test] 7. Manually triggering the paused cron job...');
    const triggerResponse = await fetch(`${baseUrl}/cron/${cronJobId}/trigger`, {
      method: 'POST',
    });
    const triggerResult = await triggerResponse.json() as any;
    console.log('Trigger Response:', JSON.stringify(triggerResult));

    // Wait 3 seconds to let the task process
    await delay(3000);
    const manualTasks = await db.task.findMany({
      where: { id: triggerResult.data.taskId },
      include: { logs: true },
    });
    console.log(`Manual task status: ${manualTasks[0]?.status}`);
    manualTasks[0]?.logs.forEach((log) => {
      console.log(`  Log: ${log.message}`);
    });

    // 10. Test API: Delete the Cron Job
    console.log('\n[Test] 8. Deleting the cron job...');
    const deleteResponse = await fetch(`${baseUrl}/cron/${cronJobId}`, {
      method: 'DELETE',
    });
    const deleteResult = await deleteResponse.json() as any;
    console.log('Delete response:', JSON.stringify(deleteResult));

    // Verify deleted
    const countJobs = await db.cronJob.count({ where: { id: cronJobId } });
    console.log(`Cron job count in DB: ${countJobs} (Expected: 0)`);

  } catch (error) {
    console.error('Error during integration tests:', error);
  } finally {
    // Graceful cleanup
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
