import { spawn } from 'child_process';
import { db, connectDb } from '../config/database.js';
import { redis } from '../config/redis.js';
import { Worker } from '../core/Worker.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTest() {
  console.log('=== WORKER HEARTBEAT & REGISTRY TEST ===\n');
  await connectDb();

  // 1. Reset Redis state
  console.log('[Setup] Cleaning worker registry keys from Redis...');
  await redis.del('workers:active');
  const workerKeys = await redis.keys('worker:info:*');
  if (workerKeys.length > 0) {
    await redis.del(...workerKeys);
  }
  console.log('[Setup] Done.\n');

  // 2. Start API server as a child process
  console.log('[API] Starting server.ts as a child process...');
  const serverProcess = spawn('npx', ['tsx', 'src/server.ts'], {
    stdio: 'pipe',
    shell: true,
  });

  serverProcess.stdout.on('data', (data) => {
    // Keep it clean unless we need to debug
  });
  serverProcess.stderr.on('data', (data) => {
    console.error(`[Server stderr] ${data.toString().trim()}`);
  });

  await delay(4000); // Wait for API server to boot up

  const baseUrl = 'http://localhost:3000/api';

  try {
    // 3. Start a worker
    console.log('[Test] 1. Initializing and starting worker with queues "high,default"...');
    const worker = new Worker('high,default');
    await worker.start();
    const workerId = (worker as any).workerId;
    console.log(`Worker started with ID: ${workerId}`);

    // 4. Verify initial registration
    console.log('[Test] 2. Fetching active workers from API...');
    const response1 = await fetch(`${baseUrl}/workers`);
    const result1 = await response1.json() as any;
    console.log('Active workers list:', JSON.stringify(result1.data));

    if (result1.data.length !== 1 || result1.data[0].id !== workerId) {
      throw new Error(`Worker registration failed. Expected 1 worker with ID ${workerId}`);
    }
    console.log('Success: Worker registered successfully with correct ID and queues.');

    const initialHeartbeat = result1.data[0].lastHeartbeat;

    // 5. Wait to test periodic heartbeat (5s interval)
    console.log('[Test] 3. Waiting 6 seconds for periodic heartbeat update...');
    await delay(6000);

    const response2 = await fetch(`${baseUrl}/workers`);
    const result2 = await response2.json() as any;
    const updatedHeartbeat = result2.data[0].lastHeartbeat;

    console.log(`Initial heartbeat: ${initialHeartbeat}`);
    console.log(`Updated heartbeat: ${updatedHeartbeat}`);

    if (new Date(updatedHeartbeat).getTime() <= new Date(initialHeartbeat).getTime()) {
      throw new Error('Periodic heartbeat failed. Timestamp did not update.');
    }
    console.log('Success: Heartbeat timestamp updated successfully.');

    // 6. Graceful shutdown registration cleanup
    console.log('[Test] 4. Stopping worker gracefully...');
    await worker.stop();

    await delay(1000);

    const response3 = await fetch(`${baseUrl}/workers`);
    const result3 = await response3.json() as any;
    console.log('Active workers list after shutdown:', JSON.stringify(result3.data));

    if (result3.data.length !== 0) {
      throw new Error('Graceful cleanup failed. Worker registry still contains entries.');
    }
    console.log('Success: Graceful unregistration completed successfully.');

    // 7. Simulate worker crash auto-pruning (TTL expiration)
    console.log('\n[Test] 5. Simulating worker crash/timeout...');
    const workerCrash = new Worker('default');
    await workerCrash.start();
    const crashWorkerId = (workerCrash as any).workerId;
    console.log(`New worker started with ID: ${crashWorkerId}`);

    // Verify it is registered
    const response4 = await fetch(`${baseUrl}/workers`);
    const result4 = await response4.json() as any;
    if (result4.data.length !== 1 || result4.data[0].id !== crashWorkerId) {
      throw new Error('Crash-test worker failed to register.');
    }

    // Stop heartbeats manually (simulating crash) without calling worker.stop()
    console.log('Stopping worker heartbeats manually (simulating crash)...');
    clearInterval((workerCrash as any).heartbeatTimer);
    // Quit its internal redis connection as well
    await (workerCrash as any).redisClient.quit();

    console.log('Waiting 16 seconds for registry keys to expire (TTL is 15s)...');
    await delay(16000);

    const response5 = await fetch(`${baseUrl}/workers`);
    const result5 = await response5.json() as any;
    console.log('Active workers list after 16s crash simulation:', JSON.stringify(result5.data));

    if (result5.data.length !== 0) {
      throw new Error(`Auto-pruning failed. Worker registry still contains worker ${crashWorkerId}`);
    }
    console.log('Success: Crash worker auto-pruned successfully.');
    console.log('\n=== ALL HEARTBEAT TESTS PASSED ===');

  } catch (err: any) {
    console.error('\nERROR: Test execution failed:', err.message);
    process.exitCode = 1;
  } finally {
    console.log('[Cleanup] Stopping API Server...');
    serverProcess.kill('SIGTERM');
    await db.$disconnect();
    await redis.quit();
    console.log('Disconnected.');
  }
}

runTest().catch((err) => {
  console.error('Fatal test error:', err);
  process.exit(1);
});
