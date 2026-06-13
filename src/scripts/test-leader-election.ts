import { connectDb, db } from '../config/database.js';
import { redis } from '../config/redis.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// Set short lease timings so the test runs quickly
process.env.SCHEDULER_LEASE_DURATION_MS = '2000';
process.env.SCHEDULER_LEASE_RENEW_INTERVAL_MS = '500';

import { Scheduler } from '../core/Scheduler.js';

async function runTest() {
  console.log('=== SCHEDULER LEADER ELECTION TEST ===\n');
  await connectDb();

  const leaseLockKey = process.env.SCHEDULER_LEASE_KEY || 'scheduler:leader:lock';

  // 1. Reset Redis state
  console.log('[Setup] Cleaning up leader lease key in Redis...');
  await redis.del(leaseLockKey);
  console.log('[Setup] Done.\n');

  // 2. Initialize two schedulers
  console.log('[Test] 1. Initializing scheduler1 and scheduler2...');
  const scheduler1 = new Scheduler();
  const scheduler2 = new Scheduler();

  console.log(`scheduler1 ID: ${scheduler1.id}`);
  console.log(`scheduler2 ID: ${scheduler2.id}`);

  // 3. Start scheduler1
  console.log('\n[Test] 2. Starting scheduler1...');
  await scheduler1.start();
  
  // Wait a bit to let it acquire the lease
  await delay(100);
  console.log(`scheduler1 isLeader: ${scheduler1.isLeader}`);
  if (!scheduler1.isLeader) {
    throw new Error('scheduler1 failed to become leader on startup');
  }

  // Verify lease key in Redis
  const activeLeaderId = await redis.get(leaseLockKey);
  console.log(`Redis leader lock contains: ${activeLeaderId}`);
  if (activeLeaderId !== scheduler1.id) {
    throw new Error(`Expected Redis lock to contain scheduler1 ID (${scheduler1.id}), got: ${activeLeaderId}`);
  }
  console.log('Success: scheduler1 successfully acquired the leader lease lock.');

  // 4. Start scheduler2 (standby)
  console.log('\n[Test] 3. Starting scheduler2 (should remain standby)...');
  await scheduler2.start();

  await delay(600); // Wait for election checks
  console.log(`scheduler1 isLeader: ${scheduler1.isLeader}`);
  console.log(`scheduler2 isLeader: ${scheduler2.isLeader}`);

  if (scheduler2.isLeader) {
    throw new Error('scheduler2 incorrectly became leader while scheduler1 is active');
  }
  console.log('Success: scheduler2 remained in standby mode.');

  // 5. Graceful shutdown of scheduler1 (release lease lock)
  console.log('\n[Test] 4. Stopping scheduler1 gracefully (should release lease lock)...');
  await scheduler1.stop();

  // Wait for scheduler2's next election loop (renew interval is 500ms)
  console.log('Waiting for scheduler2 to detect release and acquire lease...');
  await delay(800);

  console.log(`scheduler2 isLeader: ${scheduler2.isLeader}`);
  if (!scheduler2.isLeader) {
    throw new Error('scheduler2 failed to take over as leader after scheduler1 stopped');
  }

  const newActiveLeaderId = await redis.get(leaseLockKey);
  console.log(`Redis leader lock now contains: ${newActiveLeaderId}`);
  if (newActiveLeaderId !== scheduler2.id) {
    throw new Error(`Expected Redis lock to contain scheduler2 ID (${scheduler2.id}), got: ${newActiveLeaderId}`);
  }
  console.log('Success: scheduler2 took over the leader lease lock successfully.');

  // 6. Stop scheduler2
  console.log('\n[Test] 5. Stopping scheduler2...');
  await scheduler2.stop();

  // Verify lease lock is removed
  const finalLeaderId = await redis.get(leaseLockKey);
  console.log(`Final Redis leader lock: ${finalLeaderId}`);
  if (finalLeaderId) {
    throw new Error(`Expected Redis lock to be deleted on shutdown, got: ${finalLeaderId}`);
  }
  console.log('Success: Redis lease lock cleaned up on final scheduler stop.');

  console.log('\n=== ALL LEADER ELECTION TESTS PASSED ===');
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
