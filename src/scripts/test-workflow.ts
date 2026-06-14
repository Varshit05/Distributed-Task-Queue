import { db, connectDb } from '../config/database.js';
import { redis } from '../config/redis.js';
import { Worker } from '../core/Worker.js';
import { Scheduler } from '../core/Scheduler.js';
import { WorkflowManager } from '../core/WorkflowManager.js';
import { taskRegistry } from '../workers/handlers.js';

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function runTest() {
  console.log('=== DISTRIBUTED TASK QUEUE WORKFLOW/DAG INTEGRATION TEST ===');
  await connectDb();

  // 1. Reset Database and Redis for a clean test run
  console.log('\n[Setup] Cleaning database and Redis state...');
  try {
    await db.taskDependency.deleteMany();
    await db.taskLog.deleteMany();
    await db.task.deleteMany();
    await db.workflow.deleteMany();

    // Clean up Redis keys
    await redis.del('delayed_tasks');
    await redis.del('task_stream:default');
    await redis.del('task_stream:high');
    await redis.del('scheduler:leader:lock');

    // Remove consumer groups
    try {
      await redis.xgroup('DESTROY', 'task_stream:default', 'worker_group');
    } catch (_) {}

    console.log('[Setup] Done. System is reset to clean slate.');
  } catch (err: any) {
    console.error('[Setup] Error resetting state. Is PostgreSQL running?', err.message);
    process.exit(1);
  }

  // 2. Start Worker and Scheduler programmatically
  console.log('\n[Setup] Initializing Worker and Scheduler...');
  const worker = new Worker('default');
  for (const [name, handler] of Object.entries(taskRegistry)) {
    worker.registerHandler(name, handler);
  }
  
  const scheduler = new Scheduler();

  // Start them
  await worker.start();
  await scheduler.start();

  console.log('[Setup] Worker and Scheduler are now running.');

  // 3. Define and Submit a Successful Workflow (Workflow 1)
  console.log('\n[Workflow 1] Submitting a successful DAG workflow...');
  
  // DAG Structure:
  //      A (send_email)
  //     / \
  //    B   C (generate_report, delay: 2000ms)
  //     \ /
  //      D (send_email)
  
  const workflow1 = await WorkflowManager.submitWorkflow('Successful-DAG', [
    {
      id: 'task-A',
      name: 'send_email',
      payload: { to: 'workflow.user1@example.com', subject: 'Step A completed' },
    },
    {
      id: 'task-B',
      name: 'process_image',
      payload: { imageId: 'workflow_img_123', format: 'png', width: 400, height: 300 },
      dependsOn: ['task-A'],
    },
    {
      id: 'task-C',
      name: 'generate_report',
      payload: { userId: 'usr_wf_99', reportType: 'summary' },
      delayMs: 2000,
      dependsOn: ['task-A'],
    },
    {
      id: 'task-D',
      name: 'send_email',
      payload: { to: 'workflow.manager@example.com', subject: 'All steps completed successfully' },
      dependsOn: ['task-B', 'task-C'],
    },
  ]);

  console.log(`[Workflow 1] Submitted successfully! ID: ${workflow1.id}`);

  // Monitor Workflow 1 execution
  console.log('\n[Monitor] Monitoring Workflow 1 tasks...');
  let w1Completed = false;
  for (let poll = 1; poll <= 20; poll++) {
    const tasks = await db.task.findMany({
      where: { workflowId: workflow1.id },
      orderBy: { createdAt: 'asc' },
    });

    console.log(`\n--- Workflow 1 Poll #${poll} (${new Date().toLocaleTimeString()}) ---`);
    console.table(
      tasks.map((t) => ({
        ID: t.id.substring(0, 8),
        Name: t.name,
        Status: t.status,
        Enqueued: t.isEnqueued,
        Started: t.startedAt ? t.startedAt.toLocaleTimeString() : 'No',
        Finished: t.finishedAt ? t.finishedAt.toLocaleTimeString() : 'No',
      }))
    );

    const wf = await db.workflow.findUnique({ where: { id: workflow1.id } });
    if (wf && wf.status !== 'running') {
      console.log(`\n[Workflow 1] Completed with status: ${wf.status}`);
      w1Completed = true;
      break;
    }

    await delay(2000);
  }

  if (!w1Completed) {
    console.error('[Error] Workflow 1 timed out before completing.');
  }

  // 4. Define and Submit a Failing Workflow (Workflow 2) to test cascading cancellations
  console.log('\n[Workflow 2] Submitting a failing DAG workflow to test cascading cancellations...');
  
  // DAG Structure:
  //   E (send_email, triggers failure)
  //   |
  //   F (process_image, should be cancelled)

  const workflow2 = await WorkflowManager.submitWorkflow('Failing-DAG', [
    {
      id: 'task-E',
      name: 'send_email',
      payload: { to: 'workflow.user2@example.com', subject: 'Failing Step', shouldFail: true },
      maxRetries: 1, // Fail quickly
    },
    {
      id: 'task-F',
      name: 'process_image',
      payload: { imageId: 'should_cancel_img', format: 'jpg', width: 200, height: 200 },
      dependsOn: ['task-E'],
    },
  ]);

  console.log(`[Workflow 2] Submitted successfully! ID: ${workflow2.id}`);

  // Monitor Workflow 2 execution
  console.log('\n[Monitor] Monitoring Workflow 2 tasks...');
  let w2Completed = false;
  for (let poll = 1; poll <= 15; poll++) {
    const tasks = await db.task.findMany({
      where: { workflowId: workflow2.id },
      orderBy: { createdAt: 'asc' },
    });

    console.log(`\n--- Workflow 2 Poll #${poll} (${new Date().toLocaleTimeString()}) ---`);
    console.table(
      tasks.map((t) => ({
        ID: t.id.substring(0, 8),
        Name: t.name,
        Status: t.status,
        Enqueued: t.isEnqueued,
        Started: t.startedAt ? t.startedAt.toLocaleTimeString() : 'No',
        Finished: t.finishedAt ? t.finishedAt.toLocaleTimeString() : 'No',
        Error: t.errorMessage ? t.errorMessage.substring(0, 50) : 'None',
      }))
    );

    const wf = await db.workflow.findUnique({ where: { id: workflow2.id } });
    if (wf && wf.status !== 'running') {
      console.log(`\n[Workflow 2] Completed with status: ${wf.status}`);
      w2Completed = true;
      break;
    }

    await delay(2000);
  }

  if (!w2Completed) {
    console.error('[Error] Workflow 2 timed out before completing.');
  }

  // 5. Final Verification of Task Statuses
  console.log('\n=== FINAL VERIFICATION ===');
  const allWorkflowTasks = await db.task.findMany({
    orderBy: { finishedAt: 'asc' },
  });

  console.log('\nAll tasks execution summary (ordered by completion time):');
  console.table(
    allWorkflowTasks.map((t) => ({
      WorkflowId: t.workflowId ? t.workflowId.substring(0, 8) : 'None',
      TaskId: t.id.substring(0, 8),
      Name: t.name,
      Status: t.status,
      Started: t.startedAt ? t.startedAt.toLocaleTimeString() : 'N/A',
      Finished: t.finishedAt ? t.finishedAt.toLocaleTimeString() : 'N/A',
    }))
  );

  // Validate timing/ordering of Workflow 1
  const tA = allWorkflowTasks.find((t) => t.payload && (t.payload as any).subject === 'Step A completed');
  const tB = allWorkflowTasks.find((t) => t.payload && (t.payload as any).imageId === 'workflow_img_123');
  const tC = allWorkflowTasks.find((t) => t.payload && (t.payload as any).userId === 'usr_wf_99');
  const tD = allWorkflowTasks.find((t) => t.payload && (t.payload as any).subject === 'All steps completed successfully');

  if (tA && tB && tC && tD) {
    const orderOk =
      tA.finishedAt! < tB.startedAt! &&
      tA.finishedAt! < tC.startedAt! &&
      tB.finishedAt! < tD.startedAt! &&
      tC.finishedAt! < tD.startedAt!;

    if (orderOk) {
      console.log('✔ SUCCESS: Workflow 1 tasks executed in correct topological DAG order!');
    } else {
      console.error('❌ FAILURE: Workflow 1 tasks executed out of order!');
    }
  }

  // Validate cancellation of Workflow 2
  const tF = allWorkflowTasks.find((t) => t.payload && (t.payload as any).imageId === 'should_cancel_img');
  if (tF && tF.status === 'cancelled') {
    console.log('✔ SUCCESS: Workflow 2 cascading cancellation worked as expected! Task F was cancelled.');
  } else {
    console.error('❌ FAILURE: Task F was not cancelled correctly.');
  }

  // Gracefully stop Worker and Scheduler
  console.log('\n[Cleanup] Shutting down Worker and Scheduler...');
  await worker.stop();
  await scheduler.stop();

  await db.$disconnect();
  await redis.quit();
  console.log('[Cleanup] Done. Exiting test.');
  process.exit(0);
}

runTest().catch((error) => {
  console.error('Error running workflow integration test:', error);
  process.exit(1);
});
