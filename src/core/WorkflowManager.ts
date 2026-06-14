import { db } from '../config/database.js';
import { redis } from '../config/redis.js';

export interface WorkflowTaskInput {
  id: string; // Temporary local identifier for referencing in dependsOn
  name: string;
  payload: Record<string, any>;
  queue?: string;
  maxRetries?: number;
  timeoutMs?: number;
  delayMs?: number;
  dependsOn?: string[]; // Array of local task IDs that this task depends on
}

export class WorkflowManager {
  /**
   * Validates that the workflow tasks form a Directed Acyclic Graph (DAG)
   * with no cycles and all dependencies exist.
   */
  public static validateDAG(tasks: WorkflowTaskInput[]): void {
    const allIds = new Set<string>();
    for (const t of tasks) {
      if (allIds.has(t.id)) {
        throw new Error(`Duplicate task ID "${t.id}" detected in workflow`);
      }
      allIds.add(t.id);
    }

    const adjList = new Map<string, string[]>();
    for (const t of tasks) {
      adjList.set(t.id, []);
    }

    for (const t of tasks) {
      if (t.dependsOn) {
        for (const parentId of t.dependsOn) {
          if (!allIds.has(parentId)) {
            throw new Error(`Task "${t.id}" depends on non-existent task "${parentId}"`);
          }
          // Edge from parent to child
          adjList.get(parentId)!.push(t.id);
        }
      }
    }

    const visited = new Set<string>();
    const recStack = new Set<string>();

    const hasCycle = (node: string): boolean => {
      visited.add(node);
      recStack.add(node);

      const children = adjList.get(node) || [];
      for (const child of children) {
        if (!visited.has(child)) {
          if (hasCycle(child)) return true;
        } else if (recStack.has(child)) {
          return true;
        }
      }

      recStack.delete(node);
      return false;
    };

    for (const t of tasks) {
      if (!visited.has(t.id)) {
        if (hasCycle(t.id)) {
          throw new Error("Cyclic dependency detected in workflow DAG!");
        }
      }
    }
  }

  /**
   * Submits a workflow, saves all tasks and dependencies, and enqueues root tasks.
   */
  public static async submitWorkflow(name: string, tasks: WorkflowTaskInput[]) {
    // 1. Validate DAG
    this.validateDAG(tasks);

    // 2. Create Workflow
    const workflow = await db.workflow.create({
      data: {
        name,
        status: 'running',
      },
    });

    const localToDbIdMap = new Map<string, string>();

    // 3. Create all tasks in pending state as part of a transaction
    await db.$transaction(async (tx) => {
      for (const t of tasks) {
        const dbTask = await tx.task.create({
          data: {
            name: t.name,
            queue: t.queue || 'default',
            status: 'pending',
            payload: t.payload,
            maxRetries: t.maxRetries ?? parseInt(process.env.DEFAULT_MAX_RETRIES || '3', 10),
            timeoutMs: t.timeoutMs,
            delayMs: t.delayMs,
            isEnqueued: false,
            workflowId: workflow.id,
          },
        });
        localToDbIdMap.set(t.id, dbTask.id);
      }

      // 4. Create TaskDependency relations
      for (const t of tasks) {
        if (t.dependsOn) {
          for (const parentLocalId of t.dependsOn) {
            const upstreamTaskId = localToDbIdMap.get(parentLocalId)!;
            const downstreamTaskId = localToDbIdMap.get(t.id)!;

            await tx.taskDependency.create({
              data: {
                upstreamTaskId,
                downstreamTaskId,
              },
            });
          }
        }
      }
    });

    console.log(`[WorkflowManager] Workflow "${name}" (${workflow.id}) submitted with ${tasks.length} tasks.`);

    // 5. Enqueue root tasks (0 dependencies)
    const rootTasks = tasks.filter((t) => !t.dependsOn || t.dependsOn.length === 0);
    for (const r of rootTasks) {
      const dbId = localToDbIdMap.get(r.id)!;
      const taskObj = await db.task.findUnique({ where: { id: dbId } });
      if (taskObj) {
        await this.enqueueWorkflowTask(taskObj);
      }
    }

    return workflow;
  }

  /**
   * Helper to atomically enqueue a workflow task into Redis and set its runAt schedule.
   */
  private static async enqueueWorkflowTask(task: any) {
    // Lock-free atomic transition using updateMany
    const affected = await db.task.updateMany({
      where: {
        id: task.id,
        isEnqueued: false,
      },
      data: {
        isEnqueued: true,
      },
    });

    if (affected.count === 0) {
      // Already enqueued
      return;
    }

    const queue = task.queue;
    const delayMs = task.delayMs || 0;
    const runAt = new Date(Date.now() + delayMs);
    const isDelayed = delayMs > 0;

    await db.task.update({
      where: { id: task.id },
      data: {
        runAt,
      },
    });

    await db.taskLog.create({
      data: {
        taskId: task.id,
        message: isDelayed
          ? `Task scheduled to run in workflow at ${runAt.toISOString()} (with delay ${delayMs}ms)`
          : `Task submitted to workflow for execution`,
        level: 'info',
      },
    });

    if (isDelayed) {
      const score = runAt.getTime();
      await redis.zadd('delayed_tasks', score, `${task.id}:${queue}`);
      console.log(`[WorkflowManager] Scheduled delayed task ${task.id} (${task.name}) in ${delayMs}ms`);
    } else {
      const streamKey = `task_stream:${queue}`;
      await redis.xadd(streamKey, '*', 'id', task.id);
      console.log(`[WorkflowManager] Enqueued immediate task ${task.id} (${task.name}) to stream: ${streamKey}`);
    }
  }

  /**
   * Evaluates if downstream dependencies of a completed task should be triggered,
   * and completes the workflow if all tasks are finished.
   */
  public static async handleTaskCompletion(taskId: string): Promise<void> {
    const task = await db.task.findUnique({
      where: { id: taskId },
    });

    if (!task || !task.workflowId) return;

    // Fetch downstream dependencies
    const dependencies = await db.taskDependency.findMany({
      where: { upstreamTaskId: taskId },
      include: {
        downstreamTask: {
          include: {
            dependencies: {
              include: {
                upstreamTask: true,
              },
            },
          },
        },
      },
    });

    for (const dep of dependencies) {
      const downstream = dep.downstreamTask;

      if (downstream.status !== 'pending') continue;

      const allUpstream = downstream.dependencies;
      const allCompleted = allUpstream.every((u) => u.upstreamTask.status === 'completed');

      if (allCompleted) {
        console.log(`[WorkflowManager] Dependencies met for downstream task ${downstream.id} (${downstream.name}). Triggering...`);
        await this.enqueueWorkflowTask(downstream);
      }
    }

    // Check if workflow is finished
    const remainingTasks = await db.task.count({
      where: {
        workflowId: task.workflowId,
        status: {
          notIn: ['completed', 'failed', 'cancelled'],
        },
      },
    });

    if (remainingTasks === 0) {
      const failedCount = await db.task.count({
        where: {
          workflowId: task.workflowId,
          status: 'failed',
        },
      });

      const finalStatus = failedCount > 0 ? 'failed' : 'completed';

      await db.workflow.updateMany({
        where: {
          id: task.workflowId,
          status: 'running',
        },
        data: {
          status: finalStatus,
        },
      });

      console.log(`[WorkflowManager] Workflow ${task.workflowId} finished with status "${finalStatus}"`);
    }
  }

  /**
   * Flags the workflow as failed and cancels all unfinished tasks within it.
   */
  public static async handleTaskFailure(taskId: string, errorMessage: string): Promise<void> {
    const task = await db.task.findUnique({
      where: { id: taskId },
    });

    if (!task || !task.workflowId) return;

    // Mark workflow as failed
    await db.workflow.updateMany({
      where: {
        id: task.workflowId,
        status: 'running',
      },
      data: {
        status: 'failed',
      },
    });

    // Find and cancel all pending / processing tasks in the workflow
    const nonFinalTasks = await db.task.findMany({
      where: {
        workflowId: task.workflowId,
        status: {
          notIn: ['completed', 'failed', 'cancelled'],
        },
      },
    });

    for (const t of nonFinalTasks) {
      await db.task.update({
        where: { id: t.id },
        data: {
          status: 'cancelled',
          finishedAt: new Date(),
          errorMessage: `Cancelled due to upstream failure of task ${taskId}: ${errorMessage}`,
        },
      });

      await db.taskLog.create({
        data: {
          taskId: t.id,
          message: `Task cancelled due to workflow failure. Upstream task ${taskId} failed: ${errorMessage}`,
          level: 'error',
        },
      });

      console.log(`[WorkflowManager] Cancelled task ${t.id} (${t.name}) due to workflow failure.`);
    }
  }
}
