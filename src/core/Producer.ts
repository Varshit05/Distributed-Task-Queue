import { db } from '../config/database.js';
import { redis } from '../config/redis.js';

export interface SubmitTaskOptions {
  delayMs?: number;      // Optional delay in milliseconds
  maxRetries?: number;   // Optional maximum retry count (defaults to 3)
  queue?: string;        // Optional queue name (defaults to "default")
  timeoutMs?: number;    // Optional execution timeout in milliseconds
}

export class Producer {
  /**
   * Submits a new task for distributed execution.
   * 
   * @param name The name of the task handler (e.g., "send_email", "image_processing")
   * @param payload JSON serializable object representing the task arguments
   * @param options Execution settings like delay and retries
   */
  static async submitTask(
    name: string,
    payload: Record<string, any>,
    options: SubmitTaskOptions = {}
  ) {
    const queue = options.queue || 'default';
    const maxRetries = options.maxRetries ?? parseInt(process.env.DEFAULT_MAX_RETRIES || '3', 10);
    const delayMs = options.delayMs || 0;

    const runAt = new Date(Date.now() + delayMs);
    const isDelayed = delayMs > 0;

    // 1. Persist the task in PostgreSQL (Source of Truth)
    const task = await db.task.create({
      data: {
        name,
        queue,
        status: 'pending',
        payload,
        maxRetries,
        timeoutMs: options.timeoutMs,
        runAt,
        isEnqueued: true,
      },
    });

    // Write audit log entry
    await db.taskLog.create({
      data: {
        taskId: task.id,
        message: isDelayed 
          ? `Task submitted and scheduled to run at ${runAt.toISOString()}` 
          : `Task submitted for immediate execution`,
        level: 'info',
      },
    });

    if (isDelayed) {
      // 2a. For delayed tasks: Add to Redis ZSet (delayed_tasks)
      // Score = Epoch timestamp in milliseconds
      const score = runAt.getTime();
      await redis.zadd('delayed_tasks', score, `${task.id}:${queue}`);
      
      console.log(`[Producer] Scheduled delayed task ${task.id} (${name}) to run in ${delayMs}ms`);
    } else {
      // 2b. For immediate tasks: Push task ID to Redis Stream
      const streamKey = `task_stream:${queue}`;
      
      // XADD key * id <task_id>
      // '*' means Redis will generate the stream ID automatically (timestamp-sequence)
      await redis.xadd(streamKey, '*', 'id', task.id);
      
      console.log(`[Producer] Enqueued immediate task ${task.id} (${name}) to stream: ${streamKey}`);
    }

    return task;
  }
}
