import { Redis } from 'ioredis';
import { db } from '../config/database.js';
import { createRedisClient } from '../config/redis.js';

export class Scheduler {
  private redisClient: Redis;
  private running = false;
  private timer: NodeJS.Timeout | null = null;
  private recoveryTimer: NodeJS.Timeout | null = null;
  private groupName: string;
  private claimTimeoutMs: number; // Time after which a task is considered stuck/crashed

  constructor() {
    this.redisClient = createRedisClient();
    this.groupName = process.env.QUEUE_GROUP_NAME || 'worker_group';
    this.claimTimeoutMs = 30000; // 30 seconds idle timeout

    // Register custom Lua script for atomic delayed task dispatch
    this.registerLuaScripts();
  }

  private registerLuaScripts() {
    // Define a command 'dispatchdelayedtasks' on our Redis client
    // KEYS[1] = ZSet key ('delayed_tasks')
    // ARGV[1] = current epoch timestamp
    // ARGV[2] = batch size
    this.redisClient.defineCommand('dispatchdelayedtasks', {
      numberOfKeys: 1,
      lua: `
        local zset_key = KEYS[1]
        local current_time = tonumber(ARGV[1])
        local batch_size = tonumber(ARGV[2])

        -- Get due tasks from Sorted Set
        local due_tasks = redis.call('ZRANGEBYSCORE', zset_key, '-inf', current_time, 'LIMIT', 0, batch_size)

        if #due_tasks > 0 then
          for _, task_data in ipairs(due_tasks) do
            -- Split "taskId:queue"
            local task_id, queue = string.match(task_data, "([^:]+):([^:]+)")
            if task_id and queue then
              local stream_key = "task_stream:" .. queue
              -- Add to the active stream
              redis.call('XADD', stream_key, '*', 'id', task_id)
            end
            -- Remove from ZSet
            redis.call('ZREM', zset_key, task_data)
          end
        end

        return due_tasks
      `
    });
  }

  /**
   * Starts the scheduler background loops.
   */
  public async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    console.log('[Scheduler] Starting Scheduler loops...');

    // Loop 1: Poll delayed tasks every 1 second
    const pollDelayed = async () => {
      if (!this.running) return;
      try {
        await this.dispatchDelayed();
      } catch (error) {
        console.error('[Scheduler] Error dispatching delayed tasks:', error);
      }
      this.timer = setTimeout(pollDelayed, 1000);
    };

    // Loop 2: Poll stuck/crashed worker recovery every 10 seconds
    const pollRecovery = async () => {
      if (!this.running) return;
      try {
        await this.recoverCrashedWorkers();
      } catch (error) {
        console.error('[Scheduler] Error recovering crashed workers:', error);
      }
      this.recoveryTimer = setTimeout(pollRecovery, 10000);
    };

    pollDelayed();
    pollRecovery();
  }

  /**
   * Stops the scheduler background loops.
   */
  public async stop(): Promise<void> {
    console.log('[Scheduler] Stopping Scheduler...');
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    if (this.recoveryTimer) clearTimeout(this.recoveryTimer);
    await this.redisClient.quit();
    console.log('[Scheduler] Scheduler stopped.');
  }

  /**
   * Moves matured tasks from the delayed ZSet into their respective streams.
   */
  private async dispatchDelayed(): Promise<void> {
    const now = Date.now();
    const batchSize = 100;

    // Call the custom Lua script registered in `registerLuaScripts`
    // Typescript definition bypass: we cast redisClient as any to call the dynamically registered method
    const dispatched = await (this.redisClient as any).dispatchdelayedtasks(
      'delayed_tasks',
      now.toString(),
      batchSize.toString()
    ) as string[];

    if (dispatched && dispatched.length > 0) {
      console.log(`[Scheduler] Dispatched ${dispatched.length} delayed tasks to active streams.`);
    }
  }

  /**
   * Scans active streams to find messages that have been stuck in the PEL (Pending Entries List)
   * for too long, indicating the worker died.
   */
  private async recoverCrashedWorkers(): Promise<void> {
    // 1. Find all queues that have active tasks in PostgreSQL to avoid hardcoding streams
    const activeQueues = await db.task.findMany({
      select: { queue: true },
      distinct: ['queue'],
    });

    const queues = activeQueues.map((q) => q.queue);
    if (queues.length === 0) {
      queues.push('default'); // Always check default queue
    }

    for (const queue of queues) {
      const streamKey = `task_stream:${queue}`;

      try {
        // Read pending messages in the group
        // XPENDING <streamKey> <groupName> - + 50
        // Returns list of [messageId, consumerName, idleTimeMs, deliveryCount]
        const pendingInfo = await this.redisClient.xpending(
          streamKey,
          this.groupName,
          '-',
          '+',
          50
        ) as any[];

        if (!pendingInfo || pendingInfo.length === 0) {
          continue;
        }

        for (const item of pendingInfo) {
          const [messageId, consumerName, idleTimeMs, deliveryCount] = item;

          // If the message has been idle (unacknowledged) for longer than claimTimeoutMs
          if (idleTimeMs > this.claimTimeoutMs) {
            console.log(`[Scheduler] Detected stuck task in "${streamKey}". MsgID: ${messageId}, Owner: ${consumerName}, Idle: ${idleTimeMs}ms, Deliveries: ${deliveryCount}`);

            // 2. Claim the message. This transfers ownership to "scheduler_recovery"
            // XCLAIM <streamKey> <groupName> <newConsumer> <minIdleTime> <messageId>
            const claimed = await this.redisClient.xclaim(
              streamKey,
              this.groupName,
              'scheduler_recovery',
              this.claimTimeoutMs,
              messageId
            ) as any[];

            if (!claimed || claimed.length === 0) {
              continue; // Could not claim
            }

            // Extract the task ID from the claimed message fields
            const fields = claimed[0][1];
            let taskId = '';
            for (let i = 0; i < fields.length; i += 2) {
              if (fields[i] === 'id') {
                taskId = fields[i + 1];
                break;
              }
            }

            if (!taskId) {
              // Malformed task in PEL, ack it to clear it
              await this.redisClient.xack(streamKey, this.groupName, messageId);
              continue;
            }

            // 3. Inspect PostgreSQL state for this task
            const task = await db.task.findUnique({ where: { id: taskId } });
            if (!task) {
              await this.redisClient.xack(streamKey, this.groupName, messageId);
              continue;
            }

            // Write logs and decide what to do
            const taskLogger = async (msg: string, level = 'info') => {
              console.log(`[Recovery][${taskId}] [${level.toUpperCase()}] ${msg}`);
              await db.taskLog.create({
                data: { taskId, message: msg, level },
              });
            };

            if (task.retryCount < task.maxRetries) {
              // We can reschedule the task for immediate retry
              const nextRetry = task.retryCount + 1;
              console.log(`[Scheduler] Rescheduling task ${taskId} for retry #${nextRetry} due to worker failure`);

              await db.task.update({
                where: { id: taskId },
                data: {
                  status: 'pending',
                  retryCount: nextRetry,
                  errorMessage: `Worker crashed or timed out during execution (last owned by ${consumerName})`,
                },
              });

              await taskLogger(`Worker "${consumerName}" crashed or timed out. Rescheduling task (Attempt ${nextRetry}/${task.maxRetries})`, 'warn');

              // Re-enqueue the task ID to the stream
              await this.redisClient.xadd(streamKey, '*', 'id', taskId);

              // Acknowledge the old message to remove it from the PEL
              await this.redisClient.xack(streamKey, this.groupName, messageId);
            } else {
              // Max retries exceeded
              console.log(`[Scheduler] Mark task ${taskId} as failed. Retries exhausted.`);

              await db.task.update({
                where: { id: taskId },
                data: {
                  status: 'failed',
                  finishedAt: new Date(),
                  errorMessage: `Worker "${consumerName}" crashed or timed out. Retries exhausted.`,
                },
              });

              await taskLogger(`Worker "${consumerName}" crashed or timed out. Retries exhausted. Marking task as permanently failed.`, 'error');

              // Acknowledge the old message to remove it from the PEL
              await this.redisClient.xack(streamKey, this.groupName, messageId);
            }
          }
        }
      } catch (error: any) {
        if (error.message && error.message.includes('NOGROUP')) {
          // Stream or consumer group does not exist yet. This is expected if no tasks have been enqueued
          // or if the worker hasn't started yet. Skip recovery for this queue.
          console.warn(`[Scheduler] Queue stream "${streamKey}" or consumer group "${this.groupName}" does not exist yet. Skipping recovery for now.`);
        } else {
          console.error(`[Scheduler] Error in recovery loop for stream "${streamKey}":`, error);
        }
      }
    }
  }
}
