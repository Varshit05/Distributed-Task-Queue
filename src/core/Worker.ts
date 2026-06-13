import { Redis } from 'ioredis';
import os from 'os';
import { db } from '../config/database.js';
import { createRedisClient } from '../config/redis.js';

export type TaskHandler = (
  payload: any, 
  log: (message: string, level?: string) => Promise<void>,
  context: { taskId: string; retryCount: number }
) => Promise<any>;

export class Worker {
  private redisClient: Redis;
  private running = false;
  private workerId: string;
  private queue: string;
  private queues: string[];
  private streamKeys: string[];
  private handlers: Map<string, TaskHandler> = new Map();
  private streamKey: string;
  private groupName: string;
  private activeTaskPromise: Promise<void> | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private heartbeatIntervalMs = 5000;
  private heartbeatTtlSec = 15;
  private startedAt: Date;

  constructor(queue = 'default') {
    this.queue = queue;
    this.queues = queue.split(',').map(q => q.trim()).filter(Boolean);
    if (this.queues.length === 0) {
      this.queues = ['default'];
      this.queue = 'default';
    }
    this.redisClient = createRedisClient();
    this.workerId = `worker:${os.hostname()}:${process.pid}:${Math.random().toString(36).substring(2, 8)}`;
    this.streamKeys = this.queues.map(q => `task_stream:${q}`);
    this.streamKey = this.streamKeys[0];
    this.groupName = process.env.QUEUE_GROUP_NAME || 'worker_group';
    this.startedAt = new Date();
  }

  /**
   * Registers a task handler for a specific task name.
   */
  public registerHandler(name: string, handler: TaskHandler): this {
    this.handlers.set(name, handler);
    return this;
  }

  /**
   * Starts the worker processing loop.
   */
  public async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.startedAt = new Date();
    console.log(`[Worker ${this.workerId}] Starting consumer on queues: "${this.queue}"`);

    // Ensure Consumer Group exists
    await this.ensureConsumerGroup();

    // Start heartbeats
    await this.startHeartbeat();

    // Run loop
    this.runLoop();
  }

  /**
   * Gracefully stops the worker process.
   */
  public async stop(): Promise<void> {
    console.log(`[Worker ${this.workerId}] Shutting down...`);
    this.running = false;
    
    // Clear heartbeat timer
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }

    // Wait for the active task to complete execution
    if (this.activeTaskPromise) {
      await this.activeTaskPromise;
    }

    // Unregister worker registry entries
    try {
      await this.redisClient
        .multi()
        .zrem('workers:active', this.workerId)
        .del(`worker:info:${this.workerId}`)
        .exec();
      console.log(`[Worker ${this.workerId}] Unregistered from registry.`);
    } catch (err) {
      console.error(`[Worker ${this.workerId}] Failed to unregister during shutdown:`, err);
    }

    await this.redisClient.quit();
    console.log(`[Worker ${this.workerId}] Stopped successfully.`);
  }

  private async startHeartbeat(): Promise<void> {
    try {
      await this.sendHeartbeat();
    } catch (err) {
      console.error(`[Worker ${this.workerId}] Failed to send initial heartbeat:`, err);
    }
    this.heartbeatTimer = setInterval(async () => {
      try {
        await this.sendHeartbeat();
      } catch (err) {
        console.error(`[Worker ${this.workerId}] Failed to send periodic heartbeat:`, err);
      }
    }, this.heartbeatIntervalMs);
  }

  private async sendHeartbeat(): Promise<void> {
    const now = new Date();
    const metadata = {
      id: this.workerId,
      queues: this.queues,
      startedAt: this.startedAt.toISOString(),
      lastHeartbeat: now.toISOString(),
      hostname: os.hostname(),
      pid: process.pid,
      status: 'active'
    };

    const score = now.getTime();
    await this.redisClient
      .multi()
      .zadd('workers:active', score, this.workerId)
      .setex(`worker:info:${this.workerId}`, this.heartbeatTtlSec, JSON.stringify(metadata))
      .exec();
  }

  private async ensureConsumerGroup(): Promise<void> {
    for (const streamKey of this.streamKeys) {
      try {
        // Create consumer group. 
        // MKSTREAM option creates the stream if it doesn't already exist.
        // '0' means read all messages from the beginning of the stream.
        await this.redisClient.xgroup('CREATE', streamKey, this.groupName, '0', 'MKSTREAM');
        console.log(`[Worker ${this.workerId}] Created consumer group "${this.groupName}" for stream "${streamKey}"`);
      } catch (error: any) {
        if (error.message && error.message.includes('BUSYGROUP')) {
          // Group already exists, which is expected on restarts
          console.log(`[Worker ${this.workerId}] Consumer group "${this.groupName}" already exists for stream "${streamKey}"`);
        } else {
          console.error(`[Worker ${this.workerId}] Error creating consumer group for ${streamKey}:`, error);
          throw error;
        }
      }
    }
  }

  private async runLoop(): Promise<void> {
    while (this.running) {
      try {
        let response: any = null;

        // 1. Non-blocking check of each stream in priority order
        for (const streamKey of this.streamKeys) {
          const res = await this.redisClient.xreadgroup(
            'GROUP', this.groupName, this.workerId,
            'COUNT', '1',
            'STREAMS', streamKey,
            '>'
          ) as any;

          if (res && res.length > 0 && res[0][1].length > 0) {
            response = res;
            break;
          }
        }

        // 2. If all streams were empty, block on all streams together
        if (!response) {
          response = await this.redisClient.xreadgroup(
            'GROUP', this.groupName, this.workerId,
            'COUNT', '1',
            'BLOCK', '2000',
            'STREAMS', ...this.streamKeys,
            ...Array(this.streamKeys.length).fill('>')
          ) as any;
        }

        if (!response || response.length === 0) {
          continue; // Timeout, check if still running
        }

        // Process all returned messages (usually just one, but handle multiple to prevent PEL leaks)
        for (const streamResponse of response) {
          const [stream, messages] = streamResponse;
          if (messages.length === 0) continue;

          const [messageId, fields] = messages[0];
          const fieldsMap: Record<string, string> = {};
          for (let i = 0; i < fields.length; i += 2) {
            fieldsMap[fields[i]] = fields[i + 1];
          }

          const taskId = fieldsMap['id'];
          if (!taskId) {
            console.warn(`[Worker ${this.workerId}] Received malformed message in stream ${stream}: missing task ID. Acknowledging.`);
            await this.redisClient.xack(stream, this.groupName, messageId);
            continue;
          }

          // Process task asynchronously and capture its promise
          this.activeTaskPromise = this.processTask(taskId, messageId, stream);
          await this.activeTaskPromise;
          this.activeTaskPromise = null;
        }

      } catch (error: any) {
        console.error(`[Worker ${this.workerId}] Error in consumer loop:`, error);
        
        // Auto-heal: If the consumer group was deleted or reset, attempt to re-create it
        if (error.message && error.message.includes('NOGROUP')) {
          console.log(`[Worker ${this.workerId}] Consumer group missing. Attempting to re-create...`);
          try {
            await this.ensureConsumerGroup();
          } catch (recreateError) {
            console.error(`[Worker ${this.workerId}] Failed to auto-heal consumer group:`, recreateError);
          }
        }

        // Sleep slightly to avoid tight loop on errors
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  private async processTask(taskId: string, messageId: string, streamKey: string): Promise<void> {
    // 1. Fetch Task details from Postgres
    const task = await db.task.findUnique({ where: { id: taskId } });

    if (!task) {
      console.warn(`[Worker ${this.workerId}] Task ${taskId} not found in database. Acknowledging Redis stream message.`);
      await this.redisClient.xack(streamKey, this.groupName, messageId);
      return;
    }

    // Idempotency check: If task is already completed or failed, ack and skip
    if (task.status === 'completed' || task.status === 'failed') {
      console.warn(`[Worker ${this.workerId}] Task ${taskId} is already in a final state: "${task.status}". Acknowledging.`);
      await this.redisClient.xack(streamKey, this.groupName, messageId);
      return;
    }

    console.log(`[Worker ${this.workerId}] Claimed task ${taskId} (${task.name}) from stream ${streamKey}`);

    // Helper logger inside the handler
    const taskLogger = async (message: string, level = 'info') => {
      console.log(`[TaskLog][${taskId}] [${level.toUpperCase()}] ${message}`);
      await db.taskLog.create({
        data: {
          taskId,
          message,
          level,
        },
      });
    };

    try {
      // 2. Mark status as processing in PostgreSQL
      await db.task.update({
        where: { id: taskId },
        data: {
          status: 'processing',
          startedAt: new Date(),
        },
      });

      await taskLogger(`Task execution started by ${this.workerId}`);

      // 3. Find registered handler
      const handler = this.handlers.get(task.name);
      if (!handler) {
        throw new Error(`No handler registered for task type: "${task.name}"`);
      }

      // 4. Run handler with optional timeout
      let executionPromise = handler(task.payload, taskLogger, { taskId: task.id, retryCount: task.retryCount });
      let timeoutId: NodeJS.Timeout | undefined;

      const timeoutMs = task.timeoutMs;
      if (timeoutMs && timeoutMs > 0) {
        const timeoutPromise = new Promise<never>((_, reject) => {
          timeoutId = setTimeout(() => {
            reject(new Error(`Task execution timed out after ${timeoutMs}ms`));
          }, timeoutMs);
        });
        executionPromise = Promise.race([executionPromise, timeoutPromise]);
      }

      const result = await executionPromise;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }

      // 5. Successful Execution
      await db.task.update({
        where: { id: taskId },
        data: {
          status: 'completed',
          finishedAt: new Date(),
        },
      });

      await taskLogger(`Task execution completed successfully. Result: ${JSON.stringify(result || {})}`);

      // Acknowledge in Redis Stream
      await this.redisClient.xack(streamKey, this.groupName, messageId);
      console.log(`[Worker ${this.workerId}] Successfully completed task ${taskId}`);

    } catch (error: any) {
      const errorMsg = error.message || String(error);
      await taskLogger(`Task execution failed: ${errorMsg}`, 'error');

      // 6. Handle Retry Mechanism / terminal failure
      await this.handleFailure(task.id, task.name, task.payload, task.retryCount, task.maxRetries, errorMsg, messageId, task.queue, streamKey);
    }
  }

  private async handleFailure(
    taskId: string,
    name: string,
    payload: any,
    currentRetryCount: number,
    maxRetries: number,
    errorMessage: string,
    messageId: string,
    queueName: string,
    streamKey: string
  ): Promise<void> {
    if (currentRetryCount < maxRetries) {
      const nextRetry = currentRetryCount + 1;
      
      // Calculate exponential backoff (e.g., 5s, 10s, 20s, 40s...)
      const backoffDelayMs = 5000 * Math.pow(2, nextRetry - 1);
      const runAt = new Date(Date.now() + backoffDelayMs);

      console.log(`[Worker ${this.workerId}] Task ${taskId} failed. Retrying (Attempt ${nextRetry}/${maxRetries}) in ${backoffDelayMs}ms`);

      // Update task in PostgreSQL to pending state
      await db.task.update({
        where: { id: taskId },
        data: {
          status: 'pending',
          retryCount: nextRetry,
          runAt,
          errorMessage,
        },
      });

      await db.taskLog.create({
        data: {
          taskId,
          message: `Task rescheduled for retry #${nextRetry} at ${runAt.toISOString()} due to failure`,
          level: 'warn',
        },
      });

      // Add task back to Redis Sorted Set for delayed execution
      const score = runAt.getTime();
      await this.redisClient.zadd('delayed_tasks', score, `${taskId}:${queueName}`);

      // Acknowledge this specific execution turn in the active stream (since it is now scheduled as delayed)
      await this.redisClient.xack(streamKey, this.groupName, messageId);

    } else {
      // Terminal Failure
      console.error(`[Worker ${this.workerId}] Task ${taskId} failed. Retries exhausted (${maxRetries}/${maxRetries}).`);

      await db.task.update({
        where: { id: taskId },
        data: {
          status: 'failed',
          finishedAt: new Date(),
          errorMessage,
        },
      });

      await db.taskLog.create({
        data: {
          taskId,
          message: `Task failed permanently after ${maxRetries} retries. Error: ${errorMessage}`,
          level: 'error',
        },
      });

      // Route to DLQ stream
      try {
        await this.redisClient.xadd(
          'task_stream:dlq',
          '*',
          'id', taskId,
          'originalQueue', queueName,
          'error', errorMessage
        );
        console.log(`[Worker ${this.workerId}] Routed task ${taskId} to Dead Letter Queue (DLQ)`);
      } catch (dlqError) {
        console.error(`[Worker ${this.workerId}] Failed to push task ${taskId} to DLQ:`, dlqError);
      }

      // Acknowledge to remove it from PEL
      await this.redisClient.xack(streamKey, this.groupName, messageId);
    }
  }
}
