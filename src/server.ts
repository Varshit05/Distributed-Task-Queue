import express, { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import dotenv from 'dotenv';
import parser from 'cron-parser';
import { connectDb, db } from './config/database.js';
import { redis } from './config/redis.js';
import { Producer } from './core/Producer.js';
import { taskRegistry } from './workers/handlers.js';

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

// Zod Schema for Task Submission Validation
const SubmitTaskSchema = z.object({
  name: z.string().refine((val) => Object.keys(taskRegistry).includes(val), {
    message: `Invalid task name. Allowed values: ${Object.keys(taskRegistry).join(', ')}`,
  }),
  payload: z.record(z.any()),
  options: z.object({
    delayMs: z.number().nonnegative().optional(),
    maxRetries: z.number().nonnegative().optional(),
    queue: z.string().min(1).optional(),
    timeoutMs: z.number().nonnegative().optional(),
  }).optional(),
});

// Endpoint: Submit a task
app.post('/api/tasks', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const parsed = SubmitTaskSchema.safeParse(req.body);
    
    if (!parsed.success) {
      return res.status(400).json({
        success: false,
        error: 'Validation failed',
        details: parsed.error.flatten().fieldErrors,
      });
    }

    const { name, payload, options } = parsed.data;
    const task = await Producer.submitTask(name, payload, options);

    return res.status(201).json({
      success: true,
      message: 'Task submitted successfully',
      data: {
        id: task.id,
        name: task.name,
        queue: task.queue,
        status: task.status,
        runAt: task.runAt,
      },
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Track task status
app.get('/api/tasks/:id', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;
    const task = await db.task.findUnique({
      where: { id },
    });

    if (!task) {
      return res.status(404).json({
        success: false,
        error: 'Task not found',
      });
    }

    return res.status(200).json({
      success: true,
      data: task,
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Get task audit logs
app.get('/api/tasks/:id/logs', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;

    // Check if task exists
    const taskExists = await db.task.findUnique({
      where: { id },
      select: { id: true },
    });

    if (!taskExists) {
      return res.status(404).json({
        success: false,
        error: 'Task not found',
      });
    }

    // Retrieve logs sorted by creation time
    const logs = await db.taskLog.findMany({
      where: { taskId: id },
      orderBy: { createdAt: 'asc' },
    });

    return res.status(200).json({
      success: true,
      data: logs,
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Manually retry a failed or cancelled task
app.post('/api/tasks/:id/retry', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;

    // 1. Find the task in DB
    const task = await db.task.findUnique({
      where: { id },
    });

    if (!task) {
      return res.status(404).json({
        success: false,
        error: 'Task not found',
      });
    }

    // Only allow retrying terminal failure or cancelled states
    if (task.status !== 'failed' && task.status !== 'cancelled') {
      return res.status(400).json({
        success: false,
        error: `Cannot retry task with status "${task.status}". Only "failed" or "cancelled" tasks can be retried.`,
      });
    }

    const nextRetryCount = 0; // Reset retry count for manual execution
    const runAt = new Date();

    // 2. Update task state in PostgreSQL to pending
    const updatedTask = await db.task.update({
      where: { id },
      data: {
        status: 'pending',
        retryCount: nextRetryCount,
        errorMessage: null,
        runAt,
        startedAt: null,
        finishedAt: null,
      },
    });

    // Write manual retry log
    await db.taskLog.create({
      data: {
        taskId: id,
        message: `Task manually resubmitted for retry`,
        level: 'info',
      },
    });

    // 3. Enqueue immediate execution to Redis stream
    const streamKey = `task_stream:${task.queue}`;
    await redis.xadd(streamKey, '*', 'id', id);

    console.log(`[Server] Manually re-enqueued task ${id} to stream: ${streamKey}`);

    return res.status(200).json({
      success: true,
      message: 'Task manually resubmitted successfully',
      data: {
        id: updatedTask.id,
        status: updatedTask.status,
        queue: updatedTask.queue,
      },
    });
  } catch (error) {
    next(error);
  }
});

// Zod Schema for Cron Job Validation
const CreateCronJobSchema = z.object({
  name: z.string().min(1),
  taskName: z.string().refine((val) => Object.keys(taskRegistry).includes(val), {
    message: `Invalid task name. Allowed values: ${Object.keys(taskRegistry).join(', ')}`,
  }),
  expression: z.string().refine((val) => {
    try {
      parser.parse(val);
      return true;
    } catch {
      return false;
    }
  }, {
    message: 'Invalid cron expression',
  }),
  payload: z.record(z.any()),
});

// Endpoint: Register/Create a Cron Job
app.post('/api/cron', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const parsed = CreateCronJobSchema.safeParse(req.body);
    
    if (!parsed.success) {
      return res.status(400).json({
        success: false,
        error: 'Validation failed',
        details: parsed.error.flatten().fieldErrors,
      });
    }

    const { name, taskName, expression, payload } = parsed.data;

    // Check if name is unique
    const existing = await db.cronJob.findUnique({
      where: { name },
    });

    if (existing) {
      return res.status(400).json({
        success: false,
        error: `Cron job with name "${name}" already exists`,
      });
    }

    // Calculate initial nextRunAt
    const interval = parser.parse(expression, { currentDate: new Date() });
    const nextRunAt = interval.next().toDate();

    const cronJob = await db.cronJob.create({
      data: {
        name,
        taskName,
        expression,
        payload,
        nextRunAt,
      },
    });

    return res.status(201).json({
      success: true,
      message: 'Cron job registered successfully',
      data: cronJob,
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: List all Cron Jobs
app.get('/api/cron', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const cronJobs = await db.cronJob.findMany({
      orderBy: { createdAt: 'desc' },
    });

    return res.status(200).json({
      success: true,
      data: cronJobs,
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Get single Cron Job by ID or Name
app.get('/api/cron/:idOrName', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { idOrName } = req.params;
    const cronJob = await db.cronJob.findFirst({
      where: {
        OR: [
          { id: idOrName },
          { name: idOrName },
        ],
      },
    });

    if (!cronJob) {
      return res.status(404).json({
        success: false,
        error: 'Cron job not found',
      });
    }

    return res.status(200).json({
      success: true,
      data: cronJob,
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Toggle Active Status (Pause/Resume)
app.patch('/api/cron/:id/toggle', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;
    const existing = await db.cronJob.findUnique({
      where: { id },
    });

    if (!existing) {
      return res.status(404).json({
        success: false,
        error: 'Cron job not found',
      });
    }

    const nextActive = !existing.isActive;
    let nextRunAt = existing.nextRunAt;

    if (nextActive) {
      // Recalculate next run from now since it is being resumed
      const interval = parser.parse(existing.expression, { currentDate: new Date() });
      nextRunAt = interval.next().toDate();
    }

    const updated = await db.cronJob.update({
      where: { id },
      data: {
        isActive: nextActive,
        nextRunAt,
      },
    });

    return res.status(200).json({
      success: true,
      message: `Cron job ${nextActive ? 'resumed' : 'paused'} successfully`,
      data: updated,
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Manually trigger a Cron Job immediately
app.post('/api/cron/:id/trigger', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;
    const cronJob = await db.cronJob.findUnique({
      where: { id },
    });

    if (!cronJob) {
      return res.status(404).json({
        success: false,
        error: 'Cron job not found',
      });
    }

    // Submit task immediately to queue
    const task = await Producer.submitTask(cronJob.taskName, cronJob.payload as Record<string, any>, {
      queue: 'default',
    });

    await db.taskLog.create({
      data: {
        taskId: task.id,
        message: `Manually triggered execution of cron job "${cronJob.name}"`,
        level: 'info',
      },
    });

    return res.status(200).json({
      success: true,
      message: `Cron job "${cronJob.name}" manually triggered successfully`,
      data: {
        taskId: task.id,
        status: task.status,
      },
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Delete a Cron Job
app.delete('/api/cron/:id', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const { id } = req.params;
    
    // Check existence
    const existing = await db.cronJob.findUnique({
      where: { id },
      select: { id: true, name: true },
    });

    if (!existing) {
      return res.status(404).json({
        success: false,
        error: 'Cron job not found',
      });
    }

    await db.cronJob.delete({
      where: { id },
    });

    return res.status(200).json({
      success: true,
      message: `Cron job "${existing.name}" deleted successfully`,
    });
  } catch (error) {
    next(error);
  }
});

// Endpoint: Get all active workers
app.get('/api/workers', async (req: Request, res: Response, next: NextFunction) => {
  try {
    const now = Date.now();
    // 1. Prune dead workers from active set (inactive for > 15s)
    await redis.zremrangebyscore('workers:active', '-inf', now - 15000);

    // 2. Fetch all active worker IDs
    const workerIds = await redis.zrange('workers:active', 0, -1);

    if (workerIds.length === 0) {
      return res.status(200).json({
        success: true,
        data: [],
      });
    }

    // 3. Fetch metadata for all active workers in a pipeline
    const pipeline = redis.pipeline();
    for (const workerId of workerIds) {
      pipeline.get(`worker:info:${workerId}`);
    }

    const rawResults = await pipeline.exec();
    const workers: any[] = [];

    if (rawResults) {
      for (const [err, result] of rawResults) {
        if (err) {
          console.error('[Server] Error fetching worker info from Redis pipeline:', err);
          continue;
        }
        if (result && typeof result === 'string') {
          try {
            workers.push(JSON.parse(result));
          } catch (parseErr) {
            console.error('[Server] Failed to parse worker metadata:', parseErr);
          }
        }
      }
    }

    return res.status(200).json({
      success: true,
      data: workers,
    });
  } catch (error) {
    next(error);
  }
});

// Global Error Handler
app.use((err: any, req: Request, res: Response, next: NextFunction) => {
  console.error('[Express Error]', err);
  return res.status(500).json({
    success: false,
    error: 'Internal Server Error',
    message: process.env.NODE_ENV === 'development' ? err.message : undefined,
  });
});

// Start Server
async function startServer() {
  await connectDb();
  app.listen(PORT, () => {
    console.log(`[Server] API server running on port ${PORT} in ${process.env.NODE_ENV} mode`);
  });
}

startServer().catch((err) => {
  console.error('[Server] Failed to start:', err);
  process.exit(1);
});
