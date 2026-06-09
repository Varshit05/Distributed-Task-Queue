import express, { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import dotenv from 'dotenv';
import { connectDb, db } from './config/database.js';
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
