import { Redis } from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';

// Standard connection for producing tasks, publishing events, and general operations
export const redis = new Redis(REDIS_URL, {
  maxRetriesPerRequest: null, // ioredis recommends setting this to null when using blocking commands/streams
});

// Helper to create dedicated blocking connections for Workers and Schedulers
export function createRedisClient(): Redis {
  return new Redis(REDIS_URL, {
    maxRetriesPerRequest: null,
  });
}

redis.on('connect', () => {
  console.log('Successfully connected to Redis');
});

redis.on('error', (err) => {
  console.error('Redis connection error:', err);
});
