import { TaskHandler } from '../core/Worker.js';

// Helper to pause execution
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
   * Simulates sending an email.
   * Payload: { to: string; subject: string; body: string; shouldFail?: boolean }
   */
export const sendEmailHandler: TaskHandler = async (payload, log, context) => {
  const { to, subject, shouldFail } = payload;
  
  await log(`Preparing to send email to ${to} with subject "${subject}"...`);
  await delay(1000);

  if (shouldFail) {
    await log(`Simulating email delivery failure to ${to}`, 'warn');
    throw new Error(`SMTP connection timeout for server mail.${to.split('@')[1]}`);
  }

  await delay(1000);
  await log(`Email sent successfully to ${to} (Message ID: msg_${Math.random().toString(36).substring(2, 10)})`);
  
  return { delivered: true, sentAt: new Date().toISOString() };
};

/**
   * Simulates resizing / processing an image.
   * Payload: { imageId: string; format: string; width: number; height: number }
   */
export const processImageHandler: TaskHandler = async (payload, log) => {
  const { imageId, format, width, height } = payload;

  await log(`Loading image ${imageId} from cloud storage...`);
  await delay(800);

  await log(`Processing image operations: Resize to ${width}x${height}, convert to ${format}...`);
  await delay(1500);

  await log(`Saving optimized image ${imageId}_${width}x${height}.${format} back to bucket...`);
  await delay(700);

  return { 
    success: true, 
    path: `/uploads/processed/${imageId}_${width}x${height}.${format}`,
    bytesSaved: Math.floor(Math.random() * 500000) + 100000
  };
};

/**
   * Simulates generating a PDF report.
   * Payload: { userId: string; reportType: string; failAttempts?: number }
   */
export const generateReportHandler: TaskHandler = async (payload, log, context) => {
  const { userId, reportType, failAttempts = 0 } = payload;
  const currentRetry = context.retryCount;

  await log(`Starting report generation of type "${reportType}" for user ${userId}...`);
  await delay(1000);

  // If failAttempts is set, we fail for the first 'failAttempts' times, and succeed on the next retry.
  // This is perfect for verifying the retry and backoff system!
  if (currentRetry < failAttempts) {
    await log(`Database lock encountered while generating report for user ${userId}. Retrying soon...`, 'warn');
    throw new Error(`Database transaction deadlock on user_activity table (Retry attempt ${currentRetry + 1}/${failAttempts} expected to fail)`);
  }

  await delay(1500);
  await log(`PDF generated and uploaded. Rendering ${Math.floor(Math.random() * 50) + 10} pages of transaction history...`);
  await delay(500);

  return {
    reportUrl: `https://storage.taskqueue.internal/reports/${userId}-${reportType}-${Date.now()}.pdf`,
    pages: Math.floor(Math.random() * 50) + 10,
    generatedAt: new Date().toISOString()
  };
};

// Registry mapping task names to their handler functions
export const taskRegistry: Record<string, TaskHandler> = {
  send_email: sendEmailHandler,
  process_image: processImageHandler,
  generate_report: generateReportHandler,
};
