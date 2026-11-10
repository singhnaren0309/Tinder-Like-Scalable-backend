'use strict';

require('dotenv').config();

const cron = require('node-cron');
const { createLogger } = require('../../../shared/src/utils/logger');
const { feedGeneratorWorker } = require('./feed-generator');

const logger = createLogger('FeedCronWorker');
const CRON_INTERVAL = process.env.CRON_FEED_INTERVAL || '*/10 * * * *'; // Every 10 minutes

logger.info(`⏰ Initializing Feed Stack Pre-Computation Cron Job (Schedule: "${CRON_INTERVAL}")`);

// Run an initial feed generation on startup
feedGeneratorWorker
  .runBatchGeneration()
  .then(() => logger.info('Initial feed generation cycle complete.'))
  .catch((err) => logger.error(`Initial feed generation error: ${err.message}`));

// Schedule recurring cron job
cron.schedule(CRON_INTERVAL, async () => {
  logger.info('[CRON TICK] Running scheduled feed stack pre-population job...');
  try {
    await feedGeneratorWorker.runBatchGeneration();
  } catch (err) {
    logger.error(`Cron job execution failed: ${err.message}`);
  }
});

module.exports = feedGeneratorWorker;

