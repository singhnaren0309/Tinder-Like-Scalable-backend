const { createLogger } = require('../../../shared/src/utils/logger');

const logger = createLogger('GatewayErrorHandler');

function errorHandler(err, req, res, next) {
  logger.error(`Unhandled error at ${req.method} ${req.url}: ${err.message}`, { stack: err.stack });

  res.status(err.status || 500).json({
    success: false,
    error: err.message || 'Internal Server Error in API Gateway',
  });
}

module.exports = {
  errorHandler,
};

