const jwt = require('jsonwebtoken');

const JWT_SECRET = process.env.JWT_SECRET || 'tinder_super_secret_jwt_key_scalable_2026';

function authMiddleware(req, res, next) {
  // Allow unauthenticated access to health, auth endpoints, and email inspection
  if (
    req.path.startsWith('/api/v1/auth') ||
    req.path === '/api/v1/notifications/emails' ||
    req.path === '/health' ||
    req.path === '/'
  ) {
    return next();
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    return res.status(401).json({
      success: false,
      error: 'Unauthorized: Missing or invalid authorization token format (Bearer <token>)',
    });
  }

  const token = authHeader.split(' ')[1];
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    req.user = decoded;
    req.headers['x-user-id'] = decoded.userId;
    req.headers['x-user-email'] = decoded.email;
    next();
  } catch (err) {
    return res.status(401).json({
      success: false,
      error: 'Unauthorized: Invalid or expired token',
    });
  }
}

module.exports = {
  authMiddleware,
};

