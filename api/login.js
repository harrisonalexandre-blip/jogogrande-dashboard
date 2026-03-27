const crypto = require('crypto');
const Redis = require('ioredis');

const SALT = 'jogo2024';
const REDIS_KEY = 'jg-auth-users';
const INITIAL_USERS = [
  {
    email: 'harrisonwash@gmail.com',
    name: 'Harrison',
    role: 'admin',
    hash: '496eb9af1521421ae86a33f6d53347b4e0fdc267957a54be460256c92a0645b6'
  }
];

function sha256(email, password) {
  return crypto.createHash('sha256').update(`${email}:${password}:${SALT}`).digest('hex');
}

let _redis = null;
function getRedis() {
  if (!_redis) {
    _redis = new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 2,
      connectTimeout: 5000,
      lazyConnect: false
    });
    _redis.on('error', (e) => console.error('Redis error:', e.message));
  }
  return _redis;
}

async function getUsers() {
  let users = [...INITIAL_USERS];
  if (!process.env.REDIS_URL) return users;
  try {
    const redis = getRedis();
    const data = await redis.get(REDIS_KEY);
    if (data) {
      const redisUsers = JSON.parse(data);
      // Merge: Redis users + INITIAL_USERS (initial takes priority for admin)
      const initialEmails = new Set(INITIAL_USERS.map(u => u.email.toLowerCase()));
      const extra = redisUsers.filter(u => !initialEmails.has(u.email.toLowerCase()));
      users = [...INITIAL_USERS, ...extra];
    }
    // Sync back to Redis
    await redis.set(REDIS_KEY, JSON.stringify(users));
  } catch (e) {
    console.error('getUsers error:', e.message);
  }
  return users;
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  try {
    const { email = '', password = '' } = req.body || {};
    const emailNorm = email.toLowerCase().trim();
    if (!emailNorm || !password) {
      return res.status(400).json({ error: 'E-mail e senha obrigatorios' });
    }

    const users = await getUsers();
    const h = sha256(emailNorm, password);
    const user = users.find(u => u.email.toLowerCase() === emailNorm && u.hash === h);
    if (!user) return res.status(401).json({ error: 'E-mail ou senha incorretos' });

    return res.status(200).json({ email: user.email, name: user.name, role: user.role });
  } catch (err) {
    console.error('login handler error:', err.message);
    return res.status(500).json({ error: 'Erro interno no servidor' });
  }
};
