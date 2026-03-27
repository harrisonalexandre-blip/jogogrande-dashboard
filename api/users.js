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
  if (!process.env.REDIS_URL) return { users: INITIAL_USERS };
  try {
    const redis = getRedis();
    const data = await redis.get(REDIS_KEY);
    if (data) return { users: JSON.parse(data) };
  } catch (e) {
    console.error('getUsers error:', e.message);
  }
  return { users: INITIAL_USERS };
}

async function saveUsers(users) {
  if (!process.env.REDIS_URL) throw new Error('REDIS_URL nao configurado');
  const redis = getRedis();
  await redis.set(REDIS_KEY, JSON.stringify(users));
}

async function validateAdmin(req) {
  const { adminEmail = '', adminPassword = '' } = req.body || {};
  const emailNorm = adminEmail.toLowerCase().trim();
  if (!emailNorm || !adminPassword) return false;
  const h = sha256(emailNorm, adminPassword);
  const { users } = await getUsers();
  const user = users.find(u => u.email.toLowerCase() === emailNorm && u.hash === h);
  return user && user.role === 'admin';
}

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).end();

  try {
    const body = req.body || {};
    const { action } = body;

    if (action === 'list') {
      if (!(await validateAdmin(req))) return res.status(403).json({ error: 'Nao autorizado' });
      const { users } = await getUsers();
      return res.status(200).json({
        users: users.map(u => ({ email: u.email, name: u.name, role: u.role }))
      });
    }

    if (action === 'add') {
      if (!(await validateAdmin(req))) return res.status(403).json({ error: 'Nao autorizado' });
      const newEmail = (body.newEmail || body.email || '').toLowerCase().trim();
      const newName = body.newName || body.name || '';
      const newPassword = body.newPassword || body.password || '';
      const newRole = body.newRole || body.role || 'socio';

      if (!newEmail || !newPassword || !newName) {
        return res.status(400).json({ error: 'E-mail, nome e senha obrigatorios' });
      }
      const validRoles = ['admin', 'socio', 'crm'];
      if (!validRoles.includes(newRole)) {
        return res.status(400).json({ error: 'Role invalido' });
      }

      const { users } = await getUsers();
      if (users.find(u => u.email.toLowerCase() === newEmail)) {
        return res.status(409).json({ error: 'E-mail ja cadastrado' });
      }

      const hash = sha256(newEmail, newPassword);
      users.push({ email: newEmail, name: newName, role: newRole, hash });
      await saveUsers(users);

      return res.status(200).json({ success: true, message: 'Usuario ' + newName + ' adicionado com sucesso' });
    }

    if (action === 'remove') {
      if (!(await validateAdmin(req))) return res.status(403).json({ error: 'Nao autorizado' });
      const targetEmail = (body.targetEmail || body.email || '').toLowerCase().trim();

      if (targetEmail === 'harrisonwash@gmail.com') {
        return res.status(400).json({ error: 'Nao e possivel remover o admin principal' });
      }

      const { users } = await getUsers();
      const filtered = users.filter(u => u.email.toLowerCase() !== targetEmail);
      if (filtered.length === users.length) {
        return res.status(404).json({ error: 'Usuario nao encontrado' });
      }

      await saveUsers(filtered);
      return res.status(200).json({ success: true, message: 'Usuario removido com sucesso' });
    }

    return res.status(400).json({ error: 'Acao invalida' });

  } catch (err) {
    console.error('users handler error:', err.message);
    return res.status(500).json({ error: err.message });
  }
};
