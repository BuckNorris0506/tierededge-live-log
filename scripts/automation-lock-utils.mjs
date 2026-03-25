import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';

const LOCK_BASE_DIR = path.join(os.tmpdir(), 'tierededge-automation-locks');
const LOCK_MAX_AGE_SECONDS = 900;

function lockDir(lockName) {
  return path.join(LOCK_BASE_DIR, `${lockName}.lock`);
}

function lockMetaPath(lockName) {
  return path.join(lockDir(lockName), 'owner');
}

async function readLockMeta(lockName) {
  try {
    const raw = await fs.readFile(lockMetaPath(lockName), 'utf8');
    return raw.split('\n').filter(Boolean).reduce((acc, line) => {
      const [key, ...rest] = line.split('=');
      acc[key] = rest.join('=');
      return acc;
    }, {});
  } catch {
    return null;
  }
}

function lockAgeSeconds(startedAt) {
  const startedMs = Date.parse(String(startedAt || ''));
  if (!Number.isFinite(startedMs)) return null;
  return Math.floor((Date.now() - startedMs) / 1000);
}

async function processAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(Number(pid), 0);
    return true;
  } catch {
    return false;
  }
}

export async function clearNamedLock(lockName) {
  await fs.rm(lockDir(lockName), { recursive: true, force: true });
}

export async function acquireNamedLock(lockName, owner = 'unknown') {
  await fs.mkdir(LOCK_BASE_DIR, { recursive: true });
  const dir = lockDir(lockName);
  const metaPath = lockMetaPath(lockName);

  try {
    await fs.mkdir(dir);
  } catch (error) {
    if (error?.code !== 'EEXIST') throw error;

    const meta = await readLockMeta(lockName);
    const alive = await processAlive(meta?.pid);
    const age = lockAgeSeconds(meta?.started_at);
    const stale = (meta?.pid && !alive) || (age !== null && age > LOCK_MAX_AGE_SECONDS);
    if (!stale) {
      return { acquired: false, staleRecovered: false, existing: meta };
    }
    await clearNamedLock(lockName);
    await fs.mkdir(dir);
    await fs.writeFile(path.join(dir, 'stale_recovery'), `${new Date().toISOString()}\n`, 'utf8').catch(() => {});
    const recoveredMeta = meta || null;
    await fs.writeFile(metaPath, [
      `lock_name=${lockName}`,
      `owner=${owner}`,
      `pid=${process.pid}`,
      `started_at=${new Date().toISOString()}`,
      `cwd=${process.cwd()}`,
      'stale_recovered=true',
    ].join('\n') + '\n', 'utf8');
    return { acquired: true, staleRecovered: true, recovered: recoveredMeta };
  }

  await fs.writeFile(metaPath, [
    `lock_name=${lockName}`,
    `owner=${owner}`,
    `pid=${process.pid}`,
    `started_at=${new Date().toISOString()}`,
    `cwd=${process.cwd()}`,
    'stale_recovered=false',
  ].join('\n') + '\n', 'utf8');
  return { acquired: true, staleRecovered: false };
}

