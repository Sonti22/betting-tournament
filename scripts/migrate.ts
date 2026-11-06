import { loadConfig } from '../src/config';
import { Database } from '../src/database';

async function main() {
  const db = new Database(loadConfig());
  try { await db.onModuleInit(); console.log('Migrations applied'); }
  finally { await db.onApplicationShutdown(); }
}
void main().catch(() => { console.error('Migration failed; check configuration and database availability'); process.exitCode = 1; });
