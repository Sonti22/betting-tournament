import { createApp } from './bootstrap';
import { loadConfig } from './config';

async function main() {
  const config = loadConfig();
  const app = await createApp(config);
  try { await app.listen(config.port, '0.0.0.0'); }
  catch (error) { await app.close(); throw error; }
}
void main().catch(() => {
  console.error('Startup failed; check configuration, migrations and service availability');
  process.exitCode = 1;
});
