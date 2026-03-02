import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import pg from 'pg';
import { pool } from '../src/db.js';
import { config } from '../src/config.js';

const { Client } = pg;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const schemaPath = path.resolve(__dirname, '../../../05-mvp/schema/postgres.sql');
  const sql = await fs.readFile(schemaPath, 'utf8');

  try {
    await pool.query(sql);
    // eslint-disable-next-line no-console
    console.log(`Schema applied: ${schemaPath}`);
    return;
  } catch (error) {
    if (error?.code !== '3D000') {
      throw error;
    }
  }

  const dbUrl = new URL(config.databaseUrl);
  const targetDbName = dbUrl.pathname.replace(/^\//, '');
  if (!targetDbName) {
    throw new Error('DATABASE_URL must include database name');
  }

  dbUrl.pathname = '/postgres';
  const adminClient = new Client({
    connectionString: dbUrl.toString(),
    ssl: config.dbSsl ? { rejectUnauthorized: false } : false
  });

  try {
    await adminClient.connect();
    const safeDbName = targetDbName.replace(/"/g, '""');
    await adminClient.query(`create database "${safeDbName}"`);
    // eslint-disable-next-line no-console
    console.log(`Database created: ${targetDbName}`);
  } catch (error) {
    if (error?.code !== '42P04') {
      throw error;
    }
  } finally {
    await adminClient.end();
  }

  await pool.query(sql);
  // eslint-disable-next-line no-console
  console.log(`Schema applied: ${schemaPath}`);
}

main()
  .catch((error) => {
    // eslint-disable-next-line no-console
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
