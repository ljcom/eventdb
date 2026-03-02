import { config } from '../src/config.js';
import { pool } from '../src/db.js';
import { runOrdersProjection } from '../src/projections.js';

function parseArgs(argv) {
  const args = {
    chainId: '',
    namespaceId: config.defaultNamespaceId,
    uptoSequence: undefined
  };

  for (let i = 2; i < argv.length; i += 1) {
    const current = argv[i];
    if (current === '--chain-id') {
      args.chainId = argv[i + 1] || '';
      i += 1;
      continue;
    }
    if (current === '--namespace-id') {
      args.namespaceId = argv[i + 1] || config.defaultNamespaceId;
      i += 1;
      continue;
    }
    if (current === '--upto-sequence') {
      args.uptoSequence = argv[i + 1];
      i += 1;
    }
  }

  return args;
}

async function main() {
  const { chainId, namespaceId, uptoSequence } = parseArgs(process.argv);
  if (!chainId) {
    throw new Error('Missing required --chain-id');
  }

  const result = await runOrdersProjection({
    namespaceId,
    chainId,
    uptoSequence
  });

  // eslint-disable-next-line no-console
  console.log(JSON.stringify(result, null, 2));

  if (result.status !== 'PASS') {
    process.exitCode = 1;
  }
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
