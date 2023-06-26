/**
This will probably move to its own repo at some point but easier to keep it here for now
 */
import * as os from 'os';
import * as fs from 'fs';
import {
  Keypair,
  Commitment,
  Connection,
  PublicKey,
  Transaction,
  ComputeBudgetProgram
} from '@solana/web3.js';
import {
  MangoClient,
  makeCachePerpMarketsInstruction,
  makeCachePricesInstruction,
  makeCacheRootBankInstruction,
  makeUpdateFundingInstruction,
  makeUpdateRootBankInstruction,
  getMultipleAccounts,
  zeroKey,
  Cluster, 
  Config,
  PerpEventQueueLayout,
  MangoGroup, PerpMarket, promiseUndef,
  PerpEventQueue,
  sleep,
  makeConsumeEventsInstruction
} from "@blockworks-foundation/mango-client";
import BN from 'bn.js';

let lastRootBankCacheUpdate = 0;
const groupName = process.env.GROUP || 'localnet';
const updateCacheInterval = parseInt(
  process.env.UPDATE_CACHE_INTERVAL || '2000',
);
const updateRootBankCacheInterval = parseInt(
  process.env.UPDATE_ROOT_BANK_CACHE_INTERVAL || '3000',
);
const processKeeperInterval = parseInt(
  process.env.PROCESS_KEEPER_INTERVAL || '3000',
);
const consumeEventsInterval = parseInt(
  process.env.CONSUME_EVENTS_INTERVAL || '100',
);
const maxUniqueAccounts = parseInt(process.env.MAX_UNIQUE_ACCOUNTS || '24');
const consumeEventsLimit = new BN(process.env.CONSUME_EVENTS_LIMIT || '20');
const consumeEvents = process.env.CONSUME_EVENTS ? process.env.CONSUME_EVENTS === 'true' : true;
const skipPreflight = process.env.SKIP_PREFLIGHT ? process.env.SKIP_PREFLIGHT === 'true' : true;
const cluster = (process.env.CLUSTER || 'localnet') as Cluster;
import configFile from './ids.json';
const config = new Config(configFile);
const groupIds = config.getGroup(cluster, groupName);

if (!groupIds) {
  throw new Error(`Group ${groupName} not found`);
}
const mangoProgramId = groupIds.mangoProgramId;
const mangoGroupKey = groupIds.publicKey;
const payer = Keypair.fromSecretKey(
  Uint8Array.from(
    JSON.parse(
      process.env.KEYPAIR ||
        fs.readFileSync('authority.json', 'utf-8'),
    ),
  ),
);
const connection = new Connection(
  process.env.ENDPOINT_URL || config.cluster_urls[cluster],
  'processed' as Commitment,
);
const client = new MangoClient(connection, mangoProgramId, {
  timeout: 10000,
  prioritizationFee: 10000, // number of micro lamports
});

async function main() {
  if (!groupIds) {
    throw new Error(`Group ${groupName} not found`);
  }
  const mangoGroup = await client.getMangoGroup(mangoGroupKey);
  const perpMarkets = await Promise.all(
    groupIds.perpMarkets.map((m) => {
      return mangoGroup.loadPerpMarket(
        connection,
        m.marketIndex,
        m.baseDecimals,
        m.quoteDecimals,
      );
    }),
  );

  const do_log_str = process.env.LOG || "false";
  const do_log = do_log_str === "true";
  let logId = 0
  if (do_log) {
      console.log("LOGGING ON");
      logId = connection.onLogs(mangoProgramId, (log, ctx) => {
          if (log.err != null) {
              console.log("mango error : ", log.err)
          }
          else {
              for (const l of log.logs) {
                  console.log("mango log : " + l)
              }
          }
      });
  }
  const beginSlot = await connection.getSlot();

  try {
      processUpdateCache(mangoGroup);
      processKeeperTransactions(mangoGroup, perpMarkets);

      if (consumeEvents) {
          processConsumeEvents(mangoGroup, perpMarkets);
      }
  } finally {
        if (logId) {
            // to log mango logs
            await sleep(5000)
            const endSlot = await connection.getSlot();
            const blockSlots = await connection.getBlocks(beginSlot, endSlot);
            console.log("\n\n===============================================")
            for (let blockSlot of blockSlots) {
                const block = await connection.getBlock(blockSlot);
                for (let i = 0; i < block.transactions.length; ++i) {
                    if (block.transactions[i].meta.logMessages) {
                        for (const msg of block.transactions[i].meta.logMessages) {
                            console.log('solana_message : ' + msg);
                        }
                    }
                }
            }

            connection.removeOnLogsListener(logId);
        }
    }
}
console.time('processUpdateCache');
console.time('processKeeperTransactions');
console.time('processConsumeEvents');

async function processUpdateCache(mangoGroup: MangoGroup) {
  try {
    const batchSize = 8;
    const promises: Promise<string>[] = [];
    const rootBanks = mangoGroup.tokens
      .map((t) => t.rootBank)
      .filter((t) => !t.equals(zeroKey));
    const oracles = mangoGroup.oracles.filter((o) => !o.equals(zeroKey));
    const perpMarkets = mangoGroup.perpMarkets
      .filter((pm) => !pm.isEmpty())
      .map((pm) => pm.perpMarket);
    const nowTs = Date.now();
    let shouldUpdateRootBankCache = false;
    if (nowTs - lastRootBankCacheUpdate > updateRootBankCacheInterval) {
      shouldUpdateRootBankCache = true;
      lastRootBankCacheUpdate = nowTs;
    }
    for (let i = 0; i < Math.ceil(rootBanks.length / batchSize); i++) {
      const startIndex = i * batchSize;
      const endIndex = Math.min(i * batchSize + batchSize, rootBanks.length);
      const cacheTransaction = new Transaction();

      cacheTransaction.add(
        makeCachePricesInstruction(
          mangoProgramId,
          mangoGroup.publicKey,
          mangoGroup.mangoCache,
          oracles.slice(startIndex, endIndex),
        ),
      );

      if (shouldUpdateRootBankCache) {
        cacheTransaction.add(
          makeCacheRootBankInstruction(
            mangoProgramId,
            mangoGroup.publicKey,
            mangoGroup.mangoCache,
            rootBanks.slice(startIndex, endIndex),
          ),
        );
      }

      cacheTransaction.add(
        makeCachePerpMarketsInstruction(
          mangoProgramId,
          mangoGroup.publicKey,
          mangoGroup.mangoCache,
          perpMarkets.slice(startIndex, endIndex),
        ),
      );
      if (cacheTransaction.instructions.length > 0) {
        promises.push(connection.sendTransaction(cacheTransaction, [payer], {skipPreflight}));
      }
    }

    Promise.all(promises).catch((err) => {
      console.error('Error updating cache', err);
    });
  } catch (err) {
    console.error('Error in processUpdateCache', err);
  } finally {
    console.timeLog('processUpdateCache');
    setTimeout(processUpdateCache, updateCacheInterval, mangoGroup);
  }
}

async function processConsumeEvents(
  mangoGroup: MangoGroup,
  perpMarkets: PerpMarket[],
) {
  let eventsConsumed = [];
  try {
    const eventQueuePks = perpMarkets.map((mkt) => mkt.eventQueue);
    const eventQueueAccts = await getMultipleAccounts(
      connection,
      eventQueuePks,
    );

    const perpMktAndEventQueue = eventQueueAccts.map(
      ({ publicKey, accountInfo }) => {
        const parsed = PerpEventQueueLayout.decode(accountInfo?.data);
        const eventQueue = new PerpEventQueue(parsed);
        const perpMarket = perpMarkets.find((mkt) =>
          mkt.eventQueue.equals(publicKey),
        );
        if (!perpMarket) {
          throw new Error('PerpMarket not found');
        }
        return { perpMarket, eventQueue };
      },
    );

    const promises: Promise<string | void>[] = perpMktAndEventQueue.map(
      ({ perpMarket, eventQueue }) => {
        const events = eventQueue.getUnconsumedEvents();
        if (events.length === 0) {
          // console.log('No events to consume', perpMarket.publicKey.toString(), perpMarket.eventQueue.toString());
          return promiseUndef();
        }

        const accounts: Set<string> = new Set();
        for (const event of events) {
          if (event.fill) {
            accounts.add(event.fill.maker.toBase58());
            accounts.add(event.fill.taker.toBase58());
          } else if (event.out) {
            accounts.add(event.out.owner.toBase58());
          }

          // Limit unique accounts to first 20 or 21
          if (accounts.size >= maxUniqueAccounts) {
            break;
          }
        }

        const consumeEventsInstruction = makeConsumeEventsInstruction(
          mangoProgramId,
          mangoGroup.publicKey,
          mangoGroup.mangoCache,
          perpMarket.publicKey,
          perpMarket.eventQueue,
          Array.from(accounts)
          .map((s) => new PublicKey(s))
          .sort(),
          consumeEventsLimit,
        );

        const transaction = new Transaction();
        transaction.add(consumeEventsInstruction);
        eventsConsumed.push(perpMarket.eventQueue.toString());

        return connection.sendTransaction(transaction, [payer], {skipPreflight})
          .catch((err) => {
            console.error('Error consuming events', err);
          });
      },
    );

    Promise.all(promises).catch((err) => {
      console.error('Error consuming events', err);
    });
  } catch (err) {
    console.error('Error in processConsumeEvents', err);
  } finally {
    console.timeLog('processConsumeEvents', eventsConsumed);
    setTimeout(
      processConsumeEvents,
      consumeEventsInterval,
      mangoGroup,
      perpMarkets,
    );
  }
}

async function processKeeperTransactions(
  mangoGroup: MangoGroup,
  perpMarkets: PerpMarket[],
) {
  try {
    if (!groupIds) {
      throw new Error(`Group ${groupName} not found`);
    }
    const batchSize = 8;
    const promises: Promise<string>[] = [];

    const filteredPerpMarkets = perpMarkets.filter(
      (pm) => !pm.publicKey.equals(zeroKey),
    );

    for (let i = 0; i < groupIds.tokens.length / batchSize; i++) {
      const startIndex = i * batchSize;
      const endIndex = i * batchSize + batchSize;

      const updateRootBankTransaction = new Transaction();
      groupIds.tokens.slice(startIndex, endIndex).forEach((token) => {
        updateRootBankTransaction.add(
          makeUpdateRootBankInstruction(
            mangoProgramId,
            mangoGroup.publicKey,
            mangoGroup.mangoCache,
            token.rootKey,
            token.nodeKeys,
          ),
        );
      });

      const updateFundingTransaction = new Transaction();
      filteredPerpMarkets.slice(startIndex, endIndex).forEach((market) => {
        if (market) {
          updateFundingTransaction.add(
            makeUpdateFundingInstruction(
              mangoProgramId,
              mangoGroup.publicKey,
              mangoGroup.mangoCache,
              market.publicKey,
              market.bids,
              market.asks,
            ),
          );
        }
      });

      if (updateRootBankTransaction.instructions.length > 0) {
        promises.push(
          connection.sendTransaction(updateRootBankTransaction, [payer], {skipPreflight}),
        );
      }
      if (updateFundingTransaction.instructions.length > 0) {
        promises.push(
          connection.sendTransaction(updateFundingTransaction, [payer], {skipPreflight}),
        );
      }
    }

    Promise.all(promises).catch((err) => {
      console.error('Error processing keeper instructions', err);
    });
  } catch (err) {
    console.error('Error in processKeeperTransactions', err);
  } finally {
    console.timeLog('processKeeperTransactions');
    setTimeout(
      processKeeperTransactions,
      processKeeperInterval,
      mangoGroup,
      perpMarkets,
    );
  }
}

process.on('unhandledRejection', (err: any, p: any) => {
  console.error(`Unhandled rejection: ${err} promise: ${p})`);
});

main();
