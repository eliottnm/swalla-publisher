import { PubSub } from '@google-cloud/pubsub';
import { MongoClient } from 'mongodb';
import fs from 'node:fs';
import process from 'node:process';

const DEFAULT_INTERVAL_MS = 60_000;

const args = new Set(process.argv.slice(2));
const runOnce = args.has('--once');

loadDotEnv();

const config = {
  mongoUri: process.env.MONGODB_URI,
  mongoDb: process.env.MONGODB_DB ?? 'swalla_product',
  intervalMs: Number(process.env.PUBLISHER_INTERVAL_MS ?? DEFAULT_INTERVAL_MS),
  maxProductsPerRun: Number(process.env.PUBLISHER_MAX_PRODUCTS_PER_RUN ?? 200),
  projectId: process.env.GOOGLE_CLOUD_PROJECT,
  topicName: process.env.PUBSUB_TOPIC ?? 'swalla-tasks',
};

if (!config.mongoUri) throw new Error('MONGODB_URI is required');
if (!config.projectId) throw new Error('GOOGLE_CLOUD_PROJECT is required');

const mongo = new MongoClient(config.mongoUri, {
  serverSelectionTimeoutMS: 30_000,
  connectTimeoutMS: 10_000,
  socketTimeoutMS: 45_000,
  maxPoolSize: 10,
  minPoolSize: 1,
  retryWrites: true,
  w: 'majority',
});
const db = mongo.db(config.mongoDb);
const pubsub = new PubSub({ projectId: config.projectId });
const topic = pubsub.topic(config.topicName);

const lastRunByUser = new Map();

try {
  await mongo.connect();
  await db.command({ ping: 1 });
  console.log(`Swalla publisher conectado a MongoDB (${config.mongoDb}).`);

  do {
    const startedAt = Date.now();
    await publishPendingTasks();
    if (runOnce) break;
    const elapsed = Date.now() - startedAt;
    await sleep(Math.max(1_000, config.intervalMs - elapsed));
  } while (true);
} finally {
  await mongo.close().catch(() => {});
}

async function publishPendingTasks() {
  const filters = await getActiveFilters(config.maxProductsPerRun);
  const tasks = expandFilters(filters);

  if (tasks.length === 0) {
    console.log(`[${timestamp()}] Sin tareas pendientes para publicar.`);
    return;
  }

  const batchSize = Math.max(1, Number(process.env.PUBLISHER_BATCH_SIZE ?? 10));
  const batches = chunk(tasks, batchSize);

  console.log(`[${timestamp()}] ${filters.length} filtros activos, ${tasks.length} tareas, ${batches.length} lotes de ${batchSize} en ${config.topicName}.`);

  let published = 0;
  let failed = 0;

  await Promise.all(
    batches.map(async (batch, index) => {
      try {
        const messageId = await topic.publishMessage({
          data: Buffer.from(JSON.stringify(batch)),
          attributes: {
            batchIndex: String(index),
            batchSize: String(batch.length),
          },
        });
        console.log(`[${timestamp()}] Publicado lote ${index + 1}/${batches.length} (${batch.length} tareas) - ${messageId}`);
        published += 1;
      } catch (error) {
        console.error(`[${timestamp()}] Error publicando lote ${index + 1}: ${error.message}`);
        failed += 1;
      }
    })
  );

  console.log(`[${timestamp()}] Publicación terminada. ${published} lotes publicados, ${failed} fallidos.`);
}

function chunk(items, size) {
  const batches = [];
  for (let i = 0; i < items.length; i += size) {
    batches.push(items.slice(i, i + size));
  }
  return batches;
}

async function getActiveFilters(limit) {
  const now = new Date();

  const filters = await db
    .collection('searchFilters')
    .aggregate([
      { $match: { enabled: true } },
      {
        $addFields: {
          userObjectId: {
            $convert: {
              input: '$userId',
              to: 'objectId',
              onError: null,
              onNull: null,
            },
          },
        },
      },
      {
        $lookup: {
          from: 'users',
          localField: 'userObjectId',
          foreignField: '_id',
          as: 'user',
        },
      },
      { $unwind: '$user' },
      {
        $match: {
          'user.billing.accessActive': true,
          $or: [
            { 'user.billing.status': 'active' },
            {
              'user.billing.status': 'trialing',
              'user.billing.trialEndsAt': { $gt: now },
            },
          ],
        },
      },
      { $sort: { updatedAt: -1 } },
      { $limit: limit },
      {
        $addFields: {
          delayMinutes: { $ifNull: ['$user.billing.delayMinutes', 1] },
        },
      },
      {
        $project: {
          user: 0,
          userObjectId: 0,
        },
      },
    ])
    .toArray();

  const eligibleFilters = [];
  for (const filter of filters) {
    const userId = filter.userId;
    const delayMinutes = filter.delayMinutes ?? 1;
    const lastRun = lastRunByUser.get(userId);

    if (lastRun) {
      const msSinceLastRun = now.getTime() - lastRun.getTime();
      const requiredDelayMs = delayMinutes * 60 * 1000;

      if (msSinceLastRun < requiredDelayMs) {
        console.log(`[${timestamp()}] Skipping user ${userId}: delay not met (${Math.round(msSinceLastRun / 1000)}s < ${delayMinutes}min)`);
        continue;
      }
    }

    eligibleFilters.push(filter);
    lastRunByUser.set(userId, now);
  }

  return eligibleFilters;
}

function expandFilters(filters) {
  const tasks = [];
  for (const filter of filters) {
    const variants = [filter.keywords, ...(filter.variants ?? [])]
      .map((value) => String(value ?? '').trim())
      .filter(Boolean);
    const uniqueVariants = [...new Set(variants)];
    for (const keywords of uniqueVariants) {
      const mode = filter.dealMode ?? 'both';
      const baseTask = {
        filterId: filter._id.toString(),
        userId: filter.userId,
        keywords,
        minPrice: Number(filter.minPrice ?? 0),
        maxPrice: Number(filter.maxPrice ?? 0),
        latitude: filter.latitude,
        longitude: filter.longitude,
        radiusKm: filter.radiusKm,
        locationLabel: filter.locationLabel ?? '',
        productType: filter.productType,
        subcategoryId: filter.subcategoryId,
        name: filter.name ?? '',
        notes: filter.notes ?? '',
        delayMinutes: filter.delayMinutes ?? 1,
      };

      if (mode === 'both') {
        tasks.push({ ...baseTask, dealMode: 'online' });
        tasks.push({ ...baseTask, dealMode: 'in_person' });
      } else {
        tasks.push({ ...baseTask, dealMode: mode });
      }
    }
  }
  return shuffle(tasks);
}

function shuffle(items) {
  const copy = [...items];
  for (let index = copy.length - 1; index > 0; index -= 1) {
    const swap = Math.floor(Math.random() * (index + 1));
    [copy[index], copy[swap]] = [copy[swap], copy[index]];
  }
  return copy;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function timestamp() {
  return new Date().toLocaleString('es-ES');
}

function loadDotEnv() {
  for (const file of ['.env.local', '.env']) {
    try {
      const content = fs.readFileSync(file, 'utf8');
      for (const line of content.split(/\r?\n/)) {
        const match = line.match(/^\s*([A-Z0-9_]+)\s*=\s*(.*)\s*$/i);
        if (!match || process.env[match[1]] !== undefined) continue;
        process.env[match[1]] = match[2].replace(/^"|"$/g, '');
      }
    } catch {
      // Optional env file.
    }
  }
}
