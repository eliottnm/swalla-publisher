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

const { ObjectId } = await import('mongodb');
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

// Mock Pub/Sub for local testing without GCP credentials
let topic;
if (process.env.PUBLISHER_MOCK_PUBSUB === 'true') {
  const publishedBatches = [];
  topic = {
    publishMessage: async (msg) => {
      const batch = JSON.parse(msg.data.toString());
      publishedBatches.push(batch);
      const id = `mock-${Date.now()}-${publishedBatches.length}`;
      console.log(`[${timestamp()}] MOCK publicado lote con ${batch.length} tareas - ${id}`);
      return id;
    },
    getPublishedBatches: () => publishedBatches,
  };
} else {
  const { PubSub } = await import('@google-cloud/pubsub');
  const pubsub = new PubSub({ projectId: config.projectId });
  topic = pubsub.topic(config.topicName);
}

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

  // Persist lastRunAt for filters whose tasks were actually published
  if (published > 0) {
    await markFiltersAsRun(filters.map((f) => f._id.toString()));
  }

  console.log(`[${timestamp()}] Publicación terminada. ${published} lotes publicados, ${failed} fallidos.`);
}

async function markFiltersAsRun(filterIds) {
  if (!filterIds || filterIds.length === 0) return;
  const now = new Date();
  try {
    const objectIds = filterIds
      .map((id) => {
        try {
          return new ObjectId(id);
        } catch {
          return null;
        }
      })
      .filter(Boolean);

    if (objectIds.length === 0) return;

    const result = await db.collection('searchFilters').updateMany(
      { _id: { $in: objectIds } },
      { $set: { lastRunAt: now } }
    );
    console.log(`[${timestamp()}] Actualizado lastRunAt para ${result.modifiedCount} filtros.`);
  } catch (error) {
    console.error(`[${timestamp()}] Error actualizando lastRunAt: ${error.message}`);
  }
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
      {
        $addFields: {
          lastRunMs: { $ifNull: ['$lastRunAt', { $dateFromString: { dateString: '1970-01-01T00:00:00Z' } }] },
        },
      },
      { $sort: { lastRunMs: 1, updatedAt: -1 } },
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
          lastRunMs: 0,
        },
      },
    ])
    .toArray();

  const eligibleFilters = [];
  for (const filter of filters) {
    const userId = filter.userId;
    const delayMinutes = filter.delayMinutes ?? 1;
    const lastRunAt = filter.lastRunAt;

    if (lastRunAt) {
      const msSinceLastRun = now.getTime() - new Date(lastRunAt).getTime();
      const requiredDelayMs = delayMinutes * 60 * 1000;

      if (msSinceLastRun < requiredDelayMs) {
        console.log(`[${timestamp()}] Skipping filter ${filter._id}: delay not met (${Math.round(msSinceLastRun / 1000)}s < ${delayMinutes}min)`);
        continue;
      }
    }

    eligibleFilters.push(filter);
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
