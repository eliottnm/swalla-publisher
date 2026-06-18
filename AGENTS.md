# AGENTS.md - swalla-publisher

## Propósito

Lee filtros activos de MongoDB y publica tareas expandidas en Google Cloud Pub/Sub para que los workers las procesen.

## Arquitectura

- `src/index.js`: lee filtros activos, aplica delays por usuario, expande variantes y modos de entrega, y publica lotes de tareas en el topic `swalla-tasks`.
- `Dockerfile`: imagen del publisher.
- `cloudbuild-publisher.yaml`: Cloud Build para la imagen.

## Deploy

Cualquier commit a `main` despliega automáticamente via GitHub Actions (`.github/workflows/deploy.yml`):

1. Buildea y sube la imagen `fynder/publisher` a Artifact Registry.
2. Actualiza el Cloud Run Job `swalla-publisher`.

## Variables de entorno

Copiar `.env.example` a `.env.local` y rellenar:

- `MONGODB_URI`: URI de MongoDB.
- `MONGODB_DB`: nombre de la base de datos.
- `GOOGLE_CLOUD_PROJECT`: ID del proyecto GCP.
- `PUBSUB_TOPIC`: topic de Pub/Sub (`swalla-tasks`).
- `PUBLISHER_INTERVAL_MS`: intervalo entre ejecuciones (default 60000).
- `PUBLISHER_MAX_PRODUCTS_PER_RUN`: máximo de filtros a procesar por run (default 200).
- `PUBLISHER_BATCH_SIZE`: tareas por mensaje de Pub/Sub (default 10).

## Ejecución local

```bash
npm install
npm run check
npm start       # loop continuo
npm run once    # una sola ejecución
```

## Convenciones

- Código ES modules.
- Sin comentarios innecesarios.
- Logs en español para consistencia con el resto del proyecto.
- No commitear `.env.local` ni credenciales.

## Comandos GCP útiles

```bash
# Ejecutar el publisher manualmente
gcloud run jobs execute swalla-publisher --region=europe-southwest1 --project=project-30a4caa9-f885-4066-a8c --wait

# Ver logs del publisher
gcloud logging read 'resource.type=cloud_run_job resource.labels.job_name=swalla-publisher'
```
