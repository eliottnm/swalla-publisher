# Swalla Publisher

Lee filtros activos desde MongoDB y publica cada búsqueda expandida como un mensaje en Google Cloud Pub/Sub para que los workers la procesen.

## Configuración

Copia `.env.example` a `.env.local` y rellena los valores.

## Ejecución

```bash
npm install
npm start       # loop continuo
npm run once    # una sola ejecución
```

## Despliegue

Se puede ejecutar como Cloud Run Job, Cloud Scheduler + Cloud Run, o como proceso persistente en una VM.
