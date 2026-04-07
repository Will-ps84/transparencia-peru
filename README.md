# ETL MEF — Transparencia Perú

Módulo de ingesta de datos de presupuesto y ejecución del gasto del **Ministerio de Economía y Finanzas (MEF)** hacia Supabase.

## Arquitectura del pipeline

```
Portal MEF (CSV/ZIP)
        │
        ▼
[etl_mef.py] ← descarga + parseo + limpieza
        │
        ▼ UPSERT por lotes
[Supabase: raw_presupuesto_mef]
        │
        ▼ RPC SQL
[Supabase: presupuesto] ← tabla analítica
        │
        ▼
[Edge Function: trigger-etl-mef] ← disparador HTTP/cron
```

## Estructura de archivos

```
etl_mef/
├── etl_mef.py                          # ETL principal Python
├── requirements.txt
├── .env.example
└── supabase/
    └── functions/
        └── trigger-etl-mef/
            └── index.ts                # Edge Function (disparador)
```

## Configuración inicial

```bash
# 1. Clonar y entrar al directorio
cd etl_mef

# 2. Crear entorno virtual
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
.venv\Scripts\activate     # Windows

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Configurar variables de entorno
cp .env.example .env
# Editar .env con tus valores reales
```

## Ejecución local

```bash
# Año actual
python etl_mef.py

# Backfill histórico (editar main() o usar Python interactivo)
python -c "from etl_mef import main; main(anios=[2020, 2021, 2022, 2023, 2024])"
```

## Deploy de la Edge Function

```bash
# Requiere Supabase CLI instalado
supabase functions deploy trigger-etl-mef --no-verify-jwt

# Configurar variables de entorno en la Edge Function
supabase secrets set ETL_SHARED_SECRET=<tu-token>
supabase secrets set ETL_RUNNER_URL=<url-microservicio-python>
```

## Configurar cron job en Supabase

En el Dashboard → Edge Functions → trigger-etl-mef → Schedule:

```
# Ejecutar el 1° de cada mes a las 3am (hora Perú = UTC-5)
0 8 1 * *
```

O vía `pg_cron` directamente en SQL:
```sql
-- Requiere extensión pg_cron habilitada
SELECT cron.schedule(
  'etl-mef-mensual',
  '0 8 1 * *',
  $$
  SELECT net.http_post(
    url := 'https://<proyecto>.supabase.co/functions/v1/trigger-etl-mef',
    headers := '{"Authorization": "Bearer <ETL_SHARED_SECRET>", "Content-Type": "application/json"}'::jsonb,
    body := '{}'::jsonb
  );
  $$
);
```

## Tabla de auditoría

El ETL registra cada ejecución en `etl_ejecuciones`:

| columna | tipo | descripción |
|---|---|---|
| fuente | text | `mef_presupuesto` |
| anio | int | Año procesado |
| filas_procesadas | int | Total de filas en el CSV |
| filas_exitosas | int | Filas insertadas/actualizadas OK |
| filas_con_error | int | Filas fallidas |
| estado | text | `exitoso` / `error` / `iniciado` |
| mensaje_error | text | Detalle del error si aplica |
| ejecutado_en | timestamptz | Timestamp UTC de ejecución |

## Notas sobre los datos del MEF

- **Encodings**: el MEF usa `latin-1` en años anteriores y `utf-8` en los más recientes. El parser prueba automáticamente distintas configuraciones.
- **Separadores**: puede ser `|` o `,` según el año. El parser maneja ambos.
- **Resource IDs**: los IDs del Portal de Datos Abiertos cambian anualmente. Actualizar `MEF_RESOURCE_IDS` en `etl_mef.py` cuando el MEF publique nuevos datasets.
- **Volumen esperado**: ~500K–2M filas por año dependiendo del nivel de granularidad.
