// supabase/functions/trigger-etl-mef/index.ts
//
// Edge Function de Supabase que actúa como punto de entrada HTTP para
// disparar el ETL de presupuesto MEF. Puede invocarse:
//   - Desde un cron job de Supabase (pg_cron o Dashboard → Cron Jobs)
//   - Manualmente vía curl / Postman para backfills
//   - Desde GitHub Actions como paso de post-deploy
//
// Variables de entorno requeridas (configurar en Dashboard → Edge Functions):
//   ETL_RUNNER_URL       URL del microservicio Python (ej. Railway / Render / Cloud Run)
//   ETL_SHARED_SECRET    Token secreto para autenticar la llamada al microservicio
//   SUPABASE_URL         Auto-inyectada por Supabase
//   SUPABASE_SERVICE_ROLE_KEY  Auto-inyectada por Supabase

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// ---------------------------------------------------------------------------
// Tipos
// ---------------------------------------------------------------------------

interface ETLPayload {
  anios?: number[];      // años a procesar; si omite → año actual
  fuente?: "mef" | "osce";  // para futuro soporte multi-fuente
}

interface ETLResponse {
  ok: boolean;
  mensaje: string;
  anios_procesados?: number[];
  error?: string;
}

// ---------------------------------------------------------------------------
// Handler principal
// ---------------------------------------------------------------------------

serve(async (req: Request): Promise<Response> => {
  // Solo aceptar POST
  if (req.method !== "POST") {
    return respuesta({ ok: false, mensaje: "Método no permitido. Usar POST." }, 405);
  }

  // Verificar Authorization header (Bearer token compartido)
  const authHeader = req.headers.get("Authorization") ?? "";
  const secretEsperado = `Bearer ${Deno.env.get("ETL_SHARED_SECRET")}`;

  if (authHeader !== secretEsperado) {
    console.error("Intento de acceso no autorizado a trigger-etl-mef");
    return respuesta({ ok: false, mensaje: "No autorizado." }, 401);
  }

  // Parsear payload (opcional — defaults razonables si no viene body)
  let payload: ETLPayload = {};
  try {
    const texto = await req.text();
    if (texto) {
      payload = JSON.parse(texto);
    }
  } catch {
    return respuesta({ ok: false, mensaje: "Body JSON inválido." }, 400);
  }

  const anioActual = new Date().getFullYear();
  const anios = payload.anios ?? [anioActual];
  const fuente = payload.fuente ?? "mef";

  console.log(`[trigger-etl-mef] Disparando ETL para fuente=${fuente}, años=${anios}`);

  // Registrar inicio de ejecución en tabla de auditoría
  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!,
  );

  await registrarInicio(supabase, fuente, anios);

  // Llamar al microservicio Python ETL
  const urlRunner = Deno.env.get("ETL_RUNNER_URL");

  if (!urlRunner) {
    // Modo fallback: ejecutar transformación SQL directamente en Supabase
    // (útil si el microservicio Python no está desplegado aún)
    console.warn("[trigger-etl-mef] ETL_RUNNER_URL no configurada — modo SQL directo");
    return await ejecutarModeSQLDirecto(supabase, anios);
  }

  // Modo normal: delegar al microservicio Python
  return await llamarMicroservicioPython(urlRunner, fuente, anios, payload);
});

// ---------------------------------------------------------------------------
// Llamada al microservicio Python
// ---------------------------------------------------------------------------

async function llamarMicroservicioPython(
  urlRunner: string,
  fuente: string,
  anios: number[],
  payload: ETLPayload,
): Promise<Response> {
  try {
    const respuestaPython = await fetch(`${urlRunner}/run`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${Deno.env.get("ETL_SHARED_SECRET")}`,
      },
      body: JSON.stringify({ fuente, anios }),
    });

    if (!respuestaPython.ok) {
      const textoError = await respuestaPython.text();
      console.error(`[trigger-etl-mef] Error del microservicio: ${textoError}`);
      return respuesta(
        { ok: false, mensaje: "Error en microservicio ETL.", error: textoError },
        502,
      );
    }

    const resultado = await respuestaPython.json();
    console.log("[trigger-etl-mef] ETL completado:", resultado);

    return respuesta(
      { ok: true, mensaje: "ETL disparado exitosamente.", anios_procesados: anios },
      200,
    );
  } catch (err) {
    console.error("[trigger-etl-mef] Error de red al llamar microservicio:", err);
    return respuesta(
      { ok: false, mensaje: "No se pudo conectar con el microservicio ETL.", error: String(err) },
      503,
    );
  }
}

// ---------------------------------------------------------------------------
// Modo SQL directo (sin microservicio Python levantado)
// Útil para desarrollo o si el ETL Python ya corrió y solo necesitas
// re-ejecutar la transformación de raw → analítica.
// ---------------------------------------------------------------------------

async function ejecutarModeSQLDirecto(
  supabase: ReturnType<typeof createClient>,
  anios: number[],
): Promise<Response> {
  const errores: string[] = [];

  for (const anio of anios) {
    try {
      console.log(`[trigger-etl-mef] Ejecutando RPC transformar_presupuesto_mef(${anio})`);
      const { error } = await supabase.rpc("transformar_presupuesto_mef", { p_anio: anio });

      if (error) {
        console.error(`[trigger-etl-mef] Error RPC año ${anio}:`, error);
        errores.push(`${anio}: ${error.message}`);
      } else {
        console.log(`[trigger-etl-mef] Transformación ${anio} completada.`);
      }
    } catch (err) {
      errores.push(`${anio}: ${String(err)}`);
    }
  }

  if (errores.length > 0) {
    return respuesta(
      {
        ok: false,
        mensaje: "Transformación completada con errores.",
        error: errores.join(" | "),
        anios_procesados: anios,
      },
      207,
    );
  }

  return respuesta(
    { ok: true, mensaje: "Transformación SQL directa completada.", anios_procesados: anios },
    200,
  );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function registrarInicio(
  supabase: ReturnType<typeof createClient>,
  fuente: string,
  anios: number[],
): Promise<void> {
  try {
    await supabase.table("etl_ejecuciones").insert({
      fuente: `${fuente}_trigger`,
      anio: anios[0],
      estado: "iniciado",
      mensaje_error: null,
      ejecutado_en: new Date().toISOString(),
    });
  } catch (err) {
    // No bloquear el ETL si falla el registro de auditoría
    console.warn("[trigger-etl-mef] No se pudo registrar inicio:", err);
  }
}

function respuesta(body: ETLResponse, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
