"""
ETL principal para ingesta de datos de presupuesto y ejecución del MEF.
Fuente: Portal de Datos Abiertos del MEF (archivos CSV/ZIP descargables).
Destino: Supabase (tabla raw_presupuesto_mef → función RPC de transformación).

Flujo:
  1. Descarga CSV/ZIP desde URLs del MEF por año
  2. Descomprime y valida estructura del archivo
  3. Limpia y normaliza los campos
  4. Hace UPSERT en lotes hacia Supabase (tabla raw)
  5. Llama RPC de Supabase para transformar raw → analítica
"""

import os
import io
import zipfile
import logging
import time
from datetime import datetime
from typing import Optional

import httpx
import pandas as pd
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Configuración de logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl_mef")


# ---------------------------------------------------------------------------
# Constantes y configuración
# ---------------------------------------------------------------------------

# URLs base del Portal de Datos Abiertos del MEF.
# El dataset "Ejecución del Gasto" usa el patrón:
#   https://datosabiertos.mef.gob.pe/datastore/dump/<resource_id>?format=csv
# Los resource_id cambian por año; se mapean aquí manualmente hasta
# que el MEF publique un endpoint de descubrimiento estable.
MEF_RESOURCE_IDS: dict[int, str] = {
    2020: "b5-ejecucion-2020",   # ← reemplazar con IDs reales del portal
    2021: "b5-ejecucion-2021",
    2022: "b5-ejecucion-2022",
    2023: "b5-ejecucion-2023",
    2024: "b5-ejecucion-2024",
}

MEF_BASE_URL = "https://datosabiertos.mef.gob.pe/datastore/dump"

# Alternativa: URL directa de descarga ZIP por año (Consulta Amigable / SIAF)
# Usar si el portal de datos abiertos no responde.
MEF_ZIP_URL_TEMPLATE = (
    "https://apps5.mineco.gob.pe/transparencia/Navegador/Docs/"
    "EjecucionGasto_{year}.zip"
)

# Tamaño de lote para UPSERT (equilibrio entre memoria y latencia de red)
BATCH_SIZE = 500

# Reintentos para requests HTTP
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# Columnas esperadas en el CSV del MEF (nombres originales en español)
# Ajustar si el MEF cambia el esquema del archivo.
COLUMNAS_MEF_REQUERIDAS = [
    "ANO_EJE",
    "MES_EJE",
    "CODIGO_DEPARTAMENTO",
    "CODIGO_PROVINCIA",
    "CODIGO_DISTRITO",
    "UBIGEO",
    "SEC_EJEC",          # Código de unidad ejecutora (clave para join con entidades)
    "NOMBRE_EJECUTORA",
    "CODIGO_FUNCION",
    "NOMBRE_FUNCION",
    "CODIGO_PROGRAMA",
    "NOMBRE_PROGRAMA",
    "FUENTE_FINANCIAMIENTO",
    "NOMBRE_FUENTE",
    "PIA",
    "PIM",
    "CERTIFICACION",
    "COMPROMISO_ANUAL",
    "EJECUCION_ATENCION_COMPROMISO_MENSUAL",
    "EJECUCION_DEVENGADO",
    "EJECUCION_GIRADO",
]

# Mapeo de columnas MEF → nombres snake_case para la tabla raw de Supabase
MAPEO_COLUMNAS: dict[str, str] = {
    "ANO_EJE":                                    "ano_eje",
    "MES_EJE":                                    "mes_eje",
    "CODIGO_DEPARTAMENTO":                        "codigo_departamento",
    "CODIGO_PROVINCIA":                           "codigo_provincia",
    "CODIGO_DISTRITO":                            "codigo_distrito",
    "UBIGEO":                                     "ubigeo",
    "SEC_EJEC":                                   "sec_ejec",
    "NOMBRE_EJECUTORA":                           "nombre_ejecutora",
    "CODIGO_FUNCION":                             "codigo_funcion",
    "NOMBRE_FUNCION":                             "nombre_funcion",
    "CODIGO_PROGRAMA":                            "codigo_programa",
    "NOMBRE_PROGRAMA":                            "nombre_programa",
    "FUENTE_FINANCIAMIENTO":                      "fuente_financiamiento",
    "NOMBRE_FUENTE":                              "nombre_fuente",
    "PIA":                                        "pia",
    "PIM":                                        "pim",
    "CERTIFICACION":                              "certificacion",
    "COMPROMISO_ANUAL":                           "compromiso_anual",
    "EJECUCION_ATENCION_COMPROMISO_MENSUAL":      "ejecucion_acm",
    "EJECUCION_DEVENGADO":                        "ejecucion_devengado",
    "EJECUCION_GIRADO":                           "ejecucion_girado",
}


# ---------------------------------------------------------------------------
# Cliente Supabase (singleton)
# ---------------------------------------------------------------------------

def obtener_cliente_supabase() -> Client:
    """Crea y retorna el cliente de Supabase usando variables de entorno."""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")  # service_role para bypasear RLS en ETL

    if not url or not key:
        raise EnvironmentError(
            "Faltan variables de entorno: SUPABASE_URL y SUPABASE_SERVICE_ROLE_KEY"
        )

    return create_client(url, key)


# ---------------------------------------------------------------------------
# Descarga de archivos
# ---------------------------------------------------------------------------

def descargar_csv_mef(anio: int, timeout_segundos: int = 120) -> bytes:
    """
    Descarga el archivo CSV/ZIP de ejecución del gasto del MEF para un año dado.
    Intenta primero el Portal de Datos Abiertos; si falla, usa la URL alternativa
    de descarga directa (ZIP del SIAF/Consulta Amigable).

    Args:
        anio: Año fiscal a descargar (ej. 2024)
        timeout_segundos: Timeout para la descarga (los archivos pueden ser grandes)

    Returns:
        Contenido binario del archivo descargado
    """
    # Intento 1: Portal de Datos Abiertos
    resource_id = MEF_RESOURCE_IDS.get(anio)
    if resource_id:
        url_primaria = f"{MEF_BASE_URL}/{resource_id}?format=csv"
        logger.info(f"[{anio}] Intentando descarga desde Portal de Datos Abiertos...")
        contenido = _descargar_con_reintentos(url_primaria, timeout_segundos)
        if contenido:
            return contenido

    # Intento 2: URL alternativa (ZIP directo)
    url_alternativa = MEF_ZIP_URL_TEMPLATE.format(year=anio)
    logger.warning(f"[{anio}] Fallback a URL alternativa: {url_alternativa}")
    contenido = _descargar_con_reintentos(url_alternativa, timeout_segundos)

    if not contenido:
        raise RuntimeError(
            f"No se pudo descargar el archivo del MEF para el año {anio}. "
            "Verificar URLs y disponibilidad del portal."
        )

    return contenido


def _descargar_con_reintentos(url: str, timeout: int) -> Optional[bytes]:
    """
    Realiza una descarga HTTP con reintentos ante fallos transitorios.

    Args:
        url: URL a descargar
        timeout: Timeout en segundos

    Returns:
        Contenido binario si fue exitoso, None si fallaron todos los intentos
    """
    for intento in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Descargando {url} (intento {intento}/{MAX_RETRIES})")
            with httpx.Client(timeout=timeout, follow_redirects=True) as cliente:
                respuesta = cliente.get(url)
                respuesta.raise_for_status()
                logger.info(
                    f"Descarga exitosa — {len(respuesta.content) / 1_048_576:.1f} MB"
                )
                return respuesta.content

        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP {e.response.status_code} en {url}: {e}")
        except httpx.RequestError as e:
            logger.warning(f"Error de red en {url}: {e}")

        if intento < MAX_RETRIES:
            logger.info(f"Reintentando en {RETRY_DELAY_SECONDS}s...")
            time.sleep(RETRY_DELAY_SECONDS)

    return None


# ---------------------------------------------------------------------------
# Parseo y limpieza del CSV
# ---------------------------------------------------------------------------

def parsear_csv_mef(contenido_binario: bytes, anio: int) -> pd.DataFrame:
    """
    Parsea el contenido binario (CSV o ZIP con CSV dentro) del MEF.
    Valida columnas requeridas, normaliza tipos y limpia valores nulos.

    Args:
        contenido_binario: Bytes descargados del MEF
        anio: Año fiscal (para logging y validación)

    Returns:
        DataFrame limpio y listo para UPSERT
    """
    # Detectar si es ZIP y extraer el CSV interno
    if contenido_binario[:2] == b"PK":
        logger.info(f"[{anio}] Descomprimiendo archivo ZIP...")
        contenido_binario = _extraer_csv_de_zip(contenido_binario)

    # Parsear CSV — el MEF usa encoding latin-1 y separador "|" o ","
    df = _parsear_csv_flexible(contenido_binario, anio)

    # Validar que estén las columnas mínimas
    columnas_faltantes = [c for c in COLUMNAS_MEF_REQUERIDAS if c not in df.columns]
    if columnas_faltantes:
        raise ValueError(
            f"[{anio}] Columnas faltantes en el CSV del MEF: {columnas_faltantes}. "
            f"Columnas disponibles: {list(df.columns)}"
        )

    # Renombrar a snake_case
    df = df.rename(columns=MAPEO_COLUMNAS)

    # Conservar solo las columnas mapeadas
    columnas_destino = list(MAPEO_COLUMNAS.values())
    df = df[[c for c in columnas_destino if c in df.columns]]

    # Limpiar y convertir tipos
    df = _limpiar_dataframe(df)

    logger.info(
        f"[{anio}] CSV parseado: {len(df):,} filas × {len(df.columns)} columnas"
    )
    return df


def _extraer_csv_de_zip(contenido_zip: bytes) -> bytes:
    """Extrae el primer archivo CSV encontrado dentro de un ZIP."""
    with zipfile.ZipFile(io.BytesIO(contenido_zip)) as zf:
        archivos_csv = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not archivos_csv:
            raise ValueError("El ZIP no contiene ningún archivo .csv")

        nombre_csv = archivos_csv[0]
        logger.info(f"Extrayendo '{nombre_csv}' del ZIP...")
        return zf.read(nombre_csv)


def _parsear_csv_flexible(contenido: bytes, anio: int) -> pd.DataFrame:
    """
    Intenta parsear el CSV probando distintas combinaciones de encoding
    y separador, ya que el MEF no es consistente entre años.
    """
    intentos = [
        {"encoding": "latin-1", "sep": "|"},
        {"encoding": "latin-1", "sep": ","},
        {"encoding": "utf-8",   "sep": "|"},
        {"encoding": "utf-8",   "sep": ","},
        {"encoding": "utf-8-sig", "sep": ","},
    ]

    for config in intentos:
        try:
            df = pd.read_csv(
                io.BytesIO(contenido),
                encoding=config["encoding"],
                sep=config["sep"],
                dtype=str,           # todo como string inicialmente; tipado explícito después
                low_memory=False,
            )
            # Normalizar nombres de columna: strip + upper
            df.columns = [c.strip().upper() for c in df.columns]

            if len(df.columns) > 5:  # sanidad mínima
                logger.info(
                    f"[{anio}] CSV parseado con encoding={config['encoding']}, "
                    f"sep='{config['sep']}'"
                )
                return df

        except Exception as e:
            logger.debug(f"Fallo con config {config}: {e}")

    raise ValueError(f"[{anio}] No se pudo parsear el CSV con ninguna configuración conocida.")


def _limpiar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpieza y conversión de tipos del DataFrame:
    - Strip de strings
    - Conversión de montos a float (el MEF usa comas como decimal en algunos años)
    - Conversión de año/mes a int
    - Reemplazo de NaN con None (compatible con JSON/Supabase)
    """
    columnas_numericas = [
        "pia", "pim", "certificacion", "compromiso_anual",
        "ejecucion_acm", "ejecucion_devengado", "ejecucion_girado",
    ]
    columnas_enteras = ["ano_eje", "mes_eje", "codigo_funcion", "codigo_programa"]
    columnas_texto = [
        "ubigeo", "sec_ejec", "nombre_ejecutora", "nombre_funcion",
        "nombre_programa", "fuente_financiamiento", "nombre_fuente",
        "codigo_departamento", "codigo_provincia", "codigo_distrito",
    ]

    # Limpiar strings
    for col in columnas_texto:
        if col in df.columns:
            df[col] = df[col].str.strip()

    # Convertir montos (reemplazar coma decimal → punto)
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = (
                df[col]
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
                .fillna(0.0)
            )

    # Convertir enteros
    for col in columnas_enteras:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # Reemplazar NaN → None para serialización JSON limpia
    df = df.where(pd.notnull(df), None)

    # Eliminar filas sin sec_ejec (registros inválidos/headers repetidos)
    df = df[df["sec_ejec"].notna() & (df["sec_ejec"] != "")]

    return df.reset_index(drop=True)


# ---------------------------------------------------------------------------
# UPSERT en lotes hacia Supabase
# ---------------------------------------------------------------------------

def upsert_en_lotes(
    supabase: Client,
    df: pd.DataFrame,
    tabla: str = "raw_presupuesto_mef",
) -> dict:
    """
    Inserta o actualiza el DataFrame en la tabla raw de Supabase en lotes.
    Usa upsert con la constraint única (sec_ejec, ano_eje, mes_eje,
    codigo_funcion, fuente_financiamiento) para evitar duplicados.

    Args:
        supabase: Cliente de Supabase autenticado
        df: DataFrame limpio a persistir
        tabla: Nombre de la tabla destino

    Returns:
        Resumen con filas procesadas, exitosas y errores
    """
    total_filas = len(df)
    filas_ok = 0
    errores = 0
    lotes_procesados = 0

    logger.info(
        f"Iniciando UPSERT de {total_filas:,} filas en '{tabla}' "
        f"(lotes de {BATCH_SIZE})"
    )

    # Convertir DataFrame a lista de dicts (JSON-serializable)
    registros = df.to_dict(orient="records")

    for inicio in range(0, total_filas, BATCH_SIZE):
        lote = registros[inicio : inicio + BATCH_SIZE]
        lote_num = inicio // BATCH_SIZE + 1

        try:
            respuesta = (
                supabase.table(tabla)
                .upsert(
                    lote,
                    on_conflict="sec_ejec,ano_eje,mes_eje,codigo_funcion,fuente_financiamiento",
                )
                .execute()
            )

            filas_ok += len(lote)
            lotes_procesados += 1

            if lotes_procesados % 10 == 0:
                progreso = (inicio + len(lote)) / total_filas * 100
                logger.info(
                    f"  Lote {lote_num}: {filas_ok:,}/{total_filas:,} filas "
                    f"({progreso:.1f}%)"
                )

        except Exception as e:
            errores += len(lote)
            logger.error(f"  Error en lote {lote_num} (filas {inicio}–{inicio+len(lote)}): {e}")

            # Continuar con el siguiente lote (no abortar toda la carga)
            continue

    logger.info(
        f"UPSERT completado — OK: {filas_ok:,} | Errores: {errores:,} | "
        f"Lotes: {lotes_procesados}"
    )

    return {"total": total_filas, "exitosas": filas_ok, "errores": errores}


# ---------------------------------------------------------------------------
# Llamada RPC de transformación (raw → analítica)
# ---------------------------------------------------------------------------

def ejecutar_transformacion_rpc(
    supabase: Client,
    anio: int,
    funcion_rpc: str = "transformar_presupuesto_mef",
) -> None:
    """
    Llama la función RPC de Supabase que transforma los datos raw en la
    tabla analítica `presupuesto` y recalcula los indicadores de alerta.

    La función SQL debe existir en Supabase como:
      CREATE FUNCTION transformar_presupuesto_mef(p_anio INT) ...

    Args:
        supabase: Cliente de Supabase autenticado
        anio: Año a transformar
        funcion_rpc: Nombre de la función RPC en Supabase
    """
    logger.info(f"[{anio}] Ejecutando RPC '{funcion_rpc}'...")
    try:
        supabase.rpc(funcion_rpc, {"p_anio": anio}).execute()
        logger.info(f"[{anio}] Transformación RPC completada exitosamente.")
    except Exception as e:
        logger.error(f"[{anio}] Error en RPC '{funcion_rpc}': {e}")
        raise


# ---------------------------------------------------------------------------
# Registro de ejecución en tabla de auditoría
# ---------------------------------------------------------------------------

def registrar_ejecucion(
    supabase: Client,
    anio: int,
    resultado: dict,
    error: Optional[str] = None,
) -> None:
    """
    Persiste un registro de auditoría de la ejecución del ETL en la tabla
    `etl_ejecuciones` para trazabilidad y monitoreo.

    Args:
        supabase: Cliente de Supabase
        anio: Año procesado
        resultado: Dict con métricas (total, exitosas, errores)
        error: Mensaje de error si el proceso falló
    """
    registro = {
        "fuente": "mef_presupuesto",
        "anio": anio,
        "filas_procesadas": resultado.get("total", 0),
        "filas_exitosas": resultado.get("exitosas", 0),
        "filas_con_error": resultado.get("errores", 0),
        "estado": "error" if error else "exitoso",
        "mensaje_error": error,
        "ejecutado_en": datetime.utcnow().isoformat(),
    }

    try:
        supabase.table("etl_ejecuciones").insert(registro).execute()
    except Exception as e:
        logger.warning(f"No se pudo registrar la ejecución en auditoría: {e}")


# ---------------------------------------------------------------------------
# Orquestador principal
# ---------------------------------------------------------------------------

def procesar_anio(supabase: Client, anio: int) -> None:
    """
    Orquesta el pipeline completo para un año fiscal:
      1. Descarga CSV/ZIP del MEF
      2. Parsea y limpia los datos
      3. UPSERT en tabla raw de Supabase
      4. Llama RPC de transformación → tabla analítica

    Args:
        supabase: Cliente de Supabase autenticado
        anio: Año fiscal a procesar
    """
    inicio = time.time()
    logger.info(f"{'='*60}")
    logger.info(f"INICIANDO PIPELINE MEF — AÑO {anio}")
    logger.info(f"{'='*60}")

    resultado = {"total": 0, "exitosas": 0, "errores": 0}
    error_mensaje = None

    try:
        # Paso 1: Descarga
        contenido = descargar_csv_mef(anio)

        # Paso 2: Parseo y limpieza
        df = parsear_csv_mef(contenido, anio)

        # Paso 3: UPSERT raw
        resultado = upsert_en_lotes(supabase, df)

        # Paso 4: Transformación analítica (solo si hubo datos)
        if resultado["exitosas"] > 0:
            ejecutar_transformacion_rpc(supabase, anio)

    except Exception as e:
        error_mensaje = str(e)
        logger.error(f"[{anio}] Pipeline fallido: {e}", exc_info=True)

    finally:
        # Siempre registrar en auditoría
        registrar_ejecucion(supabase, anio, resultado, error_mensaje)

        duracion = time.time() - inicio
        logger.info(
            f"[{anio}] Pipeline finalizado en {duracion:.1f}s — "
            f"Estado: {'OK' if not error_mensaje else 'ERROR'}"
        )


def main(anios: Optional[list[int]] = None) -> None:
    """
    Punto de entrada principal del ETL.
    Por defecto procesa el año actual; acepta lista de años para backfill.

    Args:
        anios: Lista de años a procesar. Si es None, usa el año actual.

    Uso:
        # Año actual (modo normal, cron)
        python etl_mef.py

        # Backfill histórico
        python etl_mef.py  # modificar anios en el bloque __main__
    """
    if anios is None:
        anios = [datetime.now().year]

    supabase = obtener_cliente_supabase()

    logger.info(f"ETL MEF — Procesando años: {anios}")

    for anio in anios:
        try:
            procesar_anio(supabase, anio)
        except Exception as e:
            # Error no manejado: loguear y continuar con el siguiente año
            logger.critical(f"Error crítico procesando año {anio}: {e}", exc_info=True)

    logger.info("ETL MEF completado.")


if __name__ == "__main__":
    # Para backfill histórico: main(anios=[2020, 2021, 2022, 2023, 2024])
    main()
