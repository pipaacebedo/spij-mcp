"""spij-mcp: servidor MCP para el SPIJ (Sistema Peruano de Informacion Juridica).

Busca y lee normas legales y jurisprudencia del acervo del SPIJ
(spij.minjus.gob.pe) directamente desde tu harness MCP: 7 tools con
busqueda filtrada, lectura progresiva de textos y diagnostico de busqueda.

Proyecto NO oficial, sin afiliacion con MINJUS ni con el SPIJ. La
autenticacion usa la misma sesion publica del sitio web y se renueva sola
(JWT de 24 h). El servidor no guarda archivos: devuelve contenido limpio.
"""

import asyncio
import html
import json
import os
import re
import time
import unicodedata
from typing import Any, Awaitable, Callable, Optional

import httpx
from mcp.server.fastmcp import FastMCP

BASE_BACK = os.getenv("SPIJ_BACK", "https://spijwsii.minjus.gob.pe/spij-ext-back/")
BASE_SOLR = os.getenv("SPIJ_SOLR", "https://spijwsii.minjus.gob.pe/spij-ext-solr/")
FRONT_URL = os.getenv("SPIJ_FRONT", "https://spij.minjus.gob.pe/spij-ext-web/#/detallenorma/")
SPIJ_USER = os.getenv("SPIJ_USER", "spijext")
SPIJ_PASS = os.getenv("SPIJ_PASS", "password")

TIMEOUT = float(os.getenv("SPIJ_TIMEOUT", "60"))
MAX_CONCURRENCY = int(os.getenv("SPIJ_MAX_CONCURRENCY", "4"))
INFLIGHT_CAP = int(os.getenv("SPIJ_INFLIGHT_CAP", "24"))
CACHE_TTL = int(os.getenv("SPIJ_CACHE_TTL", "600"))
CACHE_BYTES = int(os.getenv("SPIJ_CACHE_BYTES", str(16 * 1024 * 1024)))
CACHE_MAX_ENTRY = int(os.getenv("SPIJ_CACHE_MAX_ENTRY", str(2 * 1024 * 1024)))
MAX_PAGINAS = int(os.getenv("SPIJ_MAX_PAGINAS", "10"))
TAM_PAGINA = 10
MAX_TEXTO = int(os.getenv("SPIJ_MAX_TEXTO", "20000"))

_TTL_BUSCAR = CACHE_TTL
_TTL_DETALLE = max(CACHE_TTL, 1800)
_TTL_WORD = max(CACHE_TTL, 3600)
_TTL_MAESTROS = 86400

_URL_RE = re.compile(r"^H\d{1,8}$")
_ORDENES = {"1", "2", "3", "4"}

_INSTRUCCIONES = (
    "Servidor SPIJ (normas y jurisprudencia peruana). Lee en 4 niveles para "
    "gastar pocos tokens sin perder acceso al corpus completo.\n"
    "0 DESCUBRIR - buscar_normas / buscar_jurisprudencia: AND implicito con "
    "TODAS las palabras; EMPIEZA con 1-2 palabras; sin comillas ni OR/NOT; "
    "comodin '*' ok ('presupuest*'). Si hay ruido, repite la MISMA cadena "
    "como 'sumilla': busca en el resumen y es mucho mas preciso. "
    "Jurisprudencia: filtra con organismo='TC' (o 'CORTE SUPREMA', "
    "'OSIPTEL'...): el indice no aplica 'tomo'. Los filtros se COMBINAN: "
    "numero + dispositivo + fechas aisan un dispositivo concreto (ej "
    "numero='874' + dispositivo='DECRETO LEGISLATIVO'). Los filtros "
    "resuelven parciales solos; si la respuesta trae 'candidatos', elige "
    "uno exacto. Valores validos: listar_filtros.\n"
    "1 MAPEAR - estructura_norma(id): indice de encabezados con offsets "
    "(vacio en sentencias: son prosa, pasa al nivel 2).\n"
    "2 SALTAR - buscar_en_norma(id, texto): ocurrencias de una palabra o "
    "frase dentro del texto, con offset.\n"
    "3 LEER - texto_completo(id, offset, max_chars) o detalle_norma(id, "
    "...); desde_final=True para leer el RESUELVE de una sentencia directo. "
    "Los offsets de los niveles 1-2 apuntan al mismo texto canonico. Los "
    "ids H... solo valen en este servidor.\n"
    "Cada doc trae id, url_web y sumilla (sumilla_chars acota; la completa "
    "esta en detalle_norma). Ante 0 resultados usa las 'sugerencias' "
    "({param, consulta, total}); si buscas por 'numero' y no existe "
    "registro propio, la respuesta trae los documentos que lo mencionan. "
    "Paginas de 10: 'start' + 'siguiente_start'. Fechas AAAA-MM-DD. Cita "
    "siempre: id + codigoNorma + fechaPublicacion + url_web."
)

mcp = FastMCP("spij-mcp", instructions=_INSTRUCCIONES, stateless_http=True)

_CLIENT: Optional[httpx.AsyncClient] = None
_SEM: Optional[asyncio.Semaphore] = None
_TOK_BACK = ""
_TOK_SOLR = ""
_TOK_EXP = 0.0

_CACHE: dict[str, tuple[float, float, str]] = {}
_CACHE_BYTES = 0
_INFLIGHT: dict[str, asyncio.Future] = {}
_INFLIGHT_UPSTREAM = 0


def _client() -> httpx.AsyncClient:
    global _CLIENT
    if _CLIENT is None:
        _CLIENT = httpx.AsyncClient(
            timeout=TIMEOUT,
            follow_redirects=True,
            headers={
                "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/126.0.0.0 Safari/537.36"),
                "Accept-Language": "es-PE,es;q=0.9",
                "Origin": "https://spij.minjus.gob.pe",
                "Referer": "https://spij.minjus.gob.pe/spij-ext-web/",
            },
        )
    return _CLIENT


def _sem() -> asyncio.Semaphore:
    global _SEM
    if _SEM is None:
        _SEM = asyncio.Semaphore(MAX_CONCURRENCY)
    return _SEM


# ---------------------------------------------------------------------- auth

async def _ensure_auth(force: bool = False) -> None:
    """Obtiene (o renueva) los dos JWT del SPIJ con las credenciales publicas."""
    global _TOK_BACK, _TOK_SOLR, _TOK_EXP
    if not force and _TOK_BACK and _TOK_SOLR and time.time() < _TOK_EXP:
        return
    client = _client()
    r = await client.post(
        BASE_BACK + "authenticate",
        json={"usuario": SPIJ_USER, "clave": SPIJ_PASS, "tipo": 1})
    if r.status_code != 200:
        raise RuntimeError(f"autenticacion SPIJ fallida (HTTP {r.status_code})")
    tok_b = (r.json() or {}).get("value") or ""
    r = await client.post(
        BASE_SOLR + "authenticate",
        json={"usuario": SPIJ_USER, "clave": SPIJ_PASS})
    if r.status_code != 200:
        raise RuntimeError(f"autenticacion SPIJ-solr fallida (HTTP {r.status_code})")
    tok_s = (r.json() or {}).get("value") or ""
    if not tok_b or not tok_s:
        raise RuntimeError("autenticacion SPIJ sin token")
    _TOK_BACK, _TOK_SOLR = tok_b, tok_s
    _TOK_EXP = time.time() + 23 * 3600


def _es_error_auth(status: int, text: str) -> bool:
    if status in (401, 403):
        return True
    if status == 400 and "Authorization" in text:
        return True
    if status == 500 and ("JWT" in text or "token" in text.lower()):
        return True
    return False


# ---------------------------------------------------------------------- cache

def _cache_get(key: str) -> Optional[str]:
    hit = _CACHE.get(key)
    if hit and time.time() - hit[0] < hit[1]:
        return hit[2]
    if hit:
        global _CACHE_BYTES
        _CACHE.pop(key, None)
        _CACHE_BYTES -= len(hit[2])
    return None


def _cache_put(key: str, body: str, ttl: float) -> None:
    global _CACHE_BYTES
    size = len(body)
    if size > CACHE_MAX_ENTRY:
        return
    while _CACHE and _CACHE_BYTES + size > CACHE_BYTES:
        viejo = next(iter(_CACHE))
        _, _, viejo_body = _CACHE.pop(viejo)
        _CACHE_BYTES -= len(viejo_body)
    if _CACHE_BYTES + size > CACHE_BYTES:
        return
    _CACHE[key] = (time.time(), ttl, body)
    _CACHE_BYTES += size


async def _fetch_cached(key: str, ttl: float, fn: Callable[[], Awaitable[str]]) -> str:
    global _INFLIGHT_UPSTREAM
    cached = _cache_get(key)
    if cached is not None:
        return cached
    tarea = _INFLIGHT.get(key)
    if tarea is not None:
        return await asyncio.shield(tarea)
    if _INFLIGHT_UPSTREAM >= max(1, INFLIGHT_CAP):
        raise RuntimeError("429: servidor ocupado, reintenta en 30 segundos")
    _INFLIGHT_UPSTREAM += 1
    tarea = asyncio.ensure_future(fn())
    _INFLIGHT[key] = tarea
    try:
        body = await tarea
    finally:
        _INFLIGHT.pop(key, None)
        _INFLIGHT_UPSTREAM -= 1
    _cache_put(key, body, ttl)
    return body


# ---------------------------------------------------------------------- fetch

async def _do_back(path: str) -> str:
    client = _client()
    last_err = ""
    delays = (0.0, 2.0, 5.0)
    for wait in delays:
        if wait:
            await asyncio.sleep(wait)
        await _ensure_auth()
        try:
            async with _sem():
                r = await client.get(
                    BASE_BACK + path,
                    headers={"Authorization": "Bearer " + _TOK_BACK})
        except httpx.TransportError as e:
            last_err = f"red: {e}"
            continue
        if r.status_code == 200:
            return r.text
        if _es_error_auth(r.status_code, r.text[:400]):
            await _ensure_auth(force=True)
            last_err = f"HTTP {r.status_code}"
            continue
        if r.status_code >= 500:
            last_err = f"HTTP {r.status_code}"
            continue
        raise RuntimeError(f"HTTP {r.status_code}")
    raise RuntimeError(f"Tras 3 intentos: {last_err}")


async def _do_solr(payload: dict) -> str:
    client = _client()
    last_err = ""
    delays = (0.0, 2.0, 5.0)
    for wait in delays:
        if wait:
            await asyncio.sleep(wait)
        await _ensure_auth()
        try:
            async with _sem():
                r = await client.post(
                    BASE_SOLR + "api/buscar",
                    json=payload,
                    headers={"Authorization": "Bearer " + _TOK_SOLR,
                             "Content-Type": "application/json"})
        except httpx.TransportError as e:
            last_err = f"red: {e}"
            continue
        if r.status_code == 200:
            return r.text
        if _es_error_auth(r.status_code, r.text[:400]):
            await _ensure_auth(force=True)
            last_err = f"HTTP {r.status_code}"
            continue
        if r.status_code >= 500:
            last_err = f"HTTP {r.status_code}"
            continue
        raise RuntimeError(f"HTTP {r.status_code}")
    raise RuntimeError(f"Tras 3 intentos: {last_err}")


# ---------------------------------------------------------------------- utils

def _sin_acentos(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s)
                   if unicodedata.category(c) != "Mn")


def _resolver(nombre: str, opciones: list[str]) -> tuple[Optional[str], list[str]]:
    """Match por igualdad exacta (sin acentos) o inclusion. Devuelve
    (eleccion, candidatos)."""
    n = _sin_acentos(nombre or "").strip().upper()
    if not n or n == "NINGUNO":
        return None, []
    exactos = [o for o in opciones if _sin_acentos(o).strip().upper() == n]
    if exactos:
        return exactos[0], []
    contienen = [o for o in opciones if n in _sin_acentos(o).strip().upper()]
    if len(contienen) == 1:
        return contienen[0], []
    if len(contienen) > 1:
        return None, contienen[:8]
    return None, []


def _norm_fecha(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = str(s).strip()
    m = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def _limpia_html(s: Optional[str]) -> str:
    if not s:
        return ""
    s = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", s)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</p>|</div>|</h[1-6]>|</tr>|</li>", "\n", s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = html.unescape(s)
    s = s.replace("\xa0", " ").replace("\u200b", "")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r" ?\n ?", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _cortar(s: str, limite: int = MAX_TEXTO) -> tuple[str, bool]:
    if len(s) <= limite:
        return s, False
    return s[:limite], True


def _rango(s: str, offset: int, max_chars: int,
           desde_final: bool = False) -> tuple[str, int, bool, Optional[int]]:
    """Devuelve (texto, total_caracteres, hay_mas, siguiente_offset).

    desde_final=True lee los ULTIMOS max_chars caracteres (la decision
    'RESUELVE' de una sentencia vive al final del texto)."""
    total = len(s)
    lim = max(1, min(int(max_chars or MAX_TEXTO), MAX_TEXTO))
    if desde_final:
        ini = max(0, total - lim)
        texto = s[ini:]
        return texto, total, ini > 0, None
    off = max(0, min(int(offset or 0), total))
    texto = s[off:off + lim]
    hay_mas = off + len(texto) < total
    sig = (off + lim) if hay_mas else None
    return texto, total, hay_mas, sig


def _parse_total(v: Any) -> int:
    try:
        return int(float(str(v)))
    except (TypeError, ValueError):
        return 0


SUMILLA_DEFAULT = 240


def _parse_doc(d: dict, sumilla_max: int = SUMILLA_DEFAULT) -> dict:
    nid = str(d.get("id") or "")
    sumilla = _limpia_html(d.get("sumilla"))
    doc = {
        "id": nid,
        "url_web": FRONT_URL + nid,
        "codigoNorma": (d.get("codigoNorma") or "").strip(),
        "dispositivoLegal": (d.get("dispositivoLegal") or "").strip(),
        "sector": (d.get("sector") or "").strip(),
        "fechaPublicacion": (d.get("fechaPublicacion") or "").strip(),
        "sumilla": sumilla,
    }
    if sumilla_max > 0 and len(sumilla) > sumilla_max:
        doc["sumilla"] = sumilla[:sumilla_max].rstrip()
        doc["sumilla_total"] = len(sumilla)
    return doc


def _payload_buscar(texto: Optional[str], sumilla: Optional[str],
                    dispositivo: list, agrupacion: list, sector: list,
                    numero: str, fecha_ini: Optional[str],
                    fecha_fin: Optional[str], orden: str,
                    desde: int, hasta: int, tipo_norma: str = "NR",
                    tomo: str = "", materia: str = "") -> dict:
    return {
        "tipoNorma": tipo_norma,
        "filtros": {
            "buscarHistorico": False,
            "busquedaSugerida": False,
            "numeroDispositivoLegal": numero or " ",
            "dispositivoLegal": dispositivo,
            "tomo": {"id": "", "nombre": tomo},
            "materia": {"id": "", "nombre": materia},
            "agrupacion": agrupacion,
            "sector": sector,
            "subSector": {"id": "", "nombre": ""},
            "orden": orden,
        },
        "facetsSeleccionadas": {
            "fechaPublicacionGap": {"numero": 10, "unidad": "YEAR"}},
        "textoBusqueda": texto,
        "textoSumilla": sumilla,
        "fechaInicio": fecha_ini,
        "fechaFin": fecha_fin,
        "desde": desde,
        "hasta": hasta,
    }


def _nota_vacio() -> str:
    return ("Sin resultados. Usa las 'sugerencias' incluidas si vienen; si no, "
            "acorta la consulta o revisa los filtros con listar_filtros.")


def _copia_payload(payload: dict) -> dict:
    return json.loads(json.dumps(payload))


def _payload_texto(payload: dict) -> dict:
    """Copia del payload sin filtros categoricos (conserva fechas, orden,
    tipoNorma). Las sugerencias cuentan el texto puro: el agente decide
    re-agregar filtros despues."""
    p = _copia_payload(payload)
    f = p["filtros"]
    f["dispositivoLegal"] = []
    f["agrupacion"] = []
    f["sector"] = []
    f["tomo"] = {"id": "", "nombre": ""}
    f["materia"] = {"id": "", "nombre": ""}
    f["subSector"] = {"id": "", "nombre": ""}
    f["numeroDispositivoLegal"] = " "
    return p


async def _sugerencias_corta(payload: dict, consulta: str) -> list:
    """0 resultados: prueba versiones mas cortas de la consulta SIN filtros
    categoricos (max 2 llamadas upstream; para en el primer >0)."""
    palabras = (consulta or "").split()
    if not palabras:
        return []
    base = _payload_texto(payload)
    cortas = ([" ".join(palabras[:2]), palabras[0]] if len(palabras) >= 3
              else [palabras[0]])
    out = []
    vistos = set()
    for corta in cortas:
        if corta in vistos:
            continue
        vistos.add(corta)
        p2 = _copia_payload(base)
        p2["textoBusqueda"] = corta
        key = "B:" + json.dumps(p2, sort_keys=True, ensure_ascii=False)
        try:
            body = await _fetch_cached(key, _TTL_BUSCAR, lambda p=p2: _do_solr(p))
            t = _parse_total(json.loads(body).get("totalEncontrados"))
        except (RuntimeError, ValueError):
            continue
        out.append({"param": "consulta", "consulta": corta, "total": t})
        if t > 0:
            break
    return out


async def _sugerencias_sumilla(payload: dict, sumilla: str) -> list:
    """0 resultados con sumilla: prueba la sumilla acortada (1 palabra)."""
    palabras = (sumilla or "").split()
    if len(palabras) < 2:
        return []
    base = _payload_texto(payload)
    p2 = _copia_payload(base)
    p2["textoSumilla"] = palabras[0]
    key = "B:" + json.dumps(p2, sort_keys=True, ensure_ascii=False)
    try:
        body = await _fetch_cached(key, _TTL_BUSCAR, lambda p=p2: _do_solr(p))
        t = _parse_total(json.loads(body).get("totalEncontrados"))
    except (RuntimeError, ValueError):
        return []
    return [{"param": "sumilla", "consulta": palabras[0], "total": t}]


async def _fallback_numero(payload: dict, numero: str) -> Optional[dict]:
    """El numero no esta indexado como registro: busca documentos que lo
    mencionen (texto=numero). Devuelve {'total','docs'} o None."""
    p3 = _copia_payload(payload)
    p3["filtros"]["numeroDispositivoLegal"] = " "
    p3["textoBusqueda"] = numero
    key = "B:" + json.dumps(p3, sort_keys=True, ensure_ascii=False)
    try:
        body = await _fetch_cached(key, _TTL_BUSCAR, lambda p=p3: _do_solr(p))
        j = json.loads(body)
    except (RuntimeError, ValueError):
        return None
    t = _parse_total(j.get("totalEncontrados"))
    if t <= 0:
        return None
    return {"total": t, "docs": [_parse_doc(d) for d in (j.get("resultados") or [])]}


_NOTA_NUMERO = (
    "no existe registro propio con ese numero en el indice del SPIJ; estos "
    "son los documentos que lo mencionan (busqueda del numero como texto)"
)
NUMERO_AMBIGUO_MIN = 50


# ------------------------------------------------------------- maestros cache

_MAESTROS_KEY = "M:maestros"
_SECTOR_KEY = "M:sector"


async def _maestros() -> dict:
    body = await _fetch_cached(_MAESTROS_KEY, _TTL_MAESTROS, lambda: _do_back("api/maestros"))
    try:
        j = json.loads(body)
        return (j[0] if isinstance(j, list) and j else {}) or {}
    except ValueError:
        return {}


async def _sectores() -> list[dict]:
    body = await _fetch_cached(_SECTOR_KEY, _TTL_MAESTROS, lambda: _do_back("api/sector"))
    try:
        j = json.loads(body)
        lst = j[0].get("sectores") if isinstance(j, list) and j else None
        return lst or []
    except (ValueError, AttributeError):
        return []


async def _lista_sectores_unicos() -> list[str]:
    rows = await _sectores()
    vistos: set[str] = set()
    out: list[str] = []
    for r in rows:
        n = (r.get("nombre") or "").strip()
        if n and n != "NINGUNO" and n not in vistos:
            vistos.add(n)
            out.append(n)
    return sorted(out)


# ---------------------------------------------------------------------- tools

@mcp.tool()
async def listar_filtros(categoria: Optional[str] = None,
                         tomo: Optional[str] = None) -> dict:
    """Lista los valores validos para los filtros del SPIJ.

    Args:
        categoria: una de 'dispositivos', 'agrupaciones', 'tomos', 'materias',
                   'sectores'. Si se omite, devuelve un resumen con conteos
                   y ejemplos de cada categoria.
        tomo: solo para categoria='materias': el tomo de jurisprudencia
              del que se quieren las especialidades, ej 'TRIBUNAL CONSTITUCIONAL'.

    'dispositivos': tipos de dispositivo legal (ej DECRETO SUPREMO).
    'agrupaciones': grupos del acervo (ej LEGISLACION SUPRANACIONAL).
    'tomos': tomos de jurisprudencia (ej TRIBUNAL CONSTITUCIONAL).
    'materias': especialidades de un tomo de jurisprudencia (requiere 'tomo').
    'sectores': entidades emisoras (hay cientos; si el tuyo no esta en la
                lista puedes escribir parte del nombre en buscar_normas y
                se resuelve automaticamente).

    Devuelve:
        dict con 'ok' y la lista 'valores': son los strings EXACTOS que
        aceptan buscar_normas y buscar_jurisprudencia.
    """
    try:
        ma = await _maestros()
        cat = (categoria or "").strip().lower()
        if not cat:
            resumen = {}
            for k, lista in (("dispositivos", ma.get("dispositivolegal")),
                             ("agrupaciones", ma.get("agrupacion")),
                             ("tomos", ma.get("tomo"))):
                nombres = [x.get("nombre") for x in (lista or []) if x.get("nombre")]
                resumen[k] = {"total": len(nombres), "ejemplos": nombres[1:6]}
            sec = await _lista_sectores_unicos()
            resumen["sectores"] = {"total": len(sec), "ejemplos": sec[:5]}
            mat_keys = [k for k in (ma.get("materia") or {}).keys() if k != "NINGUNO"]
            resumen["materias"] = {"total": len(mat_keys), "ejemplos": mat_keys[:5],
                                   "nota": "usa listar_filtros(categoria='materias') para ver las materias de un tomo"}
            return {"ok": True, **resumen}

        if cat == "dispositivos":
            nombres = [x.get("nombre") for x in (ma.get("dispositivolegal") or [])
                       if x.get("nombre") and x.get("nombre") != "NINGUNO"]
            return {"ok": True, "categoria": cat, "valores": sorted(set(nombres))}
        if cat == "agrupaciones":
            nombres = [x.get("nombre") for x in (ma.get("agrupacion") or [])
                       if x.get("nombre") and x.get("nombre") != "NINGUNO"]
            return {"ok": True, "categoria": cat, "valores": nombres}
        if cat == "tomos":
            nombres = [x.get("nombre") for x in (ma.get("tomo") or [])
                       if x.get("nombre") and x.get("nombre") != "NINGUNO"]
            return {"ok": True, "categoria": cat, "valores": nombres}
        if cat == "materias":
            if not tomo:
                tomos = [x.get("nombre") for x in (ma.get("tomo") or [])
                         if x.get("nombre") and x.get("nombre") != "NINGUNO"]
                return {"ok": True, "categoria": cat,
                        "tomos_disponibles": tomos,
                        "nota": ("indica el 'tomo' (ej listar_filtros("
                                 "categoria='materias', tomo='TRIBUNAL "
                                 "CONSTITUCIONAL')) para ver sus especialidades")}
            eleccion, cand = await _resuelve_tomo(tomo)
            if not eleccion:
                return {"ok": False, "error": "tomo ambiguo o desconocido",
                        "candidatos": cand or [],
                        "pista": "usa listar_filtros(categoria='tomos')"}
            base = (ma.get("materia") or {}).get(eleccion) or []
            nombres = [x.get("nombre") for x in base
                       if x.get("nombre") and x.get("nombre") != "NINGUNO"]
            return {"ok": True, "categoria": "materias", "tomo": eleccion,
                    "valores": nombres}
        if cat == "sectores":
            todos = await _lista_sectores_unicos()
            return {"ok": True, "categoria": cat, "total": len(todos),
                    "valores": todos[:200],
                    "nota": ("hay %d sectores en total; si el tuyo no esta en la "
                             "lista escribe parte del nombre en el filtro 'sector' "
                             "de buscar_normas y se resuelve automaticamente"
                             % len(todos))}
        return {"ok": False,
                "error": ("categoria desconocida: %r. Usa 'dispositivos', "
                          "'agrupaciones', 'tomos', 'materias' o 'sectores'" % categoria)}
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}


async def _resuelve_tomo(tomo: Optional[str]) -> tuple[Optional[str], list[str]]:
    ma = await _maestros()
    tomos = [x.get("nombre") for x in (ma.get("tomo") or [])
             if x.get("nombre") and x.get("nombre") != "NINGUNO"]
    if not tomo:
        return None, tomos
    eleccion, cand = _resolver(tomo, tomos)
    return eleccion, cand


async def _resuelve_materia(tomo: str, materia: Optional[str]) -> tuple[Optional[str], list[str]]:
    ma = await _maestros()
    base = (ma.get("materia") or {}).get(tomo) or []
    nombres = [x.get("nombre") for x in base
               if x.get("nombre") and x.get("nombre") != "NINGUNO"]
    if not materia:
        return None, nombres
    eleccion, cand = _resolver(materia, nombres)
    return eleccion, (cand or nombres[:8])


@mcp.tool()
async def buscar_normas(consulta: Optional[str] = None,
                        sumilla: Optional[str] = None,
                        dispositivo: Optional[str] = None,
                        sector: Optional[str] = None,
                        agrupacion: Optional[str] = None,
                        numero: Optional[str] = None,
                        fecha_ini: Optional[str] = None,
                        fecha_fin: Optional[str] = None,
                        orden: str = "1",
                        start: int = 0,
                        paginas: int = 1,
                        sumilla_chars: int = SUMILLA_DEFAULT) -> dict:
    """Busca normas legales en el SPIJ (normas generales, administrativas, etc).

    Args:
        consulta: texto libre en el contenido. El buscador hace AND implicito
                  con TODAS las palabras (ej 'transferencia partidas').
                  OJO: NO acepta comillas ni OR. Si acepta comodin al final
                  de una raiz, ej 'presupuest*'. Empieza con 1-2 palabras.
        sumilla: texto a buscar solo en la sumilla (resumen) de la norma.
        dispositivo: tipo de dispositivo legal, ej 'DECRETO SUPREMO'.
                     Acepta parcial ('decreto sup') y se resuelve solo;
                     si es ambiguo devuelve candidatos. Ver listar_filtros.
        sector: entidad emisora, ej 'ECONOMIA Y FINANZAS'. Acepta parcial.
        agrupacion: grupo del acervo, ej 'LEGISLACION SUPRANACIONAL'.
        numero: numero del dispositivo, ej '200-2026-EF' o '31068' (sin
                prefijo 'Nº'). OJO: algunas leyes no estan indexadas como
                registro propio; en ese caso la respuesta trae los
                documentos que lo mencionan (con una nota que lo explica).
        fecha_ini / fecha_fin: rango de publicacion, formato AAAA-MM-DD.
        orden: '1' = fecha (recientes primero, default), '2'/'3'/'4' = relevancia.
        start: desplazamiento (multiplo de 10). Pagina N -> start = (N-1)*10.
        paginas: paginas consecutivas a traer en una sola llamada (cada pagina
                 = 10 resultados, max 10 paginas).
        sumilla_chars: maximo de caracteres de la sumilla por documento
                       (0 = sin limite). Para leer la sumilla completa usa
                       detalle_norma(id).

    Devuelve:
        ok, total, desde, hasta, siguiente_start (si hay mas paginas),
        normas: lista de {id, url_web, codigoNorma, dispositivoLegal, sector,
        fechaPublicacion, sumilla} (con 'sumilla_total' si se acoto).
        Ante 0 resultados devuelve 'nota' y 'sugerencias'
        ({param, consulta, total} de versiones mas cortas de la consulta
        o de la sumilla, sin filtros categoricos).
    """
    try:
        f_dis: list = []
        f_agr: list = []
        f_sec: list = []
        if dispositivo:
            ma = await _maestros()
            ops = [x.get("nombre") for x in (ma.get("dispositivolegal") or [])
                   if x.get("nombre") and x.get("nombre") != "NINGUNO"]
            eleccion, cand = _resolver(dispositivo, ops)
            if not eleccion:
                return {"ok": False, "error": "dispositivo ambiguo o desconocido",
                        "candidatos": cand or ops[:8],
                        "pista": "usa listar_filtros(categoria='dispositivos')"}
            f_dis = [eleccion]
        if agrupacion:
            ma = await _maestros()
            ops = [x.get("nombre") for x in (ma.get("agrupacion") or [])
                   if x.get("nombre") and x.get("nombre") != "NINGUNO"]
            eleccion, cand = _resolver(agrupacion, ops)
            if not eleccion:
                return {"ok": False, "error": "agrupacion ambigua o desconocida",
                        "candidatos": cand or ops,
                        "pista": "usa listar_filtros(categoria='agrupaciones')"}
            f_agr = [eleccion]
        if sector:
            ops = await _lista_sectores_unicos()
            eleccion, cand = _resolver(sector, ops)
            if not eleccion:
                return {"ok": False, "error": "sector ambiguo o desconocido",
                        "candidatos": cand or [],
                        "pista": "usa listar_filtros(categoria='sectores')"}
            f_sec = [eleccion]

        paginas = max(1, min(int(paginas or 1), MAX_PAGINAS))
        start = max(0, int(start or 0))
        if orden not in _ORDENES:
            orden = "1"
        f_ini, f_fin = _norm_fecha(fecha_ini), _norm_fecha(fecha_fin)
        desde = start
        hasta = start + TAM_PAGINA * paginas
        payload = _payload_buscar(
            texto=(consulta or None) if (consulta and consulta.strip()) else None,
            sumilla=(sumilla or None) if (sumilla and sumilla.strip()) else None,
            dispositivo=f_dis, agrupacion=f_agr, sector=f_sec,
            numero=(numero or "").strip(), fecha_ini=f_ini, fecha_fin=f_fin,
            orden=orden, desde=desde, hasta=hasta, tipo_norma="NR")
        key = "B:" + json.dumps(payload, sort_keys=True, ensure_ascii=False)
        body = await _fetch_cached(key, _TTL_BUSCAR, lambda: _do_solr(payload))
        j = json.loads(body)
        total = _parse_total(j.get("totalEncontrados"))
        docs = [_parse_doc(d, sumilla_chars) for d in (j.get("resultados") or [])]
        out: dict = {"ok": True, "total": total, "desde": desde, "hasta": hasta,
                     "normas": docs}
        if desde + len(docs) < total:
            out["siguiente_start"] = desde + len(docs)
        if numero and total > NUMERO_AMBIGUO_MIN:
            out["nota"] = ("coincidencia parcial del numero %r (%d resultados): "
                           "afina agregando dispositivo y/o fechas" % (numero, total))
        if not docs:
            if numero and not (consulta or "").strip() and not (sumilla or "").strip():
                fb = await _fallback_numero(payload, numero)
                if fb:
                    return {"ok": True, "total": fb["total"], "desde": desde,
                            "hasta": hasta, "normas": fb["docs"],
                            "nota": _NOTA_NUMERO}
            out["nota"] = _nota_vacio()
            sug = await _sugerencias_corta(payload, (consulta or "").strip())
            if not sug and (sumilla or "").strip():
                sug = await _sugerencias_sumilla(payload, (sumilla or "").strip())
            if sug:
                out["sugerencias"] = sug
        return out
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
async def buscar_jurisprudencia(consulta: Optional[str] = None,
                                tomo: Optional[str] = None,
                                materia: Optional[str] = None,
                                organismo: Optional[str] = None,
                                numero: Optional[str] = None,
                                fecha_ini: Optional[str] = None,
                                fecha_fin: Optional[str] = None,
                                orden: str = "1",
                                start: int = 0,
                                paginas: int = 1,
                                sumilla_chars: int = SUMILLA_DEFAULT) -> dict:
    """Busca jurisprudencia en el SPIJ (TC, cortes, precedentes, sentencias).

    Args:
        consulta: texto libre (AND implicito, sin comillas ni OR; comodin * ok).
                  Empieza con 1-2 palabras.
        tomo: tomo de jurisprudencia, ej 'TRIBUNAL CONSTITUCIONAL'.
              OJO: se envia al SPIJ pero el indice NO lo aplica (verificado:
              los totales son identicos con y sin tomo) - filtra con la
              consulta y/o fechas.
        materia: especialidad dentro del tomo, ej 'JURISPRUDENCIA ADMINISTRATIVA'
                 del tomo del TC. Ver listar_filtros(categoria='materias').
        organismo: filtra LOCALMENTE por organismo emisor, ej 'TC',
                   'CORTE SUPREMA', 'OSIPTEL', 'CGD' (coincide contra
                   codigo/sector/dispositivo, sin acentos). El servidor
                   escanea hasta 5 paginas del SPIJ y devuelve solo los
                   coincidentes (con 'filtrados' y 'escaneadas'). Es la
                   via correcta: el indice no filtra por tomo.
        numero: numero de expediente o codigo, ej '03209-2024-PA/TC'
                (expediente completo del TC funciona).
        fecha_ini / fecha_fin: rango de publicacion AAAA-MM-DD.
        orden: '1' = fecha (default), '2'/'3'/'4' = relevancia.
        start / paginas: igual que buscar_normas (por cada pagina devuelta
                         el servidor puede escanear varias internamente).
        sumilla_chars: maximo de caracteres de la sumilla por documento
                       (0 = sin limite).

    Devuelve:
        ok, total (del indice), organismo, escaneadas (paginas escaneadas),
        filtrados (coincidencias encontradas), normas, siguiente_start (si
        quedan paginas por escanear). Si la consulta base arroja 0
        resultados incluye         'sugerencias'; si el indice arroja resultados
        pero ninguno del organismo pedido, incluye 'organismos_disponibles'
        (los organismos presentes en lo escaneado). El texto se obtiene con
        detalle_norma(id) o texto_completo(id).
    """
    try:
        f_tomo: Optional[str] = None
        f_mat: Optional[str] = None
        if tomo:
            eleccion, cand = await _resuelve_tomo(tomo)
            if not eleccion:
                return {"ok": False, "error": "tomo ambiguo o desconocido",
                        "candidatos": cand or [],
                        "pista": "usa listar_filtros(categoria='tomos')"}
            f_tomo = eleccion
        if materia:
            eleccion, cand = await _resuelve_materia(f_tomo or "", materia)
            if not eleccion:
                return {"ok": False, "error": "materia ambigua o desconocida",
                        "candidatos": cand or [],
                        "pista": ("indica 'tomo' y usa "
                                  "listar_filtros(categoria='materias')")}
            f_mat = eleccion

        paginas = max(1, min(int(paginas or 1), MAX_PAGINAS))
        start = max(0, int(start or 0))
        if orden not in _ORDENES:
            orden = "1"
        f_ini, f_fin = _norm_fecha(fecha_ini), _norm_fecha(fecha_fin)
        desde = start
        hasta = start + TAM_PAGINA * paginas
        payload = _payload_buscar(
            texto=(consulta or None) if (consulta and consulta.strip()) else None,
            sumilla=None, dispositivo=[], agrupacion=[], sector=[],
            numero=(numero or "").strip(), fecha_ini=f_ini, fecha_fin=f_fin,
            orden=orden, desde=desde, hasta=hasta, tipo_norma="JR",
            tomo=f_tomo or "", materia=f_mat or "")

        if organismo and organismo.strip():
            org = organismo.strip()
            acum = []
            toda = []
            escaneadas = 0
            pos = start
            base_total = 0
            while escaneadas < ORGANISMO_SCAN_MAX:
                payload["desde"] = pos
                payload["hasta"] = pos + TAM_PAGINA
                key = "B:" + json.dumps(payload, sort_keys=True, ensure_ascii=False)
                body = await _fetch_cached(key, _TTL_BUSCAR,
                                           lambda p=payload: _do_solr(p))
                j = json.loads(body)
                base_total = _parse_total(j.get("totalEncontrados"))
                pag = [_parse_doc(d, sumilla_chars)
                       for d in (j.get("resultados") or [])]
                if not pag:
                    break
                escaneadas += 1
                pos += TAM_PAGINA
                toda.extend(pag)
                for d in pag:
                    if _match_organismo(d, org):
                        acum.append(d)
                if len(acum) >= TAM_PAGINA * paginas or pos >= base_total:
                    break
            out: dict = {"ok": True, "total": base_total, "organismo": org,
                         "escaneadas": escaneadas, "filtrados": len(acum),
                         "desde": start, "hasta": start + len(acum),
                         "normas": acum[:TAM_PAGINA * paginas]}
            if pos < base_total:
                out["siguiente_start"] = pos
            if not acum:
                if base_total == 0:
                    out["nota"] = ("la consulta no arroja resultados en el "
                                   "indice; revisa las 'sugerencias'")
                    sug = await _sugerencias_corta(payload,
                                                   (consulta or "").strip())
                    if sug:
                        out["sugerencias"] = sug
                else:
                    out["nota"] = ("ningun documento de %d pagina(s) "
                                   "escaneada(s) coincide con organismo %r; "
                                   "ajusta la consulta o escanea mas con start"
                                   % (escaneadas, org))
                    orgs = _organismos_de_docs(toda)
                    if orgs:
                        out["organismos_disponibles"] = orgs
                        out["nota"] = (out["nota"] + "; organismos presentes "
                                       "en lo escaneado: 'organismos_disponibles'")
            elif len(acum) < TAM_PAGINA * paginas and pos < base_total:
                out["nota"] = ("escaneo parcial: usa siguiente_start para "
                               "mas coincidencias del organismo")
            return out

        key = "B:" + json.dumps(payload, sort_keys=True, ensure_ascii=False)
        body = await _fetch_cached(key, _TTL_BUSCAR, lambda: _do_solr(payload))
        j = json.loads(body)
        total = _parse_total(j.get("totalEncontrados"))
        docs = [_parse_doc(d, sumilla_chars) for d in (j.get("resultados") or [])]
        out: dict = {"ok": True, "total": total, "desde": desde, "hasta": hasta,
                     "normas": docs}
        if desde + len(docs) < total:
            out["siguiente_start"] = desde + len(docs)
        if numero and total > NUMERO_AMBIGUO_MIN:
            out["nota"] = ("coincidencia parcial del numero %r (%d resultados): "
                           "afina agregando fechas u otra consulta" % (numero, total))
        if not docs:
            if numero and not (consulta or "").strip():
                fb = await _fallback_numero(payload, numero)
                if fb:
                    return {"ok": True, "total": fb["total"], "desde": desde,
                            "hasta": hasta, "normas": fb["docs"],
                            "nota": _NOTA_NUMERO}
            out["nota"] = _nota_vacio()
            sug = await _sugerencias_corta(payload, (consulta or "").strip())
            if sug:
                out["sugerencias"] = sug
        return out
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}


async def _texto_norma(nid: str) -> tuple[str, str]:
    """Texto canonico de una norma (word -> detalle). Fuente unica para
    texto_completo, estructura_norma y buscar_en_norma: los offsets que
    devuelven apuntan al MISMO texto."""
    texto_full = ""
    fuente = ""
    try:
        texto_full = _limpia_html(await _word_html(nid))
        fuente = "procesarword"
    except RuntimeError:
        pass
    if len(texto_full) < 400:
        try:
            j2 = await _detalle_json(nid)
            alterno = _limpia_html(j2.get("textoCompleto") or "")
            if len(alterno) > len(texto_full):
                texto_full = alterno
                fuente = "detallenorma"
        except RuntimeError:
            pass
    return texto_full, fuente


_RE_ENCABEZADO = re.compile(
    r"^(T[ÍI]TULO|CAP[ÍI]TULO|SECCI[ÓO]N|SUBCAP[ÍI]TULO|DISPOSICIONES|ANEXO|"
    r"PRIMERA|SEGUNDA|TERCERA|CUARTA|QUINTA|SEXTA)\b")
_RE_ARTICULO = re.compile(r"^(Art(?:[íi]culo|\.)\s*(?:N|N[°º])?[\.º°]?\s*\d+)")
MAX_ENCABEZADOS = 150
ORGANISMO_SCAN_MAX = 5


def _match_organismo(doc: dict, organismo: str) -> bool:
    """Match sin acentos del organismo contra codigo/sector/dispositivo.
    ej: 'TC' pilla '-TC' y 'TRIBUNAL CONSTITUCIONAL'; 'OSIPTEL' pilla el
    sector y el codigo '/OSIPTEL'."""
    n = _sin_acentos(organismo or "").strip().upper()
    if not n:
        return False
    campos = " ".join((doc.get("codigoNorma") or "", doc.get("sector") or "",
                       doc.get("dispositivoLegal") or ""))
    return n in _sin_acentos(campos).upper()


def _organismos_de_docs(docs: list) -> list:
    """Tally de organismos presentes en una tanda de documentos: etiquetas
    de sector + sufijos de codigo (ej 'TC', 'OSIPTEL')."""
    vistos: set[str] = set()
    for d in docs:
        sec = (d.get("sector") or "").strip().upper()
        if sec and sec != "NINGUNO":
            vistos.add(_sin_acentos(sec).upper())
        cod = (d.get("codigoNorma") or "").strip().upper()
        if "/" in cod:
            tag = _sin_acentos(cod.rsplit("/", 1)[-1]).strip()
            if 2 <= len(tag) <= 12:
                vistos.add(tag)
    return sorted(vistos)[:12]


@mcp.tool()
async def estructura_norma(id: str) -> dict:
    """Devuelve el INDICE de encabezados de una norma del SPIJ, con el
    offset de cada uno para leer secciones concretas con
    texto_completo(id, offset=X, max_chars=N).

    Args:
        id: identificador del SPIJ, ej 'H1453062' (lo dan buscar_*).

    Devuelve:
        ok, id, url_web, fuente, total_caracteres, total_encabezados,
        encabezados: lista de {tipo: 'seccion'|'articulo', titulo, offset}
        (hasta 150; con 'siguiente_encabezado' si hay mas).
        Los offsets apuntan al mismo texto canonico que sirve texto_completo.
        Si la norma no tiene encabezados (sentencias del TC son prosa),
        'encabezados' viene vacia: usa buscar_en_norma o lectura por rangos.
    """
    try:
        nid = (id or "").strip().upper()
        if not _URL_RE.match(nid):
            return {"ok": False,
                    "error": "id invalido: debe ser como 'H1453062' (lo dan buscar_*)"}
        texto, fuente = await _texto_norma(nid)
        if len(texto) < 100:
            return {"ok": False, "error": f"no se pudo obtener el texto de {nid}"}
        enc = []
        for m in re.finditer(r"[^\n]{3,140}", texto):
            lin = m.group(0).strip()
            if len(lin) > 120:
                continue
            if _RE_ENCABEZADO.match(lin.upper()) or _RE_ARTICULO.match(lin):
                tipo = "articulo" if _RE_ARTICULO.match(lin) else "seccion"
                enc.append({"tipo": tipo, "titulo": lin[:80], "offset": m.start()})
        out = {"ok": True, "id": nid, "url_web": FRONT_URL + nid,
               "fuente": fuente, "total_caracteres": len(texto),
               "total_encabezados": len(enc)}
        if len(enc) > MAX_ENCABEZADOS:
            out["encabezados"] = enc[:MAX_ENCABEZADOS]
            out["siguiente_encabezado"] = MAX_ENCABEZADOS
        else:
            out["encabezados"] = enc
        if not enc:
            out["nota"] = ("sin encabezados detectados (texto corrido); usa "
                           "buscar_en_norma para ubicar secciones o lee por "
                           "rangos con texto_completo(offset=...)")
        return out
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}


@mcp.tool()
async def buscar_en_norma(id: str, texto: str, contexto: int = 200,
                          max_matches: int = 10) -> dict:
    """Busca una palabra o frase DENTRO del texto completo de una norma del
    SPIJ, y devuelve cada ocurrencia con su offset (para leer alrededor con
    texto_completo(id, offset=X, max_chars=N)). Ideal para saltar a secciones
    concretas (ej 'RATIFICAN', 'Artículo 8', 'DISPOSICIONES FINALES') sin
    leer todo el texto. La busqueda ignora acentos y mayusculas.

    Args:
        id: identificador del SPIJ, ej 'H1385488'.
        texto: palabra o frase a buscar dentro de la norma (1-8 palabras).
        contexto: caracteres de contexto alrededor de cada ocurrencia
                  (50-500, default 200).
        max_matches: maximo de ocurrencias a devolver (1-25, default 10).

    Devuelve:
        ok, id, url_web, total_caracteres, matches: lista de
        {offset, extracto}, hay_mas (si quedan ocurrencias).
    """
    try:
        nid = (id or "").strip().upper()
        q = (texto or "").strip()
        if not _URL_RE.match(nid):
            return {"ok": False,
                    "error": "id invalido: debe ser como 'H1453062' (lo dan buscar_*)"}
        if not q:
            return {"ok": False, "error": "indica la palabra o frase a buscar"}
        contexto = max(50, min(int(contexto or 200), 500))
        max_matches = max(1, min(int(max_matches or 10), 25))
        texto_full, fuente = await _texto_norma(nid)
        if len(texto_full) < 100:
            return {"ok": False, "error": f"no se pudo obtener el texto de {nid}"}
        hay = _sin_acentos(texto_full).lower()
        aguja = _sin_acentos(q).lower()
        matches = []
        pos = 0
        hay_mas = False
        while len(matches) < max_matches:
            p = hay.find(aguja, pos)
            if p < 0:
                break
            ini = max(0, p - contexto // 2)
            fin = min(len(texto_full), p + len(aguja) + contexto // 2)
            extracto = texto_full[ini:fin].replace("\n", " ").strip()
            matches.append({"offset": p, "extracto": extracto})
            pos = p + max(1, len(aguja))
        if hay.find(aguja, pos) >= 0:
            hay_mas = True
        return {"ok": True, "id": nid, "url_web": FRONT_URL + nid,
                "buscado": q, "fuente": fuente,
                "total_caracteres": len(texto_full),
                "matches": matches, "hay_mas": hay_mas}
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}


async def _detalle_json(nid: str) -> dict:
    key = "D:" + nid
    body = await _fetch_cached(key, _TTL_DETALLE,
                               lambda: _do_back("api/detallenorma/" + nid))
    return json.loads(body)


async def _word_html(nid: str) -> str:
    key = "W:" + nid
    return await _fetch_cached(key, _TTL_WORD,
                               lambda: _do_back("api/procesarword/" + nid))


@mcp.tool()
async def detalle_norma(id: str, offset: int = 0, max_chars: int = 0,
                        desde_final: bool = False) -> dict:
    """Devuelve los METADATOS y el TEXTO de una norma del SPIJ por su id.

    Args:
        id: identificador del SPIJ, ej 'H1453062' (lo devuelve buscar_normas).
        offset: desde que caracter del texto empezar (0 = inicio).
        max_chars: maximo de caracteres de texto a devolver en esta llamada
                   (0 = usa el limite por defecto). Si el texto es mas largo,
                   'siguiente_offset' indica por donde continuar.
        desde_final: si es True, devuelve los ULTIMOS max_chars caracteres
                     (en sentencias judiciales la decision 'RESUELVE' esta
                     al final).

    Devuelve:
        ok, id, url_web (para abrir la norma en el sitio SPIJ), codigoNorma,
        dispositivoLegal, sector, fechaPublicacion, ruta, titulo, sumilla,
        texto (texto plano, sin HTML), total_caracteres, texto_truncado,
        siguiente_offset (si hay mas texto).

    Nota: si el texto es muy largo para tu contexto, lee por rangos con
    offset (ej offset=20000) o la cola con desde_final=True.
    """
    try:
        nid = (id or "").strip().upper()
        if not _URL_RE.match(nid):
            return {"ok": False,
                    "error": "id invalido: debe ser como 'H1453062' (lo da buscar_normas)"}
        j = await _detalle_json(nid)
        texto_plano = _limpia_html(j.get("textoCompleto") or "")
        texto, total_c, trunc, sig = _rango(texto_plano, offset, max_chars,
                                            desde_final)
        paywalled = "suscriptores" in (j.get("textoCompleto") or "").lower()[:400]
        out = {
            "ok": True,
            "id": nid,
            "url_web": FRONT_URL + nid,
            "codigoNorma": (j.get("codigoNorma") or "").strip(),
            "dispositivoLegal": (j.get("dispositivoLegal") or "").strip(),
            "sector": (j.get("sector") or "").strip(),
            "fechaPublicacion": (j.get("fechaPublicacion") or "").strip(),
            "ruta": (j.get("ruta") or "").strip(),
            "titulo": (j.get("titulo") or "").strip(),
            "sumilla": _limpia_html(j.get("sumilla")),
            "texto": texto,
            "total_caracteres": total_c,
            "texto_truncado": trunc,
        }
        if sig is not None:
            out["siguiente_offset"] = sig
        if paywalled:
            out["nota"] = ("el detalle vino bloqueado para suscriptores; usa "
                           "texto_completo(id) que lo obtiene por la ruta del word")
        return out
    except RuntimeError as e:
        if "404" in str(e):
            return {"ok": False, "error": f"Norma no encontrada: {id}"}
        return {"ok": False, "error": str(e)}


@mcp.tool()
async def texto_completo(id: str, offset: int = 0, max_chars: int = 0,
                         desde_final: bool = False) -> dict:
    """Devuelve el TEXTO COMPLETO de una norma o jurisprudencia del SPIJ.

    Ruta robusta: obtiene el documento word del SPIJ (ruta que incluye el
    acervo completo de jurisprudencia) y devuelve texto plano. Si esa ruta
    falla, usa el texto del detalle.

    Args:
        id: identificador del SPIJ, ej 'H1352213' (lo dan buscar_*).
        offset: desde que caracter del texto empezar (0 = inicio).
        max_chars: maximo de caracteres a devolver en esta llamada
                   (0 = usa el limite por defecto). Si hay mas texto,
                   'siguiente_offset' indica por donde continuar.
        desde_final: si es True, devuelve los ULTIMOS max_chars caracteres
                     (en sentencias judiciales la decision 'RESUELVE' esta
                     al final; combinado con max_chars=3000 la lees directo).

    Devuelve:
        ok, id, url_web, codigoNorma, fechaPublicacion, dispositivoLegal,
        texto (texto plano), total_caracteres, texto_truncado,
        siguiente_offset (si hay mas; None con desde_final).
    """
    try:
        nid = (id or "").strip().upper()
        if not _URL_RE.match(nid):
            return {"ok": False,
                    "error": "id invalido: debe ser como 'H1453062' (lo da buscar_*)"}
        meta: dict = {}
        try:
            j = await _detalle_json(nid)
            meta = {
                "codigoNorma": (j.get("codigoNorma") or "").strip(),
                "dispositivoLegal": (j.get("dispositivoLegal") or "").strip(),
                "fechaPublicacion": (j.get("fechaPublicacion") or "").strip(),
            }
        except RuntimeError:
            pass

        texto_full, fuente = await _texto_norma(nid)
        if len(texto_full) < 100:
            return {"ok": False,
                    "error": f"no se pudo obtener el texto de {nid}",
                    "texto_obtenido": texto_full}
        texto, total_c, trunc, sig = _rango(texto_full, offset, max_chars,
                                            desde_final)
        out = {"ok": True, "id": nid, "url_web": FRONT_URL + nid,
               **meta, "fuente": fuente, "texto": texto,
               "total_caracteres": total_c, "texto_truncado": trunc}
        if sig is not None:
            out["siguiente_offset"] = sig
        return out
    except RuntimeError as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------- main

def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
