"""
SISERCOM - Clasificador automatico de leads con IA (Gemini)
v5.0 - Cambios clave vs v4.6:
  * Reprocesa leads INCOMPLETOS (antes: si tenia nivel, no se tocaba mas -> mitad de campos vacios).
    Ahora procesa cualquier lead al que le falte algun campo CLAVE, y llena SOLO los vacios.
  * Motor de IA: Gemini 2.5 Flash-Lite (configurable via GEMINI_MODEL).
  * Campos deterministas (sin IA) para TODOS los leads: ID SISERCOM, Nombre, Telefono x2, Correo (del contacto).
  * Nunca deja vacios los campos clave (defaults sensatos). Ciudad/vehiculo solo si se mencionan.
  * Escritura por lotes (PATCH de a 50) + tope de clasificaciones IA por corrida.

Variables de entorno:
  KOMMO_TOKEN     (requerida)
  GEMINI_API_KEY  (requerida)
  GEMINI_MODEL    (opcional, default gemini-2.5-flash-lite)
  WINDOW_DAYS     (opcional, 0 = todos los leads; default 0)
  MAX_PER_RUN     (opcional, tope de clasificaciones IA por corrida; default 150)
"""
import os, json, time, unicodedata, requests
from datetime import datetime, timedelta
import google.generativeai as genai

# ───────────────────────────── Config ─────────────────────────────
KOMMO_TOKEN = os.environ["KOMMO_TOKEN"]
GEMINI_KEY  = os.environ["GEMINI_API_KEY"]
MODEL       = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")
WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "0"))     # 0 = todos los leads
MAX_PER_RUN = int(os.environ.get("MAX_PER_RUN", "150"))   # tope de clasificaciones IA por corrida

BASE_URL = "https://dcisnerossisercomevcom.kommo.com/api/v4"
HEADERS  = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}

genai.configure(api_key=GEMINI_KEY)

# ─────────────────────── Mapa de campos Kommo ───────────────────────
CF = {
    "canal_entrada":    {"id": 487630, "enums": {"WhatsApp":363850,"Instagram":363852,"Facebook":363854,"Formulario web":363856,"Referido":363858,"Llamada":363860,"Otro":363862}},
    "tipo_entrada":     {"id": 487632, "enums": {"Organico":363864,"Pagado":363866,"Referido":363868,"Directo":363870,"Desconocido":363872}},
    "nivel_intencion":  {"id": 487654, "enums": {"Alta":363906,"Media":363908,"Baja":363910,"No calificado":363912}},
    "lead_score":       {"id": 487656},
    "tipo_cliente":     {"id": 487676, "enums": {"Persona":363914,"Empresa":363916,"Condominio/Edificio":363918,"Flota":363920,"Otro":363922}},
    "producto_interes": {"id": 487678, "enums": {"Cargador":363924,"Instalacion":363926,"Venta+Instalacion":363928,"Solar":363930,"Baterias":363932,"Otro":363934}},
    "ciudad_zona":      {"id": 488310, "enums": {"La Paz":364674,"Santa Cruz":364676,"Otra":364678}},
    "ciudad_texto":     {"id": 488800},
    "vehiculo":         {"id": 488316},
    "proxima_accion":   {"id": 487696, "enums": {"Enviar precio":363946,"Agendar visita":363948,"Llamar":363950,"Pedir datos":363952,"Seguimiento":363954,"Descartar":363956}},
    "fuente_original":  {"id": 487698},
    "id_sisercom":      {"id": 489858},
    "nombre_cliente":   {"id": 490852},
    "telefono_cliente": {"id": 490854},
    "telefono_num":     {"id": 490386},
    "correo":           {"id": 490388},
}

# Campos que definen si un lead esta "clasificado". Si falta CUALQUIERA -> se reprocesa.
# (ciudad/vehiculo NO entran: pueden estar legitimamente vacios si el lead no los menciona.)
GATE_KEYS   = ("nivel_intencion","lead_score","tipo_cliente","producto_interes",
               "proxima_accion","canal_entrada","tipo_entrada","fuente_original")
GATE_FIELDS = [CF[k]["id"] for k in GATE_KEYS]

STAGES = {
    105087411:"Leads Entrantes", 105087415:"Contacto inicial", 105111343:"Interesado",
    105124671:"Solicita visita", 105087419:"Visita agendada", 105087423:"Cotización enviada",
    105122979:"Pago anticipo", 105122983:"Saldo pendiente", 105244923:"Otros intereses en VE",
    105130443:"Solo información", 105122735:"No interesado", 142:"Realizado (ganado)", 143:"Venta perdida",
}
ORIGIN_TO_CANAL = {"waba":"WhatsApp","whatsapp":"WhatsApp","facebook":"Facebook",
                   "instagram_business":"Instagram","instagram":"Instagram"}

NIVEL_TO_TAG   = {"Alta":"alta_intencion","Media":"media_intencion","Baja":"baja_intencion","No calificado":"no_calificado"}
ACCION_TO_TAG  = {"Agendar visita":"solicita_visita","Enviar precio":"solicita_precio","Seguimiento":"no_respondio"}
CLIENTE_TO_TAG = {"Empresa":"cliente_b2b","Flota":"interes_ev_flota","Condominio/Edificio":"cliente_b2b"}

# Para mostrar valores actuales en el contexto de la IA
NAMES_FOR_CONTEXT = {
    "nivel_intencion":"Nivel intencion", "lead_score":"Score", "tipo_cliente":"Tipo cliente",
    "producto_interes":"Producto", "ciudad_texto":"Ciudad", "vehiculo":"Vehiculo",
    "canal_entrada":"Canal", "proxima_accion":"Proxima accion",
}

def _norm(s):
    """minusculas + sin acentos, para matchear opciones de forma robusta."""
    return unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode().lower().strip()

# lookup normalizado de enums (acepta 'Orgánico', 'instalación', etc.)
for _k, _v in CF.items():
    if "enums" in _v:
        _v["_norm"] = {_norm(name): eid for name, eid in _v["enums"].items()}

# ───────────────────────── Kommo helpers ─────────────────────────
def kget(path, params=None):
    for _ in range(3):
        try:
            r = requests.get(f"{BASE_URL}{path}", headers=HEADERS, params=params or {}, timeout=30)
        except Exception as e:
            print(f"  GET error {path}: {e}"); time.sleep(0.8); continue
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            time.sleep(1.0); continue
        if r.status_code == 204:
            return {}
        print(f"  GET {path} -> HTTP {r.status_code}")
        return None
    return None

def kpatch_leads(payloads):
    """PATCH /leads en lotes de 50 (limite de Kommo). Devuelve cuantos OK."""
    ok = 0
    for i in range(0, len(payloads), 50):
        batch = payloads[i:i+50]
        try:
            r = requests.patch(f"{BASE_URL}/leads", headers=HEADERS, json=batch, timeout=40)
        except Exception as e:
            print(f"  PATCH error: {e}"); continue
        if r.status_code in (200, 201, 204):
            ok += len(batch)
        else:
            print(f"  PATCH lote HTTP {r.status_code}: {r.text[:200]}")
        time.sleep(0.3)
    return ok

def get_lead_origins():
    """{lead_id: origin} desde talks (canal real)."""
    origins, page = {}, 1
    while page <= 30:
        d = kget("/talks", {"page": page, "limit": 250})
        if not d: break
        talks = d.get("_embedded", {}).get("talks", [])
        if not talks: break
        for t in talks:
            if t.get("entity_type") == "lead" and t.get("entity_id") and t["entity_id"] not in origins:
                origins[t["entity_id"]] = t.get("origin")
        if page >= d.get("_page_count", 1): break
        page += 1; time.sleep(0.2)
    return origins

def get_contact_map():
    """{contact_id: {nombre, tel, email}} para llenar datos deterministas."""
    cmap, page = {}, 1
    while page <= 40:
        d = kget("/contacts", {"page": page, "limit": 250})
        if not d: break
        cs = d.get("_embedded", {}).get("contacts", [])
        if not cs: break
        for c in cs:
            tel = email = None
            for f in c.get("custom_fields_values") or []:
                code = f.get("field_code")
                vals = f.get("values") or [{}]
                if code == "PHONE" and not tel:   tel   = vals[0].get("value")
                if code == "EMAIL" and not email: email = vals[0].get("value")
            cmap[c["id"]] = {"nombre": c.get("name"), "tel": tel, "email": email}
        if page >= d.get("_page_count", 1): break
        page += 1; time.sleep(0.2)
    return cmap

def get_all_leads():
    """Todos los leads (o ventana WINDOW_DAYS) con contactos y tags."""
    leads, page = [], 1
    params = {"limit": 250, "with": "contacts,tags", "order[id]": "desc"}
    if WINDOW_DAYS > 0:
        params["filter[created_at][from]"] = int((datetime.now() - timedelta(days=WINDOW_DAYS)).timestamp())
    while True:
        params["page"] = page
        d = kget("/leads", params)
        if not d: break
        batch = d.get("_embedded", {}).get("leads", [])
        if not batch: break
        leads += batch
        if page >= d.get("_page_count", 1): break
        page += 1; time.sleep(0.2)
    return leads

def get_lead_notes(lid):
    d = kget(f"/leads/{lid}/notes", {"limit": 50})
    if not d: return ""
    texts = []
    for n in d.get("_embedded", {}).get("notes", []):
        p = n.get("params", {})
        if p.get("text"): texts.append(p["text"])
    return "\n".join(texts)

def filled_field_ids(lead):
    """field_ids que YA tienen un valor real en el lead."""
    out = set()
    for f in lead.get("custom_fields_values") or []:
        vals = f.get("values") or []
        if vals and any(v.get("value") not in (None, "", False) for v in vals):
            out.add(f.get("field_id"))
    return out

def primary_contact_id(lead):
    cs = lead.get("_embedded", {}).get("contacts", [])
    if not cs: return None
    main = [c for c in cs if c.get("is_main")]
    return (main[0] if main else cs[0]).get("id")

def needs_ai(lead):
    have = filled_field_ids(lead)
    return any(fid not in have for fid in GATE_FIELDS)

# ───────────────────────── IA (Gemini) ─────────────────────────
SYSTEM_PROMPT = """Sos un clasificador de leads para SISERCOM Bolivia (cargadores para vehiculos electricos e instalacion electrica).
Te paso el contexto de un lead: etapa del pipeline, etiquetas, datos de contacto, campos actuales y notas/conversacion.
Devolve UN SOLO objeto JSON valido, sin texto extra ni markdown.

REGLAS IMPORTANTES:
- Campos CLAVE que NUNCA debes dejar vacios (elegi siempre la opcion mas probable): nivel_intencion, lead_score, tipo_cliente, producto_interes, proxima_accion, tipo_entrada, fuente_original.
  - Si no hay senales de empresa/flota/edificio, tipo_cliente = "Persona".
  - Si contactaron a una empresa de cargadores EV y no se especifica, producto_interes = "Cargador".
- ciudad y vehiculo: SOLO si aparecen en el contexto. Si no se mencionan, devolve "".
- Usa EXACTAMENTE estos valores (sin inventar otros):

ETAPAS Y SIGNIFICADO:
- Leads Entrantes / Contacto inicial = nuevo, sin calificar
- Solo informacion = baja intencion, solo consulta
- No interesado / Venta perdida = descartar (score 0, proxima_accion "Descartar")
- Interesado = hay interes, calificar
- Solicita visita = ALTA intencion
- Visita agendada / Cotizacion enviada = MUY ALTA intencion
- Pago anticipo / Saldo pendiente / Realizado = cliente confirmado

LEAD SCORE (0-100):
+30 etapa Solicita visita / Visita agendada / Cotizacion enviada
+20 etapa Interesado
+25 pide precio o cotizacion
+20 menciona urgencia (hoy, esta semana)
+20 ya tiene EV o esta por recibirlo
+15 empresa / flota / condominio
+10 viene por referido
-10 solo info        -15 no respondio
Venta perdida / No interesado = score 0

NIVEL: Alta=60+ | Media=30-59 | Baja=10-29 | No calificado=menos de 10
CANAL: si hay "Canal real de origen" en el contexto USA ESE. Opciones: WhatsApp / Instagram / Facebook / Formulario web / Referido / Llamada / Otro
TIPO ENTRADA: Organico / Pagado / Referido / Directo / Desconocido
TIPO CLIENTE: Persona / Empresa / Condominio/Edificio / Flota / Otro
PRODUCTO: Cargador / Instalacion / Venta+Instalacion / Solar / Baterias / Otro
PROXIMA ACCION: Enviar precio / Agendar visita / Llamar / Pedir datos / Seguimiento / Descartar
FUENTE ORIGINAL: texto corto que resuma de donde viene el lead (ej: "Anuncio Facebook", "WhatsApp directo", "Referido", "Formulario web").

Formato EXACTO de salida (un solo JSON):
{"canal_entrada":"","tipo_entrada":"","nivel_intencion":"","lead_score":0,"tipo_cliente":"","producto_interes":"","ciudad":"","vehiculo":"","proxima_accion":"","fuente_original":""}"""

def build_context(lead, real_canal, contacto):
    lid   = lead["id"]
    name  = lead.get("name", f"Lead #{lid}")
    stage = STAGES.get(lead.get("status_id"), "Desconocida")
    tags  = [t["name"] for t in lead.get("_embedded", {}).get("tags", [])]
    cfs   = {f["field_id"]: f for f in lead.get("custom_fields_values") or []}

    lines = [f"Lead: {name}", f"Etapa del pipeline: {stage}"]
    if real_canal:
        lines.append(f"Canal real de origen (Kommo talks): {real_canal}")
    if contacto:
        if contacto.get("nombre"): lines.append(f"Nombre de contacto: {contacto['nombre']}")
        if contacto.get("tel"):    lines.append(f"Telefono: {contacto['tel']}")
        if contacto.get("email"):  lines.append(f"Correo: {contacto['email']}")
    if tags:
        lines.append(f"Etiquetas: {', '.join(tags)}")
    for key, label in NAMES_FOR_CONTEXT.items():
        fid = CF[key]["id"]
        if fid in cfs:
            v = (cfs[fid].get("values") or [{}])[0].get("value", "")
            if v: lines.append(f"{label} (actual): {v}")
    notes = get_lead_notes(lid)
    if notes:
        lines.append(f"\nNotas del equipo / conversacion:\n{notes}")
    return "\n".join(lines), stage, tags

_model  = genai.GenerativeModel(MODEL, system_instruction=SYSTEM_PROMPT)
_GENCFG = {"response_mime_type": "application/json", "temperature": 0.2, "max_output_tokens": 500}

def classify_lead(context):
    prompt = f"CONTEXTO DEL LEAD:\n{context}\n\nDevolve SOLO el JSON de clasificacion."
    for attempt in range(2):
        try:
            resp = _model.generate_content(prompt, generation_config=_GENCFG)
            text = (resp.text or "").strip()
            if text.startswith("```"):
                text = text.split("```")[1]
                if text.startswith("json"):
                    text = text[4:]
            return json.loads(text.strip())
        except Exception as e:
            print(f"  Error IA intento {attempt+1}: {e}"); time.sleep(0.6)
    return None

# ───────────────────────── Construccion de payloads ─────────────────────────
def _sel(fields, key, value):
    if not value: return
    eid = CF[key].get("_norm", {}).get(_norm(value))
    if eid:
        fields.append({"field_id": CF[key]["id"], "values": [{"enum_id": eid}]})

def add_deterministic(lead, contacto):
    """Campos que NO requieren IA: ID, nombre, telefono x2, correo. Solo si estan vacios."""
    have = filled_field_ids(lead)
    lid  = lead["id"]
    fields = []
    if CF["id_sisercom"]["id"] not in have:
        fields.append({"field_id": CF["id_sisercom"]["id"], "values": [{"value": f"SISE-{lid:05d}"}]})
    nombre = lead.get("name")
    if not nombre or str(nombre).startswith("Lead #"):
        nombre = (contacto or {}).get("nombre")
    if nombre and not str(nombre).startswith("Lead #") and CF["nombre_cliente"]["id"] not in have:
        fields.append({"field_id": CF["nombre_cliente"]["id"], "values": [{"value": str(nombre)}]})
    tel = (contacto or {}).get("tel")
    if tel:
        if CF["telefono_cliente"]["id"] not in have:
            fields.append({"field_id": CF["telefono_cliente"]["id"], "values": [{"value": str(tel)}]})
        digits = "".join(ch for ch in str(tel) if ch.isdigit())
        if digits and CF["telefono_num"]["id"] not in have:
            fields.append({"field_id": CF["telefono_num"]["id"], "values": [{"value": int(digits)}]})
    email = (contacto or {}).get("email")
    if email and CF["correo"]["id"] not in have:
        fields.append({"field_id": CF["correo"]["id"], "values": [{"value": str(email)}]})
    return fields

def add_ai_fields(lead, c, real_canal):
    """Llena SOLO los campos vacios con la clasificacion de la IA. Devuelve (fields, canal_final)."""
    have = filled_field_ids(lead)
    fields = []
    def want(key): return CF[key]["id"] not in have

    canal = real_canal or c.get("canal_entrada") or "Otro"
    if want("canal_entrada"):    _sel(fields, "canal_entrada", canal)
    if want("tipo_entrada"):     _sel(fields, "tipo_entrada", c.get("tipo_entrada") or "Desconocido")
    if want("nivel_intencion"):  _sel(fields, "nivel_intencion", c.get("nivel_intencion") or "No calificado")
    if want("lead_score") and c.get("lead_score") is not None:
        try: fields.append({"field_id": CF["lead_score"]["id"], "values": [{"value": int(c["lead_score"])}]})
        except (TypeError, ValueError): pass
    if want("tipo_cliente"):     _sel(fields, "tipo_cliente", c.get("tipo_cliente") or "Persona")
    if want("producto_interes"): _sel(fields, "producto_interes", c.get("producto_interes") or "Otro")

    ciudad = (c.get("ciudad") or "").strip()
    if ciudad:
        if want("ciudad_texto"):
            fields.append({"field_id": CF["ciudad_texto"]["id"], "values": [{"value": ciudad}]})
        cl = _norm(ciudad)
        zona = "La Paz" if "paz" in cl else ("Santa Cruz" if "cruz" in cl else "Otra")
        if want("ciudad_zona"): _sel(fields, "ciudad_zona", zona)

    veh = (c.get("vehiculo") or "").strip()
    if veh and want("vehiculo"):
        fields.append({"field_id": CF["vehiculo"]["id"], "values": [{"value": veh}]})

    if want("proxima_accion"):   _sel(fields, "proxima_accion", c.get("proxima_accion") or "Seguimiento")

    if want("fuente_original"):
        fuente = (c.get("fuente_original") or "").strip() or f"{canal} (auto)"
        fields.append({"field_id": CF["fuente_original"]["id"], "values": [{"value": fuente}]})

    return fields, canal

def compute_tags(existing_tags, c, canal_final):
    new_tags = list(existing_tags)
    nivel   = c.get("nivel_intencion")
    accion  = c.get("proxima_accion")
    cliente = c.get("tipo_cliente")
    intention_tags = ["alta_intencion", "media_intencion", "baja_intencion", "no_calificado"]
    if nivel and NIVEL_TO_TAG.get(nivel):
        new_tags = [t for t in new_tags if t not in intention_tags]
        new_tags.append(NIVEL_TO_TAG[nivel])
    if accion and ACCION_TO_TAG.get(accion) and ACCION_TO_TAG[accion] not in new_tags:
        new_tags.append(ACCION_TO_TAG[accion])
    if cliente and CLIENTE_TO_TAG.get(cliente) and CLIENTE_TO_TAG[cliente] not in new_tags:
        new_tags.append(CLIENTE_TO_TAG[cliente])
    if not canal_final and "sin_origen" not in new_tags:
        new_tags.append("sin_origen")
    return new_tags

# ───────────────────────────── Run ─────────────────────────────
def run():
    print("=" * 60)
    print(f"Clasificador SISERCOM v5.0 (Gemini/{MODEL}) - {datetime.now():%Y-%m-%d %H:%M}")
    print(f"Ventana: {'todos' if WINDOW_DAYS==0 else str(WINDOW_DAYS)+' dias'} | Tope IA/corrida: {MAX_PER_RUN}")
    print("=" * 60)

    origins   = get_lead_origins();  print(f"Talks con origen identificado: {len(origins)}")
    contactos = get_contact_map();   print(f"Contactos mapeados: {len(contactos)}")
    leads     = get_all_leads();     print(f"Leads en ventana: {len(leads)}")

    # ── FASE 1: campos deterministas (sin IA) para TODOS ──
    det_payloads = []
    for lead in leads:
        contacto = contactos.get(primary_contact_id(lead))
        f = add_deterministic(lead, contacto)
        if f:
            det_payloads.append({"id": lead["id"], "custom_fields_values": f})
    print(f"\nFASE 1 (determinista: ID/nombre/telefono/correo): {len(det_payloads)} leads")
    if det_payloads:
        print(f"  -> {kpatch_leads(det_payloads)} actualizados")

    # ── FASE 2: clasificacion IA solo para leads con campos clave faltantes ──
    pendientes = [l for l in leads if needs_ai(l)]
    print(f"\nFASE 2 (IA): {len(pendientes)} leads con campos clave faltantes (procesando hasta {MAX_PER_RUN})")
    ai_payloads, done, skipped = [], 0, 0
    for lead in pendientes[:MAX_PER_RUN]:
        lid  = lead["id"]
        name = lead.get("name", f"Lead #{lid}")
        contacto   = contactos.get(primary_contact_id(lead))
        origin     = origins.get(lid)
        real_canal = ORIGIN_TO_CANAL.get(origin) if origin else None
        ctx, stage, tags = build_context(lead, real_canal, contacto)
        c = classify_lead(ctx)
        if not c:
            skipped += 1
            print(f"  [skip] {name} (#{lid}) sin respuesta IA")
            continue
        fields, canal = add_ai_fields(lead, c, real_canal)
        new_tags = compute_tags(tags, c, canal)
        payload = {"id": lid}
        if fields:
            payload["custom_fields_values"] = fields
        if set(new_tags) != set(tags):
            payload["_embedded"] = {"tags": [{"name": t} for t in new_tags]}
        if len(payload) > 1:
            ai_payloads.append(payload); done += 1
            print(f"  [{done}] {name} (#{lid}) {stage} -> {c.get('nivel_intencion')} "
                  f"score {c.get('lead_score')} | {len(fields)} campos")
        time.sleep(0.2)

    print(f"\nEscribiendo {len(ai_payloads)} leads clasificados...")
    if ai_payloads:
        print(f"  -> {kpatch_leads(ai_payloads)} actualizados")

    restantes = max(0, len(pendientes) - MAX_PER_RUN)
    print("\n" + "=" * 60)
    print(f"RESUMEN: deterministas={len(det_payloads)} | IA clasificados={done} | "
          f"IA sin respuesta={skipped} | pendientes proxima corrida={restantes}")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    run()
