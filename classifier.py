"""
SISERCOM - Clasificador automatico de leads con IA
v4.2 - gemini-2.5-flash-lite + canal real (talks) + tags automaticos
"""
import os, json, time, requests
import google.generativeai as genai
from datetime import datetime, timedelta

KOMMO_TOKEN = os.environ["KOMMO_TOKEN"]
GEMINI_KEY  = os.environ["GEMINI_API_KEY"]
BASE_URL    = "https://dcisnerossisercomevcom.kommo.com/api/v4"
HEADERS     = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-2.5-flash-lite")

STAGES = {
    105087411: "Leads Entrantes",
    105087415: "Contacto inicial",
    105111343: "Interesado",
    105124671: "Solicita visita",
    105087419: "Visita agendada",
    105087423: "Cotización enviada",
    105122979: "Pago anticipo",
    105122983: "Saldo pendiente",
    105244923: "Otros intereses en VE",
    105130443: "Solo información",
    105122735: "No interesado",
    142: "Realizado (ganado)",
    143: "Venta perdida",
}

# Mapeo de origin de Kommo talks -> canal_entrada
ORIGIN_TO_CANAL = {
    "waba": "WhatsApp",
    "whatsapp": "WhatsApp",
    "facebook": "Facebook",
    "instagram_business": "Instagram",
    "instagram": "Instagram",
}

CF = {
    "canal_entrada":   {"id": 487630, "enums": {"WhatsApp":363850,"Instagram":363852,"Facebook":363854,"Formulario web":363856,"Referido":363858,"Llamada":363860,"Otro":363862}},
    "tipo_entrada":    {"id": 487632, "enums": {"Organico":363864,"Pagado":363866,"Referido":363868,"Directo":363870,"Desconocido":363872}},
    "nivel_intencion": {"id": 487654, "enums": {"Alta":363906,"Media":363908,"Baja":363910,"No calificado":363912}},
    "lead_score":      {"id": 487656},
    "tipo_cliente":    {"id": 487676, "enums": {"Persona":363914,"Empresa":363916,"Condominio/Edificio":363918,"Flota":363920,"Otro":363922}},
    "producto_interes":{"id": 487678, "enums": {"Cargador":363924,"Instalacion":363926,"Venta+Instalacion":363928,"Solar":363930,"Baterias":363932,"Otro":363934}},
    "ciudad_zona":     {"id": 487680},
    "vehiculo":        {"id": 487684},
    "proxima_accion":  {"id": 487696, "enums": {"Enviar precio":363946,"Agendar visita":363948,"Llamar":363950,"Pedir datos":363952,"Seguimiento":363954,"Descartar":363956}},
    "fuente_original": {"id": 487698},
}
CF_NAMES = {487630:"Canal", 487632:"Tipo entrada", 487654:"Nivel", 487656:"Score", 487676:"Tipo cliente", 487678:"Producto", 487680:"Ciudad", 487684:"Vehiculo", 487696:"Proxima accion"}

# Tags base segun nivel de intencion
NIVEL_TO_TAG = {
    "Alta": "alta_intencion",
    "Media": "media_intencion",
    "Baja": "baja_intencion",
    "No calificado": "no_calificado",
}

# Tags adicionales segun proxima accion
ACCION_TO_TAG = {
    "Agendar visita": "solicita_visita",
    "Enviar precio": "solicita_precio",
    "Seguimiento": "no_respondio",
}

# Tags adicionales segun tipo de cliente
CLIENTE_TO_TAG = {
    "Empresa": "cliente_b2b",
    "Flota": "interes_ev_flota",
    "Condominio/Edificio": "cliente_b2b",
}

def get_lead_origins():
    """Fetches all talks and returns {lead_id: origin} for source-of-truth channel."""
    origins = {}
    page = 1
    while page <= 20:
        r = requests.get(f"{BASE_URL}/talks", headers=HEADERS, params={"page": page, "limit": 250})
        if r.status_code != 200:
            break
        data = r.json()
        talks = data.get("_embedded", {}).get("talks", [])
        if not talks:
            break
        for t in talks:
            if t.get("entity_type") == "lead" and t.get("entity_id") and t["entity_id"] not in origins:
                origins[t["entity_id"]] = t.get("origin")
        page += 1
        time.sleep(0.2)
    return origins

def get_unclassified_leads():
    """Leads sin nivel_intencion en los ultimos 60 dias."""
    leads = []
    page = 1
    cutoff = int((datetime.now() - timedelta(days=60)).timestamp())
    while True:
        r = requests.get(f"{BASE_URL}/leads", headers=HEADERS, params={
            "page": page, "limit": 250,
            "with": "contacts,tags",
            "order[id]": "desc",
            "filter[created_at][from]": cutoff
        })
        if r.status_code != 200:
            print(f"  Error Kommo: {r.status_code}")
            break
        data  = r.json()
        batch = data.get("_embedded", {}).get("leads", [])
        if not batch:
            break
        for lead in batch:
            cfs = {cf["field_id"]: cf for cf in lead.get("custom_fields_values") or []}
            if CF["nivel_intencion"]["id"] not in cfs:
                leads.append(lead)
        if page >= data.get("_page_count", 1):
            break
        page += 1
        time.sleep(0.2)
    return leads

def get_lead_notes(lid):
    r = requests.get(f"{BASE_URL}/leads/{lid}/notes", headers=HEADERS, params={"limit": 50})
    if r.status_code != 200:
        return ""
    texts = []
    for n in r.json().get("_embedded", {}).get("notes", []):
        p = n.get("params", {})
        if p.get("text"):
            texts.append(p["text"])
    return "\n".join(texts)

def build_context(lead, real_canal=None):
    lid   = lead["id"]
    name  = lead.get("name", f"Lead #{lid}")
    stage = STAGES.get(lead.get("status_id"), "Desconocida")
    tags  = [t["name"] for t in lead.get("_embedded", {}).get("tags", [])]
    cfs   = {cf["field_id"]: cf for cf in lead.get("custom_fields_values") or []}

    lines = [f"Lead: {name}", f"Etapa del pipeline: {stage}"]
    if real_canal:
        lines.append(f"Canal real de origen (Kommo): {real_canal}")
    if tags:
        lines.append(f"Etiquetas: {', '.join(tags)}")
    for fid, fname in CF_NAMES.items():
        if fid in cfs:
            val = cfs[fid]["values"][0].get("value", "")
            if val:
                lines.append(f"{fname}: {val}")
    notes = get_lead_notes(lid)
    if notes:
        lines.append(f"\nNotas del equipo:\n{notes}")
    return "\n".join(lines), stage, tags

def update_lead_fields(lid, c, existing_tags, real_canal=None):
    fields = []
    def sel(k, v):
        if not v: return
        eid = CF[k].get("enums", {}).get(v)
        if eid: fields.append({"field_id": CF[k]["id"], "values": [{"enum_id": eid}]})
    def txt(k, v):
        if v: fields.append({"field_id": CF[k]["id"], "values": [{"value": str(v)}]})
    def num(k, v):
        if v is not None: fields.append({"field_id": CF[k]["id"], "values": [{"value": int(v)}]})
    
    # CANAL: usar el real del talk si esta disponible, sino el que la IA infiere
    canal_final = real_canal or c.get("canal_entrada")
    sel("canal_entrada",    canal_final)
    sel("tipo_entrada",     c.get("tipo_entrada"))
    sel("nivel_intencion",  c.get("nivel_intencion"))
    num("lead_score",       c.get("lead_score"))
    sel("tipo_cliente",     c.get("tipo_cliente"))
    sel("producto_interes", c.get("producto_interes"))
    txt("ciudad_zona",      c.get("ciudad_zona"))
    txt("vehiculo",         c.get("vehiculo"))
    sel("proxima_accion",   c.get("proxima_accion"))
    txt("fuente_original",  c.get("fuente_original"))
    
    # TAGS: construir lista basada en clasificacion
    new_tags = list(existing_tags)
    nivel = c.get("nivel_intencion")
    accion = c.get("proxima_accion")
    cliente = c.get("tipo_cliente")
    
    intention_tags = ["alta_intencion", "media_intencion", "baja_intencion", "no_calificado"]
    
    # Tag de nivel (reemplaza cualquier tag de nivel previo)
    if nivel and NIVEL_TO_TAG.get(nivel):
        new_tags = [t for t in new_tags if t not in intention_tags]
        new_tags.append(NIVEL_TO_TAG[nivel])
    
    # Tag de accion (si no existe ya)
    if accion and ACCION_TO_TAG.get(accion):
        accion_tag = ACCION_TO_TAG[accion]
        if accion_tag not in new_tags:
            new_tags.append(accion_tag)
    
    # Tag de cliente (si es B2B/Flota)
    if cliente and CLIENTE_TO_TAG.get(cliente):
        cliente_tag = CLIENTE_TO_TAG[cliente]
        if cliente_tag not in new_tags:
            new_tags.append(cliente_tag)
    
    # Si no hay canal -> tag sin_origen
    if not canal_final and "sin_origen" not in new_tags:
        new_tags.append("sin_origen")
    
    # Solo enviar update si hay cambios
    payload = {"id": lid}
    if fields:
        payload["custom_fields_values"] = fields
    if set(new_tags) != set(existing_tags):
        payload["_embedded"] = {"tags": [{"name": t} for t in new_tags]}
    
    if len(payload) == 1:
        return True
    r = requests.patch(f"{BASE_URL}/leads", headers=HEADERS, json=[payload])
    return r.status_code == 200

PROMPT = """Sos clasificador de leads para SISERCOM Bolivia (vehiculos electricos y cargadores EV).
Recibis el contexto completo: etapa del pipeline, etiquetas, campos y notas.
Devuelve SOLO el JSON, sin texto extra ni markdown.

ETAPAS Y SU SIGNIFICADO:
- Leads Entrantes / Contacto inicial = nuevo, sin calificar
- Solo informacion = baja intencion, solo consulta
- No interesado = descartar
- Interesado = hay interes, calificar
- Solicita visita = ALTA intencion
- Visita agendada = MUY ALTA intencion
- Cotizacion enviada = MUY ALTA intencion
- Pago anticipo / Saldo pendiente = cliente confirmado
- Realizado = ganado | Venta perdida = perdido

LEAD SCORE (0-100):
+30 etapa Solicita visita / Visita agendada / Cotizacion enviada
+20 etapa Interesado
+25 pide precio o cotizacion en notas
+20 menciona urgencia: hoy, esta semana
+20 ya tiene EV o esta por recibirlo
+15 es empresa, flota, condominio
+10 viene por referido
-10 solo info
-15 no respondio
Etapa Venta perdida o No interesado = score 0, accion Descartar

NIVEL: Alta=60+ | Media=30-59 | Baja=10-29 | No calificado=menos de 10
CANAL: si hay "Canal real de origen" en contexto USA ESE. Sino: WhatsApp / Instagram / Facebook / Formulario web / Referido / Llamada / Otro
TIPO ENTRADA: Organico / Pagado / Referido / Directo / Desconocido
TIPO CLIENTE: Persona / Empresa / Condominio-Edificio / Flota / Otro
PRODUCTO: Cargador / Instalacion / Venta+Instalacion / Solar / Baterias / Otro
PROXIMA ACCION: Enviar precio | Agendar visita | Llamar | Pedir datos | Seguimiento | Descartar

JSON: {"canal_entrada":"","tipo_entrada":"","nivel_intencion":"","lead_score":0,"tipo_cliente":"","producto_interes":"","ciudad_zona":"","vehiculo":"","proxima_accion":"","fuente_original":""}"""

def classify_lead(context):
    prompt = f"{PROMPT}\n\nCONTEXTO:\n{context}\n\nClasifica:"
    try:
        resp = model.generate_content(prompt)
        text = resp.text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        print(f"  Error Gemini: {e}")
        return None

def run():
    print(f"\n{'='*55}")
    print(f"SISERCOM Clasificador v4.2 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Fuentes: etapa + etiquetas + campos + notas + canal real (talks)")
    print(f"Acciones: campos + tags automaticos por nivel/accion/cliente")
    print(f"{'='*55}")
    
    print("\nObteniendo origenes reales de talks...")
    origins = get_lead_origins()
    print(f"  {len(origins)} leads con origen identificado")
    
    leads = get_unclassified_leads()
    print(f"\nLeads sin clasificar (ultimos 60 dias): {len(leads)}")
    if not leads:
        print("Nada nuevo para clasificar.")
        return
    
    ok = errors = 0
    for i, lead in enumerate(leads, 1):
        name = lead.get("name", f"Lead #{lead['id']}")
        lid = lead["id"]
        existing_tags = [t["name"] for t in lead.get("_embedded", {}).get("tags", [])]
        
        # Canal real del talk de Kommo
        origin = origins.get(lid)
        real_canal = ORIGIN_TO_CANAL.get(origin) if origin else None
        
        print(f"\n[{i}/{len(leads)}] {name} (#{lid})")
        context, stage, tags = build_context(lead, real_canal=real_canal)
        print(f"  Etapa: {stage} | Canal real: {real_canal or 'N/A'} | Tags: {tags or 'ninguna'}")
        result = classify_lead(context)
        if not result:
            errors += 1
            continue
        print(f"  -> {result.get('nivel_intencion')} | score:{result.get('lead_score')} | {result.get('proxima_accion')}")
        if update_lead_fields(lid, result, existing_tags, real_canal=real_canal):
            ok += 1
            print(f"  OK")
        else:
            errors += 1
            print(f"  ERROR")
        time.sleep(0.4)
    print(f"\n{'='*55}")
    print(f"RESUMEN: {ok} OK | {errors} errores | {len(leads)} procesados")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    run()
