"""
SISERCOM - Clasificador automatico de leads con IA
Gemini 2.5 Flash + Kommo API. Corre en GitHub Actions 8am Bolivia.
v2 - Lee conversaciones de WhatsApp ademas de notas
"""
import os, json, time, requests
import google.generativeai as genai
from datetime import datetime, timedelta

KOMMO_TOKEN = os.environ["KOMMO_TOKEN"]
GEMINI_KEY  = os.environ["GEMINI_API_KEY"]
BASE_URL    = "https://dcisnerossisercomevcom.kommo.com/api/v4"
HEADERS     = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

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

def get_unclassified_leads():
    cutoff = int((datetime.now() - timedelta(days=60)).timestamp())
    leads, page = [], 1
    while True:
        r = requests.get(f"{BASE_URL}/leads", headers=HEADERS, params={
            "page": page, "limit": 250, "with": "contacts",
            "order[id]": "desc",
            "filter[created_at][from]": cutoff
        })
        if r.status_code != 200:
            print(f"  Error Kommo leads: {r.status_code}")
            break
        data = r.json()
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
            texts.append(f"[nota] {p['text']}")
        if p.get("address"):
            texts.append(f"[ubicacion] {p['address']}")
    return "\n".join(texts)

def get_lead_talk_id(lead):
    """Obtiene el talk_id del lead desde los eventos recientes."""
    lid = lead["id"]
    r = requests.get(f"{BASE_URL}/events", headers=HEADERS, params={
        "filter[entity_id][]": lid,
        "filter[type][]": ["incoming_chat_message", "outgoing_chat_message"],
        "limit": 1,
        "order[created_at]": "desc"
    })
    if r.status_code != 200:
        return None
    events = r.json().get("_embedded", {}).get("events", [])
    if not events:
        return None
    msg = events[0].get("value_after", [{}])[0].get("message", {})
    return msg.get("talk_id")

def get_talk_messages(talk_id):
    """Lee los mensajes de WhatsApp/Instagram del talk. Requiere scope 'chats'."""
    r = requests.get(f"{BASE_URL}/talks/{talk_id}/messages", headers=HEADERS, params={"limit": 50})
    if r.status_code == 403:
        return None, "scope_missing"
    if r.status_code != 200:
        return None, f"error_{r.status_code}"
    messages = r.json().get("_embedded", {}).get("messages", [])
    lines = []
    for m in messages:
        sender = "Cliente" if m.get("author", {}).get("type") == "contact" else "Equipo"
        content = m.get("content", {})
        text = content.get("text") or content.get("type", "")
        if text:
            lines.append(f"{sender}: {text}")
    return "\n".join(lines), "ok"

def get_all_context(lead):
    """
    Combina notas + conversacion de WhatsApp para el prompt.
    Si no hay scope de chats, cae graciosamente a solo notas.
    """
    lid = lead["id"]
    notes = get_lead_notes(lid)

    talk_id = get_lead_talk_id(lead)
    conversation = ""
    chat_status = "no_talk"

    if talk_id:
        messages, chat_status = get_talk_messages(talk_id)
        if messages:
            conversation = messages
        elif chat_status == "scope_missing":
            print(f"    ! Sin scope 'chats' -- solo usando notas")

    parts = []
    if notes:
        parts.append(f"=== NOTAS DEL EQUIPO ===\n{notes}")
    if conversation:
        parts.append(f"=== CONVERSACION WHATSAPP/INSTAGRAM ===\n{conversation}")

    return "\n\n".join(parts) if parts else "", chat_status

def update_lead_fields(lid, c):
    fields = []
    def sel(k, v):
        if not v: return
        eid = CF[k].get("enums", {}).get(v)
        if eid: fields.append({"field_id": CF[k]["id"], "values": [{"enum_id": eid}]})
    def txt(k, v):
        if v: fields.append({"field_id": CF[k]["id"], "values": [{"value": str(v)}]})
    def num(k, v):
        if v is not None: fields.append({"field_id": CF[k]["id"], "values": [{"value": int(v)}]})
    sel("canal_entrada",    c.get("canal_entrada"))
    sel("tipo_entrada",     c.get("tipo_entrada"))
    sel("nivel_intencion",  c.get("nivel_intencion"))
    num("lead_score",       c.get("lead_score"))
    sel("tipo_cliente",     c.get("tipo_cliente"))
    sel("producto_interes", c.get("producto_interes"))
    txt("ciudad_zona",      c.get("ciudad_zona"))
    txt("vehiculo",         c.get("vehiculo"))
    sel("proxima_accion",   c.get("proxima_accion"))
    txt("fuente_original",  c.get("fuente_original"))
    if not fields:
        return True
    r = requests.patch(f"{BASE_URL}/leads", headers=HEADERS,
                       json=[{"id": lid, "custom_fields_values": fields}])
    return r.status_code == 200

PROMPT = """Sos clasificador de leads para SISERCOM Bolivia (vehiculos electricos y cargadores EV).
Analizas el nombre del lead, sus notas y la conversacion de WhatsApp/Instagram.
Devuelve SOLO el JSON, sin texto extra ni markdown.

LEAD SCORE (0-100) -- suma los puntos que apliquen:
+25 pide precio, cotizacion o presupuesto
+20 solicita visita tecnica o instalacion
+20 menciona urgencia: hoy, esta semana, lo antes posible
+20 ya tiene EV o esta por recibirlo pronto
+15 es empresa, flota, condominio o edificio
+10 viene por referido
-10 solo pide informacion general sin compromiso
-15 no respondio despues de seguimiento

NIVEL DE INTENCION -- basate SIEMPRE en el score:
Alta = score 60 o mas
Media = score 30 a 59
Baja = score 10 a 29
No calificado = score menos de 10 o sin datos suficientes

CANAL: WhatsApp / Instagram / Facebook / Formulario web / Referido / Llamada / Otro
TIPO ENTRADA: Organico / Pagado / Referido / Directo / Desconocido
TIPO CLIENTE: Persona / Empresa / Condominio-Edificio / Flota / Otro
PRODUCTO: Cargador / Instalacion / Venta+Instalacion / Solar / Baterias / Otro
PROXIMA ACCION:
Enviar precio    = tiene EV y pide cotizacion
Agendar visita   = quiere visita tecnica o instalacion
Llamar           = hay que calificar por telefono
Pedir datos      = falta info clave (vehiculo, zona, etc)
Seguimiento      = ya hubo contacto, retomar
Descartar        = fuera de alcance o no califica

JSON: {"canal_entrada":"","tipo_entrada":"","nivel_intencion":"","lead_score":0,"tipo_cliente":"","producto_interes":"","ciudad_zona":"","vehiculo":"","proxima_accion":"","fuente_original":""}"""

def classify_lead(name, context, cinfo=""):
    prompt = f"{PROMPT}\n\nLead: {name}\nContacto: {cinfo}\n\n{context or '(sin informacion disponible)'}\n\nClasifica:"
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
    print(f"SISERCOM Clasificador v2 - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*55}")

    leads = get_unclassified_leads()
    print(f"\nLeads sin clasificar (ultimos 60 dias): {len(leads)}")
    if not leads:
        print("Nada nuevo para clasificar.")
        return

    ok = errors = scope_warnings = 0

    for i, lead in enumerate(leads, 1):
        lid  = lead["id"]
        name = lead.get("name", f"Lead #{lid}")
        contacts = lead.get("_embedded", {}).get("contacts", [])
        cinfo = ", ".join([c.get("name", "") for c in contacts if c.get("name")])
        print(f"\n[{i}/{len(leads)}] {name} (#{lid})")

        context, chat_status = get_all_context(lead)

        has_convo = "CONVERSACION" in context
        has_notes = "NOTAS" in context
        src = []
        if has_notes:    src.append("notas")
        if has_convo:    src.append("WhatsApp")
        if chat_status == "scope_missing": scope_warnings += 1
        print(f"  Fuentes: {', '.join(src) if src else 'ninguna'}")

        result = classify_lead(name, context, cinfo)
        if not result:
            errors += 1
            continue

        print(f"  {result.get('nivel_intencion')} | score:{result.get('lead_score')} | {result.get('proxima_accion')}")
        if update_lead_fields(lid, result):
            ok += 1
            print(f"  OK")
        else:
            errors += 1
            print(f"  ERROR al guardar")

        time.sleep(0.5)

    print(f"\n{'='*55}")
    print(f"RESUMEN: {ok} OK | {errors} errores | {len(leads)} procesados")
    if scope_warnings:
        print(f"AVISO: {scope_warnings} leads sin scope 'chats' -- actualizar token en GitHub Secrets")
    print(f"{'='*55}\n")

if __name__ == "__main__":
    run()
