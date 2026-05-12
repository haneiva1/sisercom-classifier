"""
SISERCOM - Clasificador automatico de leads con IA
Gemini (gratuito) + Kommo API. Corre en Render 8am Bolivia.
"""
import os, json, time, requests
import google.generativeai as genai
from datetime import datetime

KOMMO_TOKEN = os.environ["KOMMO_TOKEN"]
GEMINI_KEY  = os.environ["GEMINI_API_KEY"]
BASE_URL    = "https://dcisnerossisercomevcom.kommo.com/api/v4"
HEADERS     = {"Authorization": f"Bearer {KOMMO_TOKEN}", "Content-Type": "application/json"}

genai.configure(api_key=GEMINI_KEY)
model = genai.GenerativeModel("gemini-2.5-flash-preview-05-20")

CF = {    "canal_entrada":    {"id": 487630, "enums": {"WhatsApp":363850,"Instagram":363852,"Facebook":363854,"Formulario web":363856,"Referido":363858,"Llamada":363860,"Otro":363862}},
    "tipo_entrada":     {"id": 487632, "enums": {"Organico":363864,"Pagado":363866,"Referido":363868,"Directo":363870,"Desconocido":363872}},
    "nivel_intencion":  {"id": 487654, "enums": {"Alta":363906,"Media":363908,"Baja":363910,"No calificado":363912}},
    "lead_score":       {"id": 487656},
    "tipo_cliente":     {"id": 487676, "enums": {"Persona":363914,"Empresa":363916,"Condominio/Edificio":363918,"Flota":363920,"Otro":363922}},
    "producto_interes": {"id": 487678, "enums": {"Cargador":363924,"Instalacion":363926,"Venta+Instalacion":363928,"Solar":363930,"Baterias":363932,"Otro":363934}},
    "ciudad_zona":      {"id": 487680},
    "vehiculo":         {"id": 487684},
    "proxima_accion":   {"id": 487696, "enums": {"Enviar precio":363946,"Agendar visita":363948,"Llamar":363950,"Pedir datos":363952,"Seguimiento":363954,"Descartar":363956}},
    "fuente_original":  {"id": 487698},
}

def get_unclassified_leads():
    leads, page = [], 1
    while True:
        r = requests.get(f"{BASE_URL}/leads", headers=HEADERS, params={"page":page,"limit":250,"with":"contacts"})
        if r.status_code != 200: break
        data = r.json()
        batch = data.get("_embedded", {}).get("leads", [])
        if not batch: break
        for lead in batch:
            cfs = {cf["field_id"]: cf for cf in lead.get("custom_fields_values") or []}
            if CF["nivel_intencion"]["id"] not in cfs:
                leads.append(lead)
        if page >= data.get("_page_count", 1): break
        page += 1
        time.sleep(0.2)
    return leads

def get_lead_notes(lid):
    r = requests.get(f"{BASE_URL}/leads/{lid}/notes", headers=HEADERS, params={"limit":50})
    if r.status_code != 200: return ""
    texts = []
    for n in r.json().get("_embedded", {}).get("notes", []):
        p = n.get("params", {})
        if p.get("text"): texts.append(f"[{n.get('note_type','')}] {p['text']}")
        if p.get("address"): texts.append(f"[ubicacion] {p['address']}")
    return "\n".join(texts)

def update_lead_fields(lid, c):
    fields = []
    def sel(k, v):
        if not v: return
        eid = CF[k].get("enums",{}).get(v)
        if eid: fields.append({"field_id":CF[k]["id"],"values":[{"enum_id":eid}]})
    def txt(k, v):
        if v: fields.append({"field_id":CF[k]["id"],"values":[{"value":str(v)}]})
    def num(k, v):
        if v is not None: fields.append({"field_id":CF[k]["id"],"values":[{"value":int(v)}]})
    sel("canal_entrada",c.get("canal_entrada")); sel("tipo_entrada",c.get("tipo_entrada"))
    sel("nivel_intencion",c.get("nivel_intencion")); num("lead_score",c.get("lead_score"))
    sel("tipo_cliente",c.get("tipo_cliente")); sel("producto_interes",c.get("producto_interes"))
    txt("ciudad_zona",c.get("ciudad_zona")); txt("vehiculo",c.get("vehiculo"))
    sel("proxima_accion",c.get("proxima_accion")); txt("fuente_original",c.get("fuente_original"))
    if not fields: return True
    r = requests.patch(f"{BASE_URL}/leads", headers=HEADERS, json=[{"id":lid,"custom_fields_values":fields}])
    return r.status_code == 200

PROMPT = """Clasificador de leads SISERCOM Bolivia (EVs/cargadores).
Analiza notas Kommo. Devuelve SOLO JSON sin texto extra.
Canal: WhatsApp/Instagram/Facebook/Formulario web/Referido/Llamada/Otro
Tipo entrada: Organico/Pagado/Referido/Directo/Desconocido
Nivel intencion: Alta(precio/visita/urgencia), Media(interes), Baja(curiosidad), No calificado
Lead score 0-100: +25 pide precio, +20 visita, +20 urgencia, +20 tiene EV, +15 empresa, +10 referido, -10 info, -15 no responde
Tipo cliente: Persona/Empresa/Condominio-Edificio/Flota/Otro
Producto: Cargador/Instalacion/Venta+Instalacion/Solar/Baterias/Otro
Proxima accion: Enviar precio/Agendar visita/Llamar/Pedir datos/Seguimiento/Descartar
JSON: {"canal_entrada":"","tipo_entrada":"","nivel_intencion":"","lead_score":0,"tipo_cliente":"","producto_interes":"","ciudad_zona":"","vehiculo":"","proxima_accion":"","fuente_original":""}"""

def classify_lead(name, notes, cinfo=""):
    prompt = f"{PROMPT}\nLead: {name}\nContacto: {cinfo}\nNotas:\n{notes or '(sin notas)'}\nClasifica:"
    try:
        resp = model.generate_content(prompt)
        text = resp.text.strip()
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"): text = text[4:]
        return json.loads(text.strip())
    except Exception as e:
        print(f"  Error: {e}"); return None

def run():
    print(f"\n{'='*50}\nSISERCOM Clasificador - {datetime.now().strftime('%Y-%m-%d %H:%M')}\n{'='*50}")
    leads = get_unclassified_leads()
    print(f"Leads sin clasificar: {len(leads)}")
    if not leads: print("Nada nuevo."); return
    ok = errors = 0
    for i, lead in enumerate(leads, 1):
        lid = lead["id"]
        name = lead.get("name", f"Lead #{lid}")
        contacts = lead.get("_embedded", {}).get("contacts", [])
        cinfo = ", ".join([c.get("name","") for c in contacts if c.get("name")])
        print(f"\n[{i}/{len(leads)}] {name}")
        notes = get_lead_notes(lid)
        result = classify_lead(name, notes, cinfo)
        if not result: errors += 1; continue
        print(f"  {result.get('nivel_intencion')} | score:{result.get('lead_score')} | {result.get('proxima_accion')}")
        if update_lead_fields(lid, result): ok += 1; print("  OK")
        else: errors += 1
        time.sleep(0.5)
    print(f"\nRESUMEN: {ok} OK | {errors} errores | {len(leads)} total\n")

if __name__ == "__main__":
    run()
