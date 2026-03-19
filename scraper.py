"""
Booking.com Price Scraper — Hygge Haus VLA
Tier 1: competencia directa (3 reintentos, pausas largas)
Tier 2: referencia de mercado (1 intento)
Resultado → Google Sheets con vistas diferenciadas
"""

import re
import time
import random
import json
import logging
from datetime import datetime, timedelta

import requests
import gspread
from google.oauth2.service_account import Credentials

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── CONFIGURACIÓN ────────────────────────────────────────────────────────────

SPREADSHEET_NAME  = "Precios Alojamientos Patagonia — Hygge Haus"
CHECKIN_NIGHTS    = 2
MAX_RETRIES_T1    = 3
MAX_RETRIES_T2    = 1
PAUSE_MIN_T1      = 4.0
PAUSE_MAX_T1      = 8.0
PAUSE_MIN_T2      = 2.0
PAUSE_MAX_T2      = 4.5


# ─── FECHAS OBJETIVO ─────────────────────────────────────────────────────────

def get_target_dates():
    """Primer viernes de cada uno de los próximos 5 meses (estadía viernes-domingo)."""
    dates = []
    today = datetime.today()
    for i in range(1, 6):
        month = (today.month + i - 1) % 12 + 1
        year  = today.year + (today.month + i - 1) // 12
        first = datetime(year, month, 1)
        days_to_friday = (4 - first.weekday()) % 7
        friday = first + timedelta(days=days_to_friday)
        dates.append((
            friday.strftime("%Y-%m-%d"),
            (friday + timedelta(days=CHECKIN_NIGHTS)).strftime("%Y-%m-%d")
        ))
    return dates


# ─── LISTA DE ALOJAMIENTOS ────────────────────────────────────────────────────

ALOJAMIENTOS = [
    # ── TIER 1 — Competencia directa ─────────────────────────────────────────
    {"id": 1,   "nombre": "Hygge Haus",               "slug": "hygge-haus",                                           "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 2,   "nombre": "Hostel La Angostura",       "slug": "hostel-la-angostura",                                  "localidad": "VLA",         "tipo": "Hostel",   "tier": 1},
    {"id": 3,   "nombre": "Acceso Bayo",               "slug": "acceso-bayo",                                          "localidad": "VLA",         "tipo": "Hosteria", "tier": 1},
    {"id": 4,   "nombre": "B&B Don Ciro",              "slug": "b-amp-b-don-ciro",                                     "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 5,   "nombre": "Huerta de Los Andes",       "slug": "huerta-de-los-andes-bed-and-breakfast",                "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 6,   "nombre": "Paso Puyehue",              "slug": "paso-puyehue",                                         "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 7,   "nombre": "Hostería Bajo Cero",        "slug": "hostel-bajo-cero",                                     "localidad": "VLA",         "tipo": "Hosteria", "tier": 1},
    {"id": 8,   "nombre": "Alehue Casa de Montaña",    "slug": "alehue-casa-de-montana",                               "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 9,   "nombre": "Hostería Epulen",           "slug": "hosteria-epulen",                                      "localidad": "VLA",         "tipo": "Hosteria", "tier": 1},
    {"id": 11,  "nombre": "Dodo House",                "slug": "dodo-house",                                           "localidad": "VLA",         "tipo": "Hosteria", "tier": 1},
    {"id": 12,  "nombre": "Hotel Angostura",           "slug": "angostura",                                            "localidad": "VLA",         "tipo": "Hotel",    "tier": 1},
    {"id": 21,  "nombre": "Ubuntu",                    "slug": "ubuntu-villa-la-angostura",                            "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 25,  "nombre": "Arcano Casa Montaña",       "slug": "arcano-casa-montana-puerto-manzano-villa-la-angstura", "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 78,  "nombre": "Como en Casa",              "slug": "b-amp-b-como-en-casa",                                 "localidad": "VLA",         "tipo": "B&B",      "tier": 1},
    {"id": 79,  "nombre": "Posta de los Colonos",      "slug": "hosteria-posta-de-los-colonos",                        "localidad": "VLA",         "tipo": "Hosteria", "tier": 1},
    {"id": 75,  "nombre": "El Mainten Escondido",      "slug": "hosteria-maiten-escondido",                            "localidad": "VLA",         "tipo": "Hosteria", "tier": 1},
    {"id": 16,  "nombre": "Hosteria Traunco",          "slug": "hosteria-traunco",                                     "localidad": "VLA",         "tipo": "Hosteria", "tier": 1},
    {"id": 115, "nombre": "Tres amapolas",             "slug": "tres-amapolas-bariloche-bariloche",                    "localidad": "Bariloche",   "tipo": "B&B",      "tier": 1},
    {"id": 116, "nombre": "Selva India",               "slug": "boutique-brc-bariloche",                               "localidad": "Bariloche",   "tipo": "B&B",      "tier": 1},
    {"id": 117, "nombre": "Bellevue",                  "slug": "bellevue",                                             "localidad": "Bariloche",   "tipo": "B&B",      "tier": 1},
    {"id": 118, "nombre": "Patagonia Signature",       "slug": "patagonia-signature-by-nordic",                        "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 1},
    {"id": 119, "nombre": "Guti Andino",               "slug": "guti-andino",                                          "localidad": "Bariloche",   "tipo": "B&B",      "tier": 1},
    {"id": 120, "nombre": "La Finca de Montaña",       "slug": "la-finca-hostel-de-montana-san-carlos-de-bariloche1",  "localidad": "Bariloche",   "tipo": "B&B",      "tier": 1},
    {"id": 122, "nombre": "Casa Nomada",               "slug": "rincon-de-la-vega",                                    "localidad": "SMA",         "tipo": "B&B",      "tier": 1},
    {"id": 126, "nombre": "Casa de Ale",               "slug": "habitacion-con-olor-a-hogar",                          "localidad": "SMA",         "tipo": "B&B",      "tier": 1},
    {"id": 127, "nombre": "Alhue Patagonia",           "slug": "alhue-patagonia-hostel",                               "localidad": "SMA",         "tipo": "Hostel",   "tier": 1},
    {"id": 128, "nombre": "Bike Hostel",               "slug": "bike-hostel-san-martin-de-los-andes",                  "localidad": "SMA",         "tipo": "Hostel",   "tier": 1},

    # ── TIER 2 — Referencia de mercado ───────────────────────────────────────
    {"id": 10,  "nombre": "Hoseria Pichi Rincon",      "slug": "hosteria-pichi-rincon",                                "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 13,  "nombre": "Paisaje Nativo",            "slug": "paisaje-nativo",                                       "localidad": "VLA",         "tipo": "Casa",     "tier": 2},
    {"id": 14,  "nombre": "Complejo Lima Lima",        "slug": "lima-lima-3-4pax",                                     "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 15,  "nombre": "BOG Alto de Antilhue",      "slug": "altos-de-antilhue-apart",                              "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 17,  "nombre": "Triplex PAX",               "slug": "triplex-vla-pax-4-centrica",                           "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 18,  "nombre": "Pueblo Patagonia",          "slug": "el-trebol-villa-la-angostura",                         "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 19,  "nombre": "Kalfulafken",               "slug": "cabana-con-costa-al-nahuel-huapi",                     "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 20,  "nombre": "Rincon de Manzano",         "slug": "rincon-de-manzano",                                    "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 22,  "nombre": "Hosteria Brisas del cerro", "slug": "hosteraa-brisas-del-cerro",                            "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 23,  "nombre": "Hosteria Las Nieves",       "slug": "hosteria-las-nieves-villa-la-angostura1",              "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 24,  "nombre": "Apu Wasi",                  "slug": "apu-wasi",                                             "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 26,  "nombre": "Hosteria CuyenCo",          "slug": "hosteria-cuyenco",                                     "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 27,  "nombre": "Hosteria Belvedere",        "slug": "hosteria-belvedere-villa-la-angostura",                "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 28,  "nombre": "Apart del Sir",             "slug": "apart-del-sir",                                        "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 29,  "nombre": "Melewe",                    "slug": "melewe",                                               "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 30,  "nombre": "Unquehue",                  "slug": "unquehue-dormis",                                      "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 31,  "nombre": "Molla Tutto",               "slug": "puerto-manzano-lodge",                                 "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 32,  "nombre": "Foresta",                   "slug": "bella-angostura-villa-la-angostura",                   "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 33,  "nombre": "Casa del Bosque",           "slug": "casa-del-bosque",                                      "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 34,  "nombre": "Depto Barrio Once 1",       "slug": "appartaments-barrio-once-ii-villa-la-angostura13",     "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 35,  "nombre": "Hosteria El Estable",       "slug": "hosteria-el-establo",                                  "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 36,  "nombre": "Hosteria Verde Morada",     "slug": "hosteria-verde-morada",                                "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 37,  "nombre": "Cabañas Lancuyen",          "slug": "lancuyen-villa-de-montaa-a-villa-la-angostura",        "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 38,  "nombre": "Sombre Verde",              "slug": "sombra-verde-alojamiento-turistico",                   "localidad": "VLA",         "tipo": "Casa",     "tier": 2},
    {"id": 39,  "nombre": "Rumel",                     "slug": "casa-de-montana-rumel",                                "localidad": "VLA",         "tipo": "Casa",     "tier": 2},
    {"id": 40,  "nombre": "Hosteria Alma Andina",      "slug": "alma-andina-hosteria",                                 "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 41,  "nombre": "Hosteria Las Semillas",     "slug": "hosteria-las-semillas",                                "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 42,  "nombre": "EpuWay",                    "slug": "epu-way",                                              "localidad": "VLA",         "tipo": "Casa",     "tier": 2},
    {"id": 43,  "nombre": "Cabañas del Montañes",      "slug": "cabanas-la-villa-del-montanes",                        "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 44,  "nombre": "Encanto del Rio",           "slug": "encanto-del-rio",                                      "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 45,  "nombre": "Hosteria Las Cumbres",      "slug": "hosteraas-las-cumbres",                                "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 46,  "nombre": "Antuquelen",                "slug": "antuquelen",                                           "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 47,  "nombre": "Remanso del Bosque",        "slug": "remanso-del-bosque-loft-cauquen-bahia-manzano",        "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 48,  "nombre": "Cielo Sur Balsas",          "slug": "cielo-sur-balsas-loft",                                "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 49,  "nombre": "La Roca",                   "slug": "la-roca-de-la-patagonia",                              "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 50,  "nombre": "Aldea Bonita",              "slug": "aldea-bonita",                                         "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 51,  "nombre": "Hosteria Patagon",          "slug": "hosteria-patagon",                                     "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 52,  "nombre": "El Bosque",                 "slug": "amigos-del-bosque",                                    "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 53,  "nombre": "Cuncumen Casa de Montaña",  "slug": "cuncumen-casa-de-montana-puerto-manzano1",             "localidad": "VLA",         "tipo": "Casa",     "tier": 2},
    {"id": 54,  "nombre": "Aparatamentos Bajo Cero",   "slug": "departamentos-bajo-cero",                              "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 55,  "nombre": "ONA",                       "slug": "ona-apart-spa",                                        "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 56,  "nombre": "Huenú",                     "slug": "hosteria-huenu",                                       "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 57,  "nombre": "La Estancia",               "slug": "la-estancia-villa-la-angostura",                       "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 58,  "nombre": "Altos del Bonito",          "slug": "altos-de-bonito",                                      "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 59,  "nombre": "Rosales",                   "slug": "rosales-alojamiento",                                  "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 60,  "nombre": "Gluck",                     "slug": "gluck-patagonia-aparts",                               "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 61,  "nombre": "Guardianes del Bayo",       "slug": "guardianes-del-bayo",                                  "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 62,  "nombre": "Origen de la Bahía",        "slug": "origen-de-la-bahia",                                   "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 63,  "nombre": "Altos los Pioneros",        "slug": "altos-los-pioneros-spa",                               "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 64,  "nombre": "Dos Bahías",                "slug": "dos-bahias-lake-resort",                               "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 65,  "nombre": "La Lucinda",                "slug": "la-lucinda",                                           "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 66,  "nombre": "El Muelle",                 "slug": "elmuellebydotboutique",                                "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 67,  "nombre": "Tilka",                     "slug": "tillka-casas-de-montaa-a",                             "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 68,  "nombre": "Arbolar",                   "slug": "arbolar",                                              "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 69,  "nombre": "Las Aguas",                 "slug": "las-aguas-rio-bonito",                                 "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 70,  "nombre": "Marinas",                   "slug": "marinas-alto-manzano",                                 "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 71,  "nombre": "Awka",                      "slug": "awka-apart",                                           "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 72,  "nombre": "Patagonia Camelot",         "slug": "patagonia-camelot",                                    "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 73,  "nombre": "La Escondida",              "slug": "hosteria-la-escondida",                                "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 74,  "nombre": "Montaña",                   "slug": "bahaa-montaa-a-resort",                                "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 76,  "nombre": "Las Maras",                 "slug": "cabaa-as-las-maras",                                   "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 77,  "nombre": "Cabañas Gnionguis",         "slug": "cabanas-gnionguis",                                    "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 80,  "nombre": "Deptos El Mercado",         "slug": "mercado-estate-departamentos",                         "localidad": "VLA",         "tipo": "Depto",    "tier": 2},
    {"id": 81,  "nombre": "Los Rododendros",           "slug": "los-rododendros-villa-la-angostura",                   "localidad": "VLA",         "tipo": "Casa",     "tier": 2},
    {"id": 82,  "nombre": "Foresta (2)",               "slug": "bella-angostura-villa-la-angostura",                   "localidad": "VLA",         "tipo": "Apart",    "tier": 2},
    {"id": 83,  "nombre": "Punta Manzano",             "slug": "punta-manzano",                                        "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 84,  "nombre": "Lekun Lekun",               "slug": "hosteria-lekun-lekun",                                 "localidad": "VLA",         "tipo": "Hosteria", "tier": 2},
    {"id": 85,  "nombre": "Lelikelen",                 "slug": "cabanas-lelikelen",                                    "localidad": "VLA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 86,  "nombre": "Tiny House",                "slug": "tiny-house-bari",                                      "localidad": "Bariloche",   "tipo": "Depto",    "tier": 2},
    {"id": 87,  "nombre": "La Loisa",                  "slug": "la-loisa-san-carlos-de-bariloche",                     "localidad": "Bariloche",   "tipo": "Cabaña",   "tier": 2},
    {"id": 88,  "nombre": "Universal Traveller",       "slug": "universal-traveller-39-s-hostel-bariloche12",          "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 89,  "nombre": "Aires del bosque",          "slug": "aires-del-bosque-cabana",                              "localidad": "Bariloche",   "tipo": "Cabaña",   "tier": 2},
    {"id": 90,  "nombre": "Alaska Patagonia",          "slug": "alaska-patagonia-hostel",                              "localidad": "Bariloche",   "tipo": "Hostel",   "tier": 2},
    {"id": 91,  "nombre": "Posada del Ñireco",         "slug": "posada-del-nireco",                                    "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 92,  "nombre": "Complejo Arrayanes",        "slug": "complejo-los-arrayanes",                               "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 93,  "nombre": "Apartur",                   "slug": "super-resort-cerro-catedral",                          "localidad": "Bariloche",   "tipo": "Apart",    "tier": 2},
    {"id": 94,  "nombre": "La Mutisia",                "slug": "la-mutisia",                                           "localidad": "Bariloche",   "tipo": "Casa",     "tier": 2},
    {"id": 95,  "nombre": "Hostel Inn",                "slug": "hostel-inn",                                           "localidad": "Bariloche",   "tipo": "Hostel",   "tier": 2},
    {"id": 96,  "nombre": "RyG",                       "slug": "ryg",                                                  "localidad": "Bariloche",   "tipo": "Depto",    "tier": 2},
    {"id": 97,  "nombre": "El Ñire",                   "slug": "hosteria-el-nire-san-carlos-de-bariloche3",            "localidad": "Bariloche",   "tipo": "Hosteria", "tier": 2},
    {"id": 98,  "nombre": "BellaVista",                "slug": "bella-vista-bariloche",                                "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 99,  "nombre": "Luma Boutique Hotel",       "slug": "residencial-eluney-san-carlos-de-bariloche",           "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 100, "nombre": "Aspen Ski",                 "slug": "aspen-ski",                                            "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 101, "nombre": "Gran Hotel Panamericano",   "slug": "gran-panamericano-san-carlos-de-bariloche",            "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 102, "nombre": "Hostería Suiza",            "slug": "casita-suiza",                                         "localidad": "Bariloche",   "tipo": "Hosteria", "tier": 2},
    {"id": 103, "nombre": "Le Charme",                 "slug": "le-charme",                                            "localidad": "Bariloche",   "tipo": "Hosteria", "tier": 2},
    {"id": 104, "nombre": "Hotel Plaza",               "slug": "plaza-san-carlos-de-bariloche",                        "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 105, "nombre": "Dinko",                     "slug": "dinko-san-carlos-de-bariloche1",                       "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 106, "nombre": "Tirol",                     "slug": "tirol",                                                "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 107, "nombre": "Le Chateau",                "slug": "le-chateau-san-carlos-de-bariloche1",                  "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 108, "nombre": "Posada de Montaña",         "slug": "posada-de-montana",                                    "localidad": "Bariloche",   "tipo": "Apart",    "tier": 2},
    {"id": 109, "nombre": "Del lago Sky",              "slug": "del-lago-sky",                                         "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 110, "nombre": "Ecoski",                    "slug": "colonial-san-carlos-de-bariloche",                     "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 111, "nombre": "Huinid",                    "slug": "villa-huinid-pioneros",                                "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 112, "nombre": "Ayres del Nahuel",          "slug": "ayres-de-nahuel",                                      "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 113, "nombre": "Crans",                     "slug": "crans-montana",                                        "localidad": "Bariloche",   "tipo": "Hotel",    "tier": 2},
    {"id": 114, "nombre": "Hostería Katy",             "slug": "hosteria-katy",                                        "localidad": "Bariloche",   "tipo": "Hostería", "tier": 2},
    {"id": 121, "nombre": "Carelhue",                  "slug": "hosteria-carelhue",                                    "localidad": "Bariloche",   "tipo": "Hostería", "tier": 2},
    {"id": 123, "nombre": "Vista Hermosa",             "slug": "cabana-vista-hermosa-san-martin-de-los-andes",         "localidad": "SMA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 124, "nombre": "Villa P",                   "slug": "cabana-villa-p",                                       "localidad": "SMA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 125, "nombre": "Rukalhue",                  "slug": "ruka-luhe",                                            "localidad": "SMA",         "tipo": "Apart",    "tier": 2},
    {"id": 129, "nombre": "Espacio Berni",             "slug": "espacio-berni",                                        "localidad": "SMA",         "tipo": "Depto",    "tier": 2},
    {"id": 130, "nombre": "Rose Garden",               "slug": "rose-garden",                                          "localidad": "SMA",         "tipo": "Apart",    "tier": 2},
    {"id": 131, "nombre": "Huella Blanca",             "slug": "huella-blanca",                                        "localidad": "SMA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 132, "nombre": "Giorgio",                   "slug": "depto-giorgio",                                        "localidad": "SMA",         "tipo": "Depto",    "tier": 2},
    {"id": 133, "nombre": "Loretta 4",                 "slug": "tu-lugar-en-loretta-4",                                "localidad": "SMA",         "tipo": "Depto",    "tier": 2},
    {"id": 134, "nombre": "Aitue",                     "slug": "cabanas-aitue",                                        "localidad": "SMA",         "tipo": "Cabaña",   "tier": 2},
    {"id": 135, "nombre": "Colibrí House",             "slug": "colibri-house",                                        "localidad": "SMA",         "tipo": "Casa",     "tier": 2},
    {"id": 136, "nombre": "Huilen de bandurrias",      "slug": "huilen-de-bandurrias",                                 "localidad": "SMA",         "tipo": "Casa",     "tier": 2},
    {"id": 137, "nombre": "Latitud 40",                "slug": "latitud-cuarenta",                                     "localidad": "SMA",         "tipo": "Hostería", "tier": 2},
    {"id": 138, "nombre": "Le Village",                "slug": "y-cabaa-as-le-village",                                "localidad": "SMA",         "tipo": "Hotel",    "tier": 2},
    {"id": 139, "nombre": "La Chira",                  "slug": "la-chira",                                             "localidad": "SMA",         "tipo": "Hostería", "tier": 2},
    {"id": 140, "nombre": "El Arbol Duende",           "slug": "hosteria-el-arbol-duende",                             "localidad": "SMA",         "tipo": "Hostería", "tier": 2},
    {"id": 141, "nombre": "Plaza Mayor",               "slug": "plaza-mayor-san-martan-de-los-andes",                  "localidad": "SMA",         "tipo": "Hostería", "tier": 2},
    {"id": 142, "nombre": "Las Lucarnas",              "slug": "hosteria-las-lucarnas",                                "localidad": "SMA",         "tipo": "Hostería", "tier": 2},
    {"id": 143, "nombre": "Amonite",                   "slug": "amonite-apart",                                        "localidad": "SMA",         "tipo": "Apart",    "tier": 2},
    {"id": 144, "nombre": "Paraiso Casa de Montaña",   "slug": "paraiso-casa-de-montana",                              "localidad": "SMA",         "tipo": "Hostería", "tier": 2},
    {"id": 145, "nombre": "Costa Traful",              "slug": "costa-traful",                                         "localidad": "V.Traful",    "tipo": "Apart",    "tier": 2},
    {"id": 146, "nombre": "Los Maitenes",              "slug": "los-maitenes-villa-traful",                            "localidad": "V.Traful",    "tipo": "Cabaña",   "tier": 2},
    {"id": 147, "nombre": "Rayen",                     "slug": "cabanas-rayen",                                        "localidad": "V.Traful",    "tipo": "Cabaña",   "tier": 2},
    {"id": 148, "nombre": "Ñancu",                     "slug": "duplex-nancu-lahuen",                                  "localidad": "V.Traful",    "tipo": "Depto",    "tier": 2},
    {"id": 149, "nombre": "Alto Traful",               "slug": "alto-traful-lodge-amp-suites",                         "localidad": "V.Traful",    "tipo": "Hotel",    "tier": 2},
    {"id": 150, "nombre": "Titania Traful",            "slug": "titania-traful",                                       "localidad": "V.Traful",    "tipo": "Casa",     "tier": 2},
    {"id": 151, "nombre": "Ñancu Lahuen",              "slug": "duplex-nancu-lahuen-villa-traful1",                    "localidad": "V.Traful",    "tipo": "Depto",    "tier": 2},
    {"id": 152, "nombre": "Glamping Vulcanche",        "slug": "glamping-vulcanche-villa-traful1",                     "localidad": "V.Traful",    "tipo": "Glamping", "tier": 2},
    {"id": 153, "nombre": "Natur Haus",                "slug": "natur-haus-villa-traful",                              "localidad": "V.Traful",    "tipo": "Casa",     "tier": 2},
    {"id": 154, "nombre": "Apartments Bariloche",      "slug": "amazing-4-bedroom-chalet-villa-traful-vt1-by-apartments-bariloche", "localidad": "V.Traful", "tipo": "Apart", "tier": 2},
]


# ─── HEADERS ROTATIVOS ───────────────────────────────────────────────────────

HEADERS_POOL = [
    {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        "Accept-Language": "es-AR,es;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    },
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept-Language": "es-AR,es;q=0.8,en;q=0.6",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Accept-Language": "es-AR,es;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    },
]


# ─── SCRAPER ─────────────────────────────────────────────────────────────────

def fetch_price(session, alojamiento, checkin, checkout):
    url = (
        f"https://www.booking.com/hotel/ar/{alojamiento['slug']}.es.html"
        f"?checkin={checkin}&checkout={checkout}"
        f"&group_adults=2&no_rooms=1&selected_currency=USD"
    )
    noches = (datetime.strptime(checkout, "%Y-%m-%d") - datetime.strptime(checkin, "%Y-%m-%d")).days
    result = {
        "id": alojamiento["id"], "nombre": alojamiento["nombre"],
        "localidad": alojamiento["localidad"], "tipo": alojamiento["tipo"],
        "tier": alojamiento["tier"], "checkin": checkin, "checkout": checkout,
        "noches": noches, "precio_usd": None, "precio_por_noche_usd": None,
        "estado": "sin_datos", "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        resp = session.get(url, headers=random.choice(HEADERS_POOL), timeout=20)
        if resp.status_code == 200:
            html = resp.text
            for pattern in [
                r'"displayPrice"\s*:\s*\{"amount"\s*:\s*([\d.]+)',
                r'"price"\s*:\s*\{"gross"\s*:\s*([\d.]+)',
                r'data-price="([\d.]+)"',
                r'"minPrice"\s*:\s*([\d.]+)',
                r'class="bui-price-display__value[^"]*"[^>]*>\s*USD\s*([\d,\.]+)',
                r'priceBreakdown.*?"grossAmount".*?"value"\s*:\s*([\d.]+)',
            ]:
                m = re.search(pattern, html)
                if m:
                    precio = float(m.group(1).replace(",", ""))
                    if precio > 0:
                        result["precio_usd"] = round(precio, 2)
                        result["precio_por_noche_usd"] = round(precio / noches, 2)
                        result["estado"] = "ok"
                        break
            if result["estado"] != "ok":
                if "captcha" in html.lower() or "robot" in html.lower():
                    result["estado"] = "bloqueado"
                elif "no disponible" in html.lower() or "not available" in html.lower():
                    result["estado"] = "sin_disponibilidad"
                else:
                    result["estado"] = "precio_no_encontrado"
        elif resp.status_code == 429:
            result["estado"] = "rate_limited"
        elif resp.status_code == 403:
            result["estado"] = "bloqueado"
        else:
            result["estado"] = f"error_http_{resp.status_code}"
    except requests.exceptions.Timeout:
        result["estado"] = "timeout"
    except Exception as e:
        result["estado"] = f"error: {str(e)[:50]}"
    return result


def fetch_with_retry(session, alojamiento, checkin, checkout):
    max_r = MAX_RETRIES_T1 if alojamiento["tier"] == 1 else MAX_RETRIES_T2
    p_min = PAUSE_MIN_T1   if alojamiento["tier"] == 1 else PAUSE_MIN_T2
    p_max = PAUSE_MAX_T1   if alojamiento["tier"] == 1 else PAUSE_MAX_T2
    for attempt in range(1, max_r + 1):
        result = fetch_price(session, alojamiento, checkin, checkout)
        if result["estado"] == "ok":
            return result
        if attempt < max_r:
            wait = random.uniform(p_min * 2, p_max * 2)
            log.info(f"  ↩ Reintento {attempt}/{max_r} — {alojamiento['nombre']} — espera {wait:.1f}s")
            time.sleep(wait)
    return result


# ─── GOOGLE SHEETS ────────────────────────────────────────────────────────────

HEADERS_SHEET = ["ID","Nombre","Localidad","Tipo","Tier","Check-in","Check-out",
                 "Noches","Precio Total USD","Precio/Noche USD","Estado","Timestamp"]
HDR_FMT = {"textFormat":{"bold":True,"foregroundColor":{"red":1,"green":1,"blue":1}},
           "backgroundColor":{"red":0.15,"green":0.35,"blue":0.55},"horizontalAlignment":"CENTER"}


def get_or_create_ws(sh, title):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=10000, cols=12)
        ws.append_row(HEADERS_SHEET)
        ws.format(f"A1:L1", HDR_FMT)
    return ws


def to_row(r):
    return [r["id"], r["nombre"], r["localidad"], r["tipo"], r["tier"],
            r["checkin"], r["checkout"], r["noches"],
            r["precio_usd"] or "", r["precio_por_noche_usd"] or "",
            r["estado"], r["timestamp"]]


def write_to_sheets(results, gc):
    sh = gc.open(SPREADSHEET_NAME)

    rows = [to_row(r) for r in results]

    # Historial acumulativo
    ws = get_or_create_ws(sh, "Historial")
    ws.append_rows(rows, value_input_option="USER_ENTERED")

    # Última corrida completa
    ws = get_or_create_ws(sh, "Ultima Corrida")
    ws.clear()
    ws.append_row(HEADERS_SHEET)
    ws.format("A1:L1", HDR_FMT)
    ws.append_rows(rows, value_input_option="USER_ENTERED")

    # Solo Tier 1
    ws = get_or_create_ws(sh, "Tier1 - Competencia Directa")
    ws.clear()
    ws.append_row(HEADERS_SHEET)
    ws.format("A1:L1", HDR_FMT)
    ws.append_rows([r for r in rows if r[4] == 1], value_input_option="USER_ENTERED")

    # Resumen estadístico
    import statistics
    ws = get_or_create_ws(sh, "Resumen Mercado")
    ws.clear()
    res_hdrs = ["Localidad","Tier","Check-in","Check-out","N Precios",
                "Mín USD/noche","Mediana USD/noche","Máx USD/noche","Promedio USD/noche"]
    ws.append_row(res_hdrs)
    ws.format("A1:I1", HDR_FMT)

    grupos = {}
    for r in results:
        if r["estado"] == "ok" and r["precio_por_noche_usd"]:
            key = (r["localidad"], r["tier"], r["checkin"], r["checkout"])
            grupos.setdefault(key, []).append(r["precio_por_noche_usd"])

    res_rows = []
    for (loc, tier, ci, co), precios in sorted(grupos.items()):
        res_rows.append([loc, tier, ci, co, len(precios),
                         round(min(precios),2), round(statistics.median(precios),2),
                         round(max(precios),2), round(statistics.mean(precios),2)])
    if res_rows:
        ws.append_rows(res_rows, value_input_option="USER_ENTERED")

    log.info(f"✅ Google Sheets actualizado — {len(rows)} registros en 4 hojas.")


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    import os
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        raise ValueError("Variable de entorno GOOGLE_CREDENTIALS_JSON no encontrada.")
    creds = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)

    fechas = get_target_dates()
    log.info(f"📅 Fechas: {[f[0] for f in fechas]}")

    t1 = [a for a in ALOJAMIENTOS if a["tier"] == 1]
    t2 = [a for a in ALOJAMIENTOS if a["tier"] == 2]
    total = len(ALOJAMIENTOS) * len(fechas)
    log.info(f"🏨 T1: {len(t1)}  T2: {len(t2)}  Total requests: {total}")

    session, all_results, count = requests.Session(), [], 0

    log.info("━━━ TIER 1 — Competencia directa (con reintentos) ━━━")
    for aloj in t1:
        for ci, co in fechas:
            count += 1
            r = fetch_with_retry(session, aloj, ci, co)
            all_results.append(r)
            precio = f"${r['precio_por_noche_usd']}/noche" if r["precio_por_noche_usd"] else r["estado"]
            log.info(f"[{count}/{total}] {'✅' if r['estado']=='ok' else '⚠️'} T1 | {aloj['nombre']} | {ci} → {precio}")
            time.sleep(random.uniform(PAUSE_MIN_T1, PAUSE_MAX_T1))

    log.info("━━━ TIER 2 — Referencia de mercado ━━━")
    for aloj in t2:
        for ci, co in fechas:
            count += 1
            r = fetch_with_retry(session, aloj, ci, co)
            all_results.append(r)
            precio = f"${r['precio_por_noche_usd']}/noche" if r["precio_por_noche_usd"] else r["estado"]
            log.info(f"[{count}/{total}] {'✅' if r['estado']=='ok' else '·'} T2 | {aloj['nombre']} | {ci} → {precio}")
            time.sleep(random.uniform(PAUSE_MIN_T2, PAUSE_MAX_T2))

    ok_t1  = sum(1 for r in all_results if r["estado"]=="ok" and r["tier"]==1)
    ok_t2  = sum(1 for r in all_results if r["estado"]=="ok" and r["tier"]==2)
    tot_t1 = len(t1) * len(fechas)
    tot_t2 = len(t2) * len(fechas)
    log.info(f"\n📊 T1: {ok_t1}/{tot_t1} ({ok_t1/tot_t1*100:.0f}%) | T2: {ok_t2}/{tot_t2} ({ok_t2/tot_t2*100:.0f}%)")

    write_to_sheets(all_results, gc)
    log.info("🎉 ¡Proceso completado!")


if __name__ == "__main__":
    main()
