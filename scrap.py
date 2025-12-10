#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from playwright.sync_api import sync_playwright
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
from lxml import html
from supabase import create_client, Client 
import time
import math
import re
import json
import datetime
import traceback
import os
import pytz

# ==================== CONFIGURAZIONE GLOBALE ====================
URL_TORNEO = 'https://www.sofascore.com/it/torneo/calcio/italy/serie-a/23#id:76457#tab:matches'
CONSENT_BUTTON_SELECTOR = 'button:has-text("Acconsento"), button:has-text("Consent")'
TARGET_GIORNATA = 14

BUTTON_GIORNATA_SELECTOR = 'button.dropdown__button[aria-haspopup="listbox"]:has-text("Round"), button.dropdown__button[aria-haspopup="listbox"]:has-text("Giornata")'
CONTAINER_GIORNATA_SELECTOR = 'div.card-component.mobile-only'
MATCH_LINK_SELECTOR = f'{CONTAINER_GIORNATA_SELECTOR} a[data-id][href*="/match/"]'
SCROLL_CONTAINER_SELECTOR = '.beautiful-scrollbar__content'

# ==================== SELETTORI CSS ====================

# Info Partita Base
SELECTOR_SQUADRA_CASA = 'div[style*="left: 0px"] bdi.textStyle_display\\.medium'
SELECTOR_SQUADRA_TRASFERTA = 'div[style*="right: 0px"] bdi.textStyle_display\\.medium'
SELECTOR_PUNTEGGIO_TOTALE = 'span.textStyle_body\\.medium.c_neutrals\\.nLv1.trunc_true'
SELECTOR_STATUS = 'div.card-component span[class*="textStyle"]'
SELECTOR_STATO_DATA = 'span.textStyle_body\\.medium.c_neutrals\\.nLv3.ta_center.d_block'
SELECTOR_ORA = 'span.textStyle_display\\.large.c_neutrals\\.nLv1.d_block.ta_center.pos_absolute'

# Moduli
SELECTOR_MODULO_CASA = 'span.Text.gHLcGU[color="onSurface.nLv1"]'
SELECTOR_MODULO_TRASFERTA = 'span.Text.gHLcGU[color="onSurface.nLv1"]'

# Eventi
SELECTOR_EVENTI_CONTAINER = 'div.hover\\:bg_surface\\.s2.cursor_pointer'
SELECTOR_EVENTO_MINUTO = 'span.textStyle_display\\.micro'
SELECTOR_EVENTO_GIOCATORE = 'span.textStyle_body\\.medium.c_neutrals\\.nLv1.h_lg'
SELECTOR_EVENTO_TIPO = 'span.textStyle_body\\.medium.c_neutrals\\.nLv3.h_lg'
SELECTOR_EVENTO_ICONA_GOL = 'svg title:has-text("Gol")'
SELECTOR_EVENTO_ICONA_GIALLO = 'path[fill*="#D9AF00"]'
SELECTOR_EVENTO_ICONA_ROSSO = 'path[fill*="error"]'
SELECTOR_EVENTO_ICONA_SOSTITUZIONE = 'svg path[d*="M12 2C6.48"]'

# Titolari
SELECTOR_TITOLARI_CONTAINER = 'div.Box.klGMtt.sc-eDPEul.gghSOi'  # Container generale formazioni
SELECTOR_TITOLARE_NUMERO = 'span[color="onColor.secondary"]'
SELECTOR_TITOLARE_NOME = 'span.Text.Dodlb span.Box.klGMtt'
SELECTOR_TITOLARE_VOTO = 'div.Box.klGMtt.sc-eDPEul.gghSOi span'

# Panchinari
SELECTOR_PANCHINARI_CONTAINER = 'div.Box.Flex.deRHiB.cQgcrM[cursor="pointer"]'
SELECTOR_PANCHINARO_NUMERO = 'bdi.Box.ewxwAz'
SELECTOR_PANCHINARO_NOME = 'span.Box.klGMtt'
SELECTOR_PANCHINARO_VOTO = 'div.Box.klGMtt.sc-eDPEul.pXLBd span'
SELECTOR_PANCHINARO_MINUTO_ENTRATA = 'span[color="secondary.default"]'
SELECTOR_PANCHINARO_SOSTITUITO = 'span[color="onSurface.nLv3"]'
                
# ===== CONFIGURAZIONE SUPABASE =====
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://ipqxjudlxcqacgtmpkzx.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlwcXhqdWRseGNxYWNndG1wa3p4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTEyNjU3OSwiZXhwIjoyMDc0NzAyNTc5fQ.9nMpSeM-p5PvnF3rwMeR_zzXXocyfzYV24vau3AcDso')

ORA_LEGALE_OFFSET = 0

# ==================== UTILITY E SUPABASE ====================

def init_supabase():
    """Inizializza il client Supabase"""
    try:
        client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connesso a Supabase")
        return client
    except Exception as e:
        print(f"❌ Errore connessione Supabase: {e}")
        traceback.print_exc()
        return None

def check_match_exists(supabase, match_id):
    """Verifica se una partita esiste già nel database"""
    try:
        response = supabase.table('Partite').select('id').eq('id', match_id).execute()
        return len(response.data) > 0
    except Exception as e:
        print(f"⚠️ Errore verifica esistenza partita {match_id}: {e}")
        return False

def insert_or_update_match(supabase, match_data):
    """Inserisce o aggiorna una partita nel database"""
    try:
        match_id = match_data['id']
        exists = check_match_exists(supabase, match_id)
        
        if exists:
            response = supabase.table('Partite').update(match_data).eq('id', match_id).execute()
            print(f"  🔄 Aggiornata partita ID {match_id}")
        else:
            response = supabase.table('Partite').insert(match_data).execute()
            print(f"  ✨ Creata nuova partita ID {match_id}")
        
        return True
    except Exception as e:
        print(f"❌ Errore inserimento/aggiornamento partita {match_data.get('id', 'N/A')}: {e}")
        traceback.print_exc()
        return False

def fetch_giocatori_mapping(supabase: Client):
    mapping = {
        'cognomi': {},
        'nomi_completi': {}
    }
    
    print("\n🔍 Caricamento mappatura ID Giocatore...")
    try:
        response = supabase.table('Giocatori').select('id, nome, nomeint, squadra').execute()
        
        for record in response.data:
            giocatore_id = record.get('id')
            squadra = record.get('squadra', '').strip()
            
            # Campo "nome" = cognome (per titolari)
            nome_cognome = pulisci_nome(record.get('nome', ''))
            if nome_cognome and giocatore_id is not None:
                mapping['cognomi'][(nome_cognome, squadra)] = giocatore_id
            
            # Campo "nomeint" = nome completo (per panchinari)
            nome_completo = pulisci_nome(record.get('nomeint', ''))
            if nome_completo and giocatore_id is not None:
                mapping['nomi_completi'][(nome_completo, squadra)] = giocatore_id
        
        print(f"✅ Mappatura caricata:")
        print(f"   • {len(mapping['cognomi'])} cognomi (per titolari)")
        print(f"   • {len(mapping['nomi_completi'])} nomi completi (per panchinari)")
        return mapping
        
    except Exception as e:
        print(f"❌ Errore durante il caricamento della mappatura giocatori: {e}")
        traceback.print_exc()
        return {'cognomi': {}, 'nomi_completi': {}}

def pulisci_nome(nome_grezzo):
    if not nome_grezzo:
        return ""
    
    # 1. Rimuove numero iniziale e (c)
    cleaned = re.sub(r'^\d+\s*\(?c?\)?\.?\s*', '', nome_grezzo.strip())

    # 2. ✅ RIMUOVE IL SEGNO DI CAPITANO (C) o (c) OVUNQUE SI TROVI
    # Rimuove (C), (c) o (C.) ovunque nel nome, inclusi spazi multipli
    cleaned = re.sub(r'\s*\([Cc]\.?\)', '', cleaned)
    
    # 2. ✅ RIMUOVE SOFT HYPHEN (&shy;, &#173;, \u00ad)
    cleaned = cleaned.replace('&shy;', '')
    cleaned = cleaned.replace('&#173;', '')
    cleaned = cleaned.replace('\u00ad', '')  # Unicode soft hyphen
    cleaned = cleaned.replace('\xad', '')     # Byte soft hyphen
    
    # 3. Normalizza spazi multipli
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

def get_giornata_button_locator(page):
    """Restituisce il localizzatore esatto per il bottone della Giornata,
       usando il ruolo e il nome ARIA univoco (Select item in event list)."""
    return page.get_by_role("combobox", name="Select item in event list")

def get_current_round(page): 
    """Estrae il numero del round dal bottone."""
    try:
        locator = get_giornata_button_locator(page) 
        text = locator.text_content().strip() 
        match = re.search(r'\d+', text)
        return int(match.group()) if match else 'N/A'
    except Exception:
        return 'N/A'

def get_inner_text_safe(locator, timeout=100):
    """Estrae testo in modo sicuro"""
    try:
        return locator.inner_text(timeout=timeout).strip()
    except:
        return ""

def get_text_content_safe(locator, timeout=100):
    """Estrae textContent completo"""
    try:
        return locator.text_content(timeout=timeout).upper().strip()
    except:
        return ""

def get_locator_count(page, xpath_str):
    """Verifica l'esistenza dell'elemento"""
    try:
        return page.locator(f"xpath={xpath_str}").count()
    except:
        return 0

def normalizza_nome_per_match(nome):
    """
    Normalizza un nome per il matching flessibile
    Es: "H. W. Meister" → "meister"
    Es: "Henrik Wendel Meister" → "meister"
    """
    if not nome:
        return ""
    
    # Prendi solo il cognome (ultima parola)
    parti = nome.split()
    if parti:
        cognome = parti[-1].lower()
        # Rimuovi caratteri speciali
        cognome = re.sub(r'[^\w\s]', '', cognome)
        return cognome
    return nome.lower()

def trova_giocatore_in_campo(nome_evento, giocatori_in_campo):
    """
    Cerca un giocatore in campo usando matching flessibile
    
    Args:
        nome_evento: Nome dal evento (es. "H. W. Meister")
        giocatori_in_campo: Dict con tutti i giocatori
    
    Returns:
        Nome chiave nel dict o None
    """
    nome_normalizzato = normalizza_nome_per_match(nome_evento)
    
    # 1. Match esatto
    if nome_evento in giocatori_in_campo:
        return nome_evento
    
    # 2. Match per cognome
    for nome_campo in giocatori_in_campo.keys():
        cognome_campo = normalizza_nome_per_match(nome_campo)
        if cognome_campo == nome_normalizzato:
            return nome_campo
    
    # 3. Match parziale (contiene)
    for nome_campo in giocatori_in_campo.keys():
        if nome_normalizzato in normalizza_nome_per_match(nome_campo):
            return nome_campo
    
    return None

# ==================== ESTRAZIONE PARTITE ====================

def fetch_giornata_matches(target_giornata):
    """Scarica la lista degli href delle partite (10 totali) della giornata target."""
    
    print(f"\n{'='*80}")
    print(f"FASE 1: RECUPERO PARTITE GIORNATA {target_giornata}")
    print(f"{'='*80}")
    
    # 🛑 Usiamo il blocco 'with' per la gestione automatica della chiusura di Playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        print(f"📡 Caricamento pagina torneo...")
        page.goto(URL_TORNEO, wait_until='domcontentloaded', timeout=90000)
        time.sleep(4)
        
        # Gestione cookie 
        try:
            page.locator(CONSENT_BUTTON_SELECTOR).click(timeout=5000)
            print("✅ Cookie accettati")
        except:
            pass
        
        time.sleep(1)
        
        # Verifica e cambio giornata
        current_round = get_current_round(page) 
        print(f"📅 Giornata visualizzata: {current_round}")
        
        if current_round != target_giornata:
            print(f"🔄 Cambio giornata da {current_round} a {target_giornata}...")
            
            try:
                # 🛡️ Clicca sul bottone con selettore ARIA robusto
                giornata_button = get_giornata_button_locator(page)
                giornata_button.click(timeout=30000, force=True)
                time.sleep(1)

                # Calcolo scroll
                try:
                    giornata_gap = int(current_round) - target_giornata
                except ValueError:
                     giornata_gap = 20 - target_giornata 
                     
                num_scrolls = max(0, math.ceil(giornata_gap / 7)) 
                
                if num_scrolls > 0:
                    print(f"📜 Distanza stimata {giornata_gap} giornate → {num_scrolls} scroll sulla tendina")
                    # 🟢 Usa il selettore del contenuto scorrevole
                    dropdown_scroll_container = page.locator(SCROLL_CONTAINER_SELECTOR).first 
                    
                    for i in range(num_scrolls):
                        dropdown_scroll_container.evaluate('el => el.scrollBy(0, 300)') 
                        time.sleep(0.5)
                        print(f"   ↓ Scroll {i+1}/{num_scrolls}")

                target_text = f"Round {target_giornata}"
                # 🛡️ Locator robusto con exact=True
                locator_li = page.get_by_role("option", name=target_text, exact=True)

                locator_li.scroll_into_view_if_needed(timeout=10000)
                time.sleep(1)
                
                element_handle = locator_li.element_handle()
                if element_handle:
                    element_handle.evaluate('el => el.click()')
                    print(f"✅ Giornata {target_giornata} selezionata")
                    
                    try:
                        # 🛡️ Verifica cambio Round con selettore CSS :has-text (correzione)
                        BUTTON_GIORNATA_SELECTOR_STRING = 'button[role="combobox"][aria-haspopup="listbox"]'
                        page.wait_for_selector(
                            f'{BUTTON_GIORNATA_SELECTOR_STRING}:has-text("{target_text}")', 
                            timeout=15000
                        )
                        print("✅ Attesa cambio Round verificata.")
                    except PlaywrightTimeoutError:
                        print("⚠️ Timeout nella verifica del cambio Round. Proseguo...")

                    time.sleep(7) 

                else:
                    raise Exception("Elemento giornata non trovato")
                    
            except Exception as e:
                # 🛑 Gestione errori SENZA chiudere il browser manualmente
                print(f"❌ Errore selezione giornata: {e}")
                return [] 
        
        # ---------------------------------------------------------
        # 🟢 LOGICA DI ESTRAZIONE HREF CON SELETTORE ANCORATO
        # ---------------------------------------------------------
        
        # 1. Attesa che almeno un elemento appaia (usando il selettore ancorato e corretto)
        try:
            print("⏳ Attesa apparizione primo link partita...")
            page.wait_for_selector(MATCH_LINK_SELECTOR, timeout=60000) 
            page.locator(MATCH_LINK_SELECTOR).first.wait_for(state='visible', timeout=10000)
            print("✅ Primo link partita apparso.")

        except PlaywrightTimeoutError:
            print("❌ Timeout: Nessun link partita trovato dopo 60 secondi.")
            return []

        time.sleep(3) 
        
        # Scroll della pagina per caricare eventuali partite lazy-loaded
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)
        
        # 2. Localizza TUTTI i link usando il selettore specifico
        match_locators = page.locator(MATCH_LINK_SELECTOR)
        num_matches = match_locators.count()
        print(f"🔗 Trovati {num_matches} link partita usando il selettore ancorato.")

        matches = []
        for i in range(num_matches):
            match_locator = match_locators.nth(i)
            
            href = match_locator.get_attribute('href')
            match_id_attr = match_locator.get_attribute('data-id')
            match_id = int(match_id_attr) if match_id_attr else 'N/A'
            
            if href:
                full_url = f"https://www.sofascore.com{href}" if not href.startswith('https://') else href
                match_name = re.search(r'/match/([^/]+)/', full_url)
                match_name = match_name.group(1) if match_name else f"match_{match_id}"
                
                matches.append({
                    'id': match_id,
                    'url': full_url,
                    'name': match_name
                })

        # --- Fine Blocco 'with' ---
        
        # Logica di deduplicazione
        matches_unici = {m['id']: m for m in matches if m['id'] != 'N/A'}
        final_matches = list(matches_unici.values())
        
        print(f"\n✅ Trovate {len(final_matches)} partite per la giornata {target_giornata}.")
        for m in final_matches:
            print(f"   • {m['name']} (ID: {m['id']})")
        
        return final_matches
# ==================== ESTRAZIONE DATI PARTITA ====================

def extract_match_basic_info(page):
    
    # ===== SQUADRE =====
    squadra_casa = None
    squadra_trasferta = None
    
    try:
        casa_locator = page.locator(SELECTOR_SQUADRA_CASA).first
        if casa_locator.count() > 0:
            squadra_casa = casa_locator.text_content().strip()
    except:
        pass
    
    try:
        trasferta_locator = page.locator(SELECTOR_SQUADRA_TRASFERTA).first
        if trasferta_locator.count() > 0:
            squadra_trasferta = trasferta_locator.text_content().strip()
    except:
        pass
    
    # Fallback se non trovati
    if not squadra_casa or not squadra_trasferta:
        try:
            teams = page.locator('a[href*="/team/"] bdi').all_text_contents()
            if len(teams) >= 2:
                squadra_casa = teams[0].strip()
                squadra_trasferta = teams[1].strip()
        except:
            pass
    
    # ===== STATO (PRIMA DI TUTTO) =====
    stato = 'NG'  # ✅ CORRETTO: usa 'stato' direttamente

    try:
        stato_text = page.locator(SELECTOR_STATO_DATA).inner_text().strip()
        
        if stato_text == "Finita":
            stato = 'F'
        elif stato_text == "Intervallo" or re.match(r'^\d+[\'\+]?$', stato_text):
            stato = 'IC'
        else:
            stato = 'NG'
            
    except Exception as e:
        print(f"⚠️ Errore nell'estrazione dello stato, default a NG: {e}")
        stato = 'NG'
    
    # ===== DATA E ORA (SOLO SE NG) =====
    data = None
    ora = None
    
    if stato == 'NG':  # ✅ CORRETTO: usa 'stato'
        cest = pytz.timezone('Europe/Rome')
        
        # DATA
        try:
            if page.locator('span:has-text("Oggi")').count() > 0:
                data = datetime.datetime.now(cest).strftime('%Y-%m-%d')
            elif page.locator('span:has-text("Domani")').count() > 0:
                data = (datetime.datetime.now(cest) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            else:
                date_spans = page.locator('span').all_text_contents()
                for text in date_spans:
                    if re.match(r'\d{2}[/-]\d{2}[/-]\d{4}', text):
                        parsed_date = datetime.datetime.strptime(text, '%d/%m/%Y') if '/' in text else datetime.datetime.strptime(text, '%d-%m-%Y')
                        data = parsed_date.strftime('%Y-%m-%d')
                        break
        except:
            pass
        
        # ORA
        try:
            time_spans = page.locator('span').all_text_contents()
            for text in time_spans:
                text = text.strip()
                if re.match(r'^\d{2}:\d{2}$', text):
                    ora_parsed = datetime.datetime.strptime(text, '%H:%M')
                    ora_corretta = ora_parsed + datetime.timedelta(hours=ORA_LEGALE_OFFSET)
                    ora = ora_corretta.strftime('%H:%M') + ":00"
                    break
        except Exception as e:
            print(f"   ⚠️ Errore estrazione ora: {e}")
            pass
        
        # PRINT FORMATTATO (solo per NG)
        if data and ora:
            data_display = datetime.datetime.strptime(data, '%Y-%m-%d').strftime('%d-%m-%Y')
            ora_display = ora[:5]
            print(f"   📅 Data/Ora: {data_display} {ora_display}")
    
    # ===== GOAL (solo se F o IC) =====
    goalcasa = None
    goaltrasferta = None
    
    if stato in ['F', 'IC']:  # ✅ CORRETTO: usa 'stato'
        try:
            # ✅ STRATEGIA 1: Usa all_text_contents()
            punteggio_locator = page.locator(SELECTOR_PUNTEGGIO_TOTALE)
            
            if punteggio_locator.count() > 0:
                all_texts = punteggio_locator.all_text_contents()
                
                for punteggio_text in all_texts:
                    punteggio_text = punteggio_text.strip()
                    match = re.search(r'(\d+)\s*-\s*(\d+)', punteggio_text)
                    
                    if match:
                        goalcasa = int(match.group(1))
                        goaltrasferta = int(match.group(2))
                        print(f"   ⚽ Goal estratti: {goalcasa} - {goaltrasferta}")
                        break
            
            # ✅ STRATEGIA 2 (FALLBACK)
            if goalcasa is None or goaltrasferta is None:
                print("   ⚠️  Tentativo strategia fallback per goal...")
                
                score_xpath = "//span[contains(@class, 'textStyle_body') and contains(text(), '-')]"
                score_elements = page.locator(f"xpath={score_xpath}").all_text_contents()
                
                for score_text in score_elements:
                    match = re.search(r'(\d+)\s*-\s*(\d+)', score_text)
                    if match:
                        goalcasa = int(match.group(1))
                        goaltrasferta = int(match.group(2))
                        print(f"   ⚽ Goal estratti (fallback): {goalcasa} - {goaltrasferta}")
                        break
            
            # ✅ STRATEGIA 3 (ULTIMO FALLBACK)
            if goalcasa is None or goaltrasferta is None:
                print("   ⚠️  Tentativo strategia finale per goal...")
                
                score_spans = page.locator('div[class*="Box"] span[class*="textStyle"]').all_text_contents()
                
                numeri_trovati = []
                for text in score_spans:
                    text = text.strip()
                    if text.isdigit():
                        numeri_trovati.append(int(text))
                
                if len(numeri_trovati) >= 2:
                    goalcasa = numeri_trovati[0]
                    goaltrasferta = numeri_trovati[1]
                    print(f"   ⚽ Goal estratti (scan numeri): {goalcasa} - {goaltrasferta}")
                    
        except Exception as e:
            print(f"   ❌ Errore estrazione goal: {e}")
            pass
    
    return {
        'casa': squadra_casa,
        'trasferta': squadra_trasferta,
        'data': data,  # None se F/IC
        'ora': ora,    # None se F/IC
        'stato': stato,  # ✅ CORRETTO: usa 'stato'
        'gcasa': goalcasa,
        'gtrasferta': goaltrasferta
    }

# ==================== ESTRAZIONE EVENTI ====================

def extract_eventi(page):
    """
    Estrae gli eventi usando lxml per velocità
    
    Args:
        page: Oggetto page di Playwright
    
    Returns:
        Lista di dict con eventi
    """
    eventi = []
    
    try:
        # ✅ SCARICA HTML UNA VOLTA SOLA
        html_content = page.content()
        tree = html.fromstring(html_content)
        
        # XPath: Cerca tutti i container degli eventi
        eventi_xpath = '//div[contains(@class, "hover:bg_surface") and contains(@class, "cursor_pointer")]'
        eventi_elements = tree.xpath(eventi_xpath)
        
        print(f"   🔍 Trovati {len(eventi_elements)} eventi")
        
        for evento_elem in eventi_elements:
            try:
                # ===== ESTRAI MINUTO =====
                minuto = ""
                minuto_xpath = './/span[contains(@class, "textStyle_display.micro")]'
                minuto_elements = evento_elem.xpath(minuto_xpath)
                if minuto_elements:
                    minuto = minuto_elements[0].text_content().strip()
                
                # ===== ESTRAI SVG TITLE =====
                svg_title_xpath = './/svg/title'
                svg_titles = evento_elem.xpath(svg_title_xpath)
                svg_text = " ".join([t.text_content() for t in svg_titles]).lower()
                
                # ===== ESTRAI NOMI (MARCATORE) - classe nLv1 =====
                marcatore_xpath = './/span[contains(@class, "textStyle_body.medium") and contains(@class, "c_neutrals.nLv1")]'
                marcatore_elements = evento_elem.xpath(marcatore_xpath)
                marcatore = marcatore_elements[0].text_content().strip() if marcatore_elements else None
                
                # ===== ESTRAI ASSIST - classe nLv3 =====
                assist_xpath = './/span[contains(@class, "textStyle_body.medium") and contains(@class, "c_neutrals.nLv3")]'
                assist_elements = evento_elem.xpath(assist_xpath)
                
                # ===== DETERMINA TIPO EVENTO =====
                tipo = "ALTRO"
                descrizione = ""
                
                # 🎯 RIGORE SBAGLIATO
                if "rigore sbagliato" in svg_text or "penalty missed" in svg_text:
                    tipo = "❌ RIGORE SBAGLIATO"
                    if marcatore:
                        descrizione = marcatore
                    # Cerca anche testo "Parato"
                    for elem in assist_elements:
                        testo = elem.text_content().strip().lower()
                        if "parato" in testo:
                            break
                
                # ⚽ GOL (include anche rigore segnato)
                elif "gol" in svg_text or "goal" in svg_text or "rigore" in svg_text:
                    tipo = "⚽ GOL"
                    
                    if not marcatore:
                        continue
                    
                    # ✅ ESTRAI ASSISTMAN (se presente)
                    assistman = None
                    for elem in assist_elements:
                        testo = elem.text_content().strip()
                        # Verifica che non sia testo descrittivo
                        if testo and not any(x in testo.lower() for x in ['parato', 'fuori', 'out', 'rigore', 'penalty', 'assist']):
                            # Verifica che contenga lettere (probabile nome)
                            if re.search(r'[A-Za-zÀ-ÿ]', testo):
                                assistman = testo
                                break
                    
                    # Costruisci descrizione
                    if assistman:
                        descrizione = f"{marcatore} (Assist: {assistman})"
                    else:
                        descrizione = marcatore
                    
                    # 🎯 Aggiungi flag rigore se presente
                    if "rigore" in svg_text or "penalty" in svg_text:
                        descrizione = f"{descrizione} [Rigore]"
                
                # 🟨 CARTELLINO GIALLO
                elif "giallo" in svg_text or "yellow" in svg_text:
                    tipo = "🟨 CARTELLINO"
                    if marcatore:
                        descrizione = marcatore
                
                # 🟥 CARTELLINO ROSSO
                elif "rosso" in svg_text or "red" in svg_text:
                    tipo = "🟥 CARTELLINO"
                    if marcatore:
                        descrizione = marcatore
                
                # 🔄 SOSTITUZIONE
                elif any("fuori" in elem.text_content().lower() or "out" in elem.text_content().lower() for elem in assist_elements):
                    tipo = "🔄 SOSTITUZIONE"
                    giocatore_entra = marcatore if marcatore else "Sconosciuto"
                    giocatore_esce = "Sconosciuto"
                    
                    # Cerca chi esce (testo dopo "Fuori:")
                    for elem in assist_elements:
                        testo = elem.text_content().strip()
                        if "fuori:" in testo.lower() or "out:" in testo.lower():
                            giocatore_esce = testo.split(":")[-1].strip()
                            break
                    
                    descrizione = f"Esce: {giocatore_esce}, Entra: {giocatore_entra}"
                
                # ✅ Aggiungi solo se tipo rilevante
                if tipo != "ALTRO" and descrizione:
                    eventi.append({
                        'tipo': tipo,
                        'descrizione': descrizione,
                        'minuto': minuto
                    })
                    
            except Exception as e:
                print(f"   ⚠️  Errore elaborazione evento: {e}")
                continue
        
        print(f"   ✅ Estratti {len(eventi)} eventi validi")
        return eventi
        
    except Exception as e:
        print(f"   ❌ Errore estrazione eventi: {e}")
        traceback.print_exc()
        return []

# ==================== ESTRAZIONE FORMAZIONI ====================

def extract_formazioni(page, squadra_casa, squadra_trasferta):
    """
    Estrae le formazioni e i voti usando XPath mirati basati sulla struttura HTML.

    Args:
        page: Oggetto page di Playwright
        squadra_casa: Nome squadra di casa
        squadra_trasferta: Nome squadra in trasferta

    Returns:
        Dict con formazioni di casa e trasferta
    """
    risultati = {"casa": [], "trasferta": []}
    
    try:
        # ✅ SCARICA HTML UNA VOLTA SOLA
        html_content = page.content()
        tree = html.fromstring(html_content)
        
        print(f"\n{'='*60}")
        print(f"📊 ESTRAZIONE FORMAZIONI (Versione Corretta)")
        print(f"{'='*60}")
        
        # --- TITOLARI ---
        # XPath dei nomi: span con color="onColor.primary" e classe "Text Dodlb"
        titolari_xpath = '//span[@color="onColor.primary" and contains(@class, "Dodlb")]'
        titolari_elements = tree.xpath(titolari_xpath)
        
        print(f"\n👥 TITOLARI ({len(titolari_elements)} trovati)")
        print(f"{'─'*60}")
        
        num_titolari_per_squadra = len(titolari_elements) // 2
        
        for idx, titolare_elem in enumerate(titolari_elements):
            try:
                # Determina squadra
                if idx < num_titolari_per_squadra:
                    squadra_key = "casa"
                    nome_squadra = squadra_casa
                    squadra_label = "🏠 CASA"
                else:
                    squadra_key = "trasferta"
                    nome_squadra = squadra_trasferta
                    squadra_label = "✈️ TRASFERTA"
                
                # ✅ ESTRAI NOME
                nome_text = titolare_elem.text_content().strip()
                nome = re.sub(r'^\d+', '', nome_text).strip()
                nome = pulisci_nome(nome) # Assumi che pulisci_nome esista
                
                # ✅ ESTRAI VOTO - LOGICA CORRETTA
                voto = None
                player_container = titolare_elem.xpath('./ancestor::div[3]') 

                if player_container:
                    # 💡 STRATEGIA CHIAVE: Cerca l'elemento che contiene 'aria-valuenow' (il tuo blocco HTML)
                    aria_voto_xpath = './/span[@role="meter" and @aria-valuenow]'
                    voto_span = player_container[0].xpath(aria_voto_xpath)
                    
                    if voto_span:
                        # Preleva il valore numerico dall'attributo 'aria-valuenow'
                        voto_text = voto_span[0].get('aria-valuenow')
                        if voto_text:
                            # Converti direttamente in float. Non serve 'replace' se l'input è un intero pulito.
                            try:
                                voto = float(voto_text)
                            except ValueError:
                                # Se per qualche motivo il valore non è numerico, resta None
                                pass
                                
                # ✅ PRINT DEBUG (il resto del tuo codice originale)
                if voto is not None:
                    print(f"    {squadra_label} | {nome:20s} | Voto: {voto}")
                    
                    risultati[squadra_key].append({
                        "nome": nome,
                        "voto": voto,
                        "ruolo": "TIT",
                        "titolare": True,
                        "squadra": nome_squadra
                    })
                else:
                    # Aggiunge il giocatore con voto: None
                    risultati[squadra_key].append({
                        "nome": nome,
                        "voto": None, 
                        "ruolo": "TIT",
                        "titolare": True,
                        "squadra": nome_squadra
                    })
                    print(f"    {squadra_label} | {nome:20s} | ❌ Voto non trovato")
                
            except Exception as e:
                print(f"   ⚠️ Errore estrazione titolare {idx}: {e}")
                continue
        
        # --- PANCHINARI ---
        
        # ===== PANCHINARI (LOGICA AGGIORNATA PER BLOCCHI SEPARATI) =====

        print(f"\n🪑 PANCHINARI (con voto) - Separazione per blocco HTML")
        print(f"{'─'*60}")
        
        giocatori_gia_processati = set(g['nome'] for g in risultati['casa'] + risultati['trasferta'])

        # --- FASE 1: Individuare i due contenitori principali ---
        # Cerchiamo i due div con classe "DooVT" che contengono i panchinari di CASA e TRASFERTA
        blocchi_squadra = tree.xpath('//div[@class="Box DooVT"]')
        
        if len(blocchi_squadra) < 2:
            print(" ⚠️ Errore: Trovati meno di 2 blocchi DooVT per i panchinari. Impossibile separare.")
        else:
            # Assunzione: Il primo blocco è CASA, il secondo è TRASFERTA.
            blocchi_mapping = {
                "casa": {"element": blocchi_squadra[0], "nome_squadra": squadra_casa, "label": "🏠 CASA"},
                "trasferta": {"element": blocchi_squadra[1], "nome_squadra": squadra_trasferta, "label": "✈️ TRASFERTA"},
            }
            
            # XPath del panchinaro: <a> che contiene <div class="deRHiB cQgcrM">
            panchinaro_a_xpath = './a/div[contains(@class, "deRHiB") and contains(@class, "cQgcrM") and @cursor="pointer"]'
            
            for squadra_key, data in blocchi_mapping.items():
                
                # Trova tutti i panchinari all'interno del blocco specifico (casa o trasferta)
                # NOTA: Qui l'elemento di partenza è il <div> con classe "deRHiB cQgcrM", 
                # che si trova all'interno del link <a>, che è un figlio del blocco DooVT.
                panchinari_squadra = data["element"].xpath(panchinaro_a_xpath)
                
                print(f"   Totali {data['label']}: {len(panchinari_squadra)}")

                for panchinaro_elem in panchinari_squadra:
                    try:
                        # ✅ ESTRAI NOME
                        nome_xpath = './/span[contains(@class, "klGMtt")]' 
                        nome_elements = panchinaro_elem.xpath(nome_xpath)
                        
                        if not nome_elements:
                            continue
                        
                        nome_text_content = nome_elements[0].text_content().strip()
                        nome = pulisci_nome(nome_text_content)
                        
                        if nome in giocatori_gia_processati:
                            continue 
                        
                        # ✅ ESTRAI VOTO (Strategia Aria-Valuenow)
                        voto = None
                        aria_voto_xpath = './/span[@role="meter" and @aria-valuenow]'
                        voto_span = panchinaro_elem.xpath(aria_voto_xpath)
                        
                        if voto_span:
                            voto_text = voto_span[0].get('aria-valuenow')
                            if voto_text:
                                voto = float(voto_text.replace(',', '.'))
                        
                        if voto is not None:
                            print(f"   {data['label']} | {nome:20s} | Voto: {voto}")

                            risultati[squadra_key].append({
                                "nome": nome,
                                "voto": voto,
                                "ruolo": "SUB",
                                "titolare": False,
                                "squadra": data["nome_squadra"]
                            })
                            giocatori_gia_processati.add(nome)
                        
                    except Exception as e:
                        continue # Salta in caso di errore di parsing
        
        # --- RIEPILOGO ---
        print(f"\n{'='*60}")
        print(f"✅ RIEPILOGO ESTRAZIONE")
        print(f"{'='*60}")
        print(f"🏠 Casa ({squadra_casa}): {len(risultati['casa'])} giocatori")
        print(f"✈️ Trasferta ({squadra_trasferta}): {len(risultati['trasferta'])} giocatori")
        print(f"{'='*60}\n")
        
        return risultati
        
    except Exception as e:
        print(f"❌ Errore estrazione formazioni: {e}")
        traceback.print_exc()
        return {"casa": [], "trasferta": []}

# ==================== ESTRAZIONE SINGOLA PARTITA ====================

def scrape_match(match_info, target_giornata, supabase):
    """Estrae SEMPRE info base, formazioni/eventi SOLO se F o IC"""
    
    print(f"\n{'─'*80}")
    print(f"📊 {match_info['name'].upper()}")
    print(f"{'─'*80}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        try:
            # Caricamento pagina
            page.goto(match_info['url'], wait_until="domcontentloaded", timeout=90000)
            time.sleep(3)
            
            # Scroll
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(1)
            
            # 1. ESTRAZIONE INFO BASE (SEMPRE)
            print("📋 Estrazione info base partita...")
            basic_info = extract_match_basic_info(page)
            
            # ✅ COSTRUISCI match_data IN BASE ALLO STATO
            if basic_info['stato'] in ['F', 'IC']:
                # ✅ PARTITA F/IC: Aggiorna SOLO stato e goal
                match_data = {
                    'id': match_info['id'],
                    'stato': basic_info['stato'],
                    'gcasa': basic_info['gcasa'],
                    'gtrasferta': basic_info['gtrasferta']
                }
            else:
                # ✅ PARTITA NG: Aggiorna tutto (prima volta)
                match_data = {
                    'id': match_info['id'],
                    'giornata': target_giornata,
                    'casa': basic_info['casa'],
                    'trasferta': basic_info['trasferta'],
                    'data': basic_info['data'],
                    'ora': basic_info['ora'],
                    'stato': basic_info['stato'],
                    'gcasa': basic_info['gcasa'],
                    'gtrasferta': basic_info['gtrasferta'],
                    'href': match_info['url']
                }
            
            print(f"   ✅ Casa: {basic_info['casa']}")
            print(f"   ✅ Trasferta: {basic_info['trasferta']}")
            print(f"   ✅ Stato: {basic_info['stato']}")
            
            # Salva info base su Supabase
            insert_or_update_match(supabase, match_data)
            
            # 2. ESTRAZIONE EVENTI E FORMAZIONI (SOLO SE F o IC)
            eventi = []
            formazioni = {"casa": [], "trasferta": []}
            
            if basic_info['stato'] in ['F', 'IC']:
                print("🔍 Partita finita/in corso → Estrazione eventi e formazioni...")
                
                # Eventi
                eventi = extract_eventi(page)
                print(f"   ✅ {len(eventi)} eventi estratti")
                
                # Formazioni
                formazioni = extract_formazioni(page, basic_info['casa'], basic_info['trasferta'])
                casa_count = len(formazioni['casa'])
                trasferta_count = len(formazioni['trasferta'])
                print(f"   ✅ Casa: {casa_count} giocatori | Trasferta: {trasferta_count} giocatori")
            else:
                print("⏭️  Partita non giocata → Skip eventi/formazioni")
            
            browser.close()
            
            return {
                'match_id': match_info['id'],
                'match_name': match_info['name'],
                'url': match_info['url'],
                'match_data': match_data,
                'eventi': eventi,
                'formazioni': formazioni
            }
            
        except Exception as e:
            print(f"❌ Errore durante l'estrazione: {e}")
            traceback.print_exc()
            browser.close()
            return None

# ==================== CALCOLO FANTAVOTO E STATISTICHE ====================

def calcola_fantavoto(voto, goal, assist, gialli, rossi, rigore_sbagl, is_top=False):
    if voto is None:
        return None
    
    try:
        voto_base = float(voto)
    except ValueError:
        return None 
    
    # Calcolo fantavoto con bonus/malus
    fvoto = voto_base + (goal * 3) + (assist * 1) - (gialli * 0.5) - (rossi * 1) - (rigore_sbagl * 3)
    
    # ✅ BONUS TOP PLAYER (+1)
    if is_top:
        fvoto += 1
    
    return round(fvoto, 2)

def trova_id_giocatore(nome_completo, giocatori_mapping):
    
    # 1. Match esatto (cognome già pulito)
    if nome_completo in giocatori_mapping:
        return giocatori_mapping[nome_completo]
    
    # 2. Estrai cognome (ultima parola)
    parti = nome_completo.split()
    if len(parti) > 1:
        cognome = parti[-1]  # Prende l'ultima parola come cognome
        
        if cognome in giocatori_mapping:
            return giocatori_mapping[cognome]
    
    # 3. Ricerca parziale case-insensitive
    nome_lower = nome_completo.lower()
    
    for nome_db, giocatore_id in giocatori_mapping.items():
        nome_db_lower = nome_db.lower()
        
        # Match se il nome DB è contenuto nel nome completo o viceversa
        if nome_db_lower in nome_lower or nome_lower in nome_db_lower:
            return giocatore_id
    
    # 4. Nessun match trovato
    return None

def identifica_top_player(formazioni):
    top_players = set()
    
    for squadra in ['casa', 'trasferta']:
        giocatori = formazioni[squadra]
        
        # Filtra solo giocatori con voto valido
        voti_validi = []
        for g in giocatori:
            voto = g.get('voto')
            if voto is not None and isinstance(voto, (int, float)):
                voti_validi.append((g['nome'], voto))
        
        if not voti_validi:
            continue
        
        # Trova il voto massimo
        max_voto = max(voti_validi, key=lambda x: x[1])[1]
        
        # Aggiungi TUTTI i giocatori con quel voto (gestisce pareggi)
        for nome, voto in voti_validi:
            if voto == max_voto:
                top_players.add(nome)
                print(f"   🏆 Top Player {squadra}: {nome} (voto {voto})")
    
    return top_players

def processa_eventi_e_voti(all_match_data, giocatori_mapping):
    """Processa dati e mostra bonus/malus e top player"""
    statistiche_per_supabase = []
    giocatori_non_trovati = set()
    
    for match in all_match_data:
        # Skip se non ci sono formazioni
        if not match['formazioni']['casa'] and not match['formazioni']['trasferta']:
            continue
        
        match_id = match['match_id']
        match_name = match['match_name']
        
        print(f"\n{'='*80}")
        print(f"⚽ ANALISI PARTITA: {match_name.upper()}")
        print(f"{'='*80}")
        
        # Identifica top players
        top_players = identifica_top_player(match['formazioni'])
        
        # Mappa voti
        giocatori_in_campo = {}
        for squadra in ['casa', 'trasferta']:
            for player_data in match['formazioni'][squadra]:
                nome = player_data['nome']
                voto = player_data['voto']
                is_titolare = player_data.get('titolare', True)
                nome_squadra = player_data.get('squadra', '')
                
                # MATCHING CON CHIAVE COMPOSTA (nome, squadra)
                if is_titolare:
                    giocatore_id = giocatori_mapping['cognomi'].get((nome, nome_squadra), None)
                else:
                    giocatore_id = giocatori_mapping['nomi_completi'].get((nome, nome_squadra), None)
                
                if giocatore_id is None:
                    giocatori_non_trovati.add(f"{nome} ({nome_squadra}) ({'TIT' if is_titolare else 'SUB'})")
                
                giocatori_in_campo[nome] = {
                    'IDpartita': match_id,
                    'IDgiocatore': giocatore_id,
                    'goal': 0,
                    'assist': 0,
                    'gialli': 0,
                    'rossi': 0,
                    'rigori sbagliati': 0,
                    'voto': voto,
                    'titolare': is_titolare,
                    'top': nome in top_players,
                    'squadra': nome_squadra
                }
        
        # Aggiorna con eventi
        print(f"\n📋 EVENTI E BONUS/MALUS")
        print(f"{'─'*80}")
        
        for evento in match['eventi']:
            tipo = evento['tipo']
            descrizione = evento['descrizione']
            
            if tipo == "⚽ GOL":
                # ✅ ESTRAI MARCATORE E ASSISTMAN
                descrizione_pulita = descrizione.replace('[Rigore]', '').strip()
                
                # Cerca pattern "Marcatore (Assist: Assistman)"
                assist_match = re.search(r'^(.+?)\s*\(Assist:\s*(.+?)\)\s*$', descrizione_pulita)
                
                if assist_match:
                    # GOL CON ASSIST
                    marcatore_nome_evento = assist_match.group(1).strip()
                    assistman_nome_evento = assist_match.group(2).strip()
                    
                    # ✅ USA MATCHING FLESSIBILE
                    marcatore_nome = trova_giocatore_in_campo(marcatore_nome_evento, giocatori_in_campo)
                    assistman_nome = trova_giocatore_in_campo(assistman_nome_evento, giocatori_in_campo)
                    
                    # ✅ GOAL AL MARCATORE
                    if marcatore_nome:
                        giocatori_in_campo[marcatore_nome]['goal'] += 1
                        print(f"   ⚽ GOL: {marcatore_nome} (+3 bonus)")
                    else:
                        print(f"   ⚠️  GOL: {marcatore_nome_evento} (non trovato in campo)")
                    
                    # ✅ ASSIST ALL'ASSISTMAN (NON GOL!)
                    if assistman_nome:
                        giocatori_in_campo[assistman_nome]['assist'] += 1
                        print(f"   🎯 ASSIST: {assistman_nome} (+1 bonus)")
                    else:
                        print(f"   ⚠️  ASSIST: {assistman_nome_evento} (non trovato in campo)")
                else:
                    # GOL SENZA ASSIST
                    marcatore_nome_evento = descrizione_pulita.strip()
                    marcatore_nome = trova_giocatore_in_campo(marcatore_nome_evento, giocatori_in_campo)
                    
                    if marcatore_nome:
                        giocatori_in_campo[marcatore_nome]['goal'] += 1
                        print(f"   ⚽ GOL: {marcatore_nome} (+3 bonus)")
                    else:
                        print(f"   ⚠️  GOL: {marcatore_nome_evento} (non trovato in campo)")

            elif tipo == "❌ RIGORE SBAGLIATO":
                giocatore_nome_evento = descrizione.strip()
                giocatore_nome = trova_giocatore_in_campo(giocatore_nome_evento, giocatori_in_campo)
                
                if giocatore_nome:
                    giocatori_in_campo[giocatore_nome]['rigori sbagliati'] += 1
                    print(f"   ❌ RIGORE SBAGLIATO: {giocatore_nome} (-3 malus)")
                else:
                    print(f"   ⚠️  RIGORE SBAGLIATO: {giocatore_nome_evento} (non trovato in campo)")
                    
            elif tipo == "🟨 CARTELLINO":
                giocatore_nome_evento = descrizione.strip()
                giocatore_nome = trova_giocatore_in_campo(giocatore_nome_evento, giocatori_in_campo)
                
                if giocatore_nome:
                    giocatori_in_campo[giocatore_nome]['gialli'] += 1
                    print(f"   🟨 GIALLO: {giocatore_nome} (-0.5 malus)")
                else:
                    print(f"   ⚠️  GIALLO: {giocatore_nome_evento} (non trovato in campo)")
            
            elif tipo == "🟥 CARTELLINO":
                giocatore_nome_evento = descrizione.strip()
                giocatore_nome = trova_giocatore_in_campo(giocatore_nome_evento, giocatori_in_campo)
                
                if giocatore_nome:
                    giocatori_in_campo[giocatore_nome]['rossi'] += 1
                    print(f"   🟥 ROSSO: {giocatore_nome} (-1 malus)")
                else:
                    print(f"   ⚠️  ROSSO: {giocatore_nome_evento} (non trovato in campo)")
        
        # Finalizzazione
        print(f"\n📊 FANTAVOTI CALCOLATI")
        print(f"{'─'*80}")
        
        for key, data in giocatori_in_campo.items():
            if data['IDgiocatore'] is not None:
                
                # ==========================================================
                # ✅ GESTIONE UNIFICATA DEL VOTO E CONVERSIONE A FLOAT
                # ==========================================================
                voto_grezzo = data['voto']
                voto_per_calcolo = 0.0
                voto_stampa = "SV"
                
                try:
                    voto_str = str(voto_grezzo).replace(',', '.') 
                    voto_float = float(voto_str)
                    
                    if voto_float > 0:
                        voto_per_calcolo = voto_float
                        voto_stampa = f"{voto_float:.1f}"
                    else:
                         voto_per_calcolo = 0.0
                         voto_stampa = f"{voto_float:.1f}"

                except (ValueError, TypeError):
                    pass 
                
                # ==========================================================
                # FINE GESTIONE VOTO
                
                gialli = data['gialli']
                rossi = data['rossi']
                
                # Se ha rosso E giallo, conta solo il rosso
                if rossi > 0 and gialli > 0:
                    gialli = 0
                    rossi = 1
                
                fvoto = calcola_fantavoto(
                    voto=voto_per_calcolo,
                    goal=data['goal'],
                    assist=data['assist'],
                    gialli=gialli,
                    rossi=rossi,
                    rigore_sbagl=data['rigori sbagliati'],
                    is_top=data['top']
                )
                
                # ✅ PRINT DETTAGLIATO
                bonus_malus = []
                
                if data['goal'] > 0:
                    bonus_malus.append(f"+{data['goal']*3} gol")
                if data['assist'] > 0:
                    bonus_malus.append(f"+{data['assist']} assist")
                if gialli > 0:
                    bonus_malus.append(f"-{gialli*0.5} giallo")
                if rossi > 0:
                    bonus_malus.append(f"-{rossi} rosso")
                if data['rigori sbagliati'] > 0:
                    bonus_malus.append(f"-{data['rigori sbagliati']*3} rigori sbagliati")
                    
                bonus_str = " | ".join(bonus_malus) if bonus_malus else "nessun bonus/malus"
                top_str = " 🏆 +1 TOP" if data['top'] else ""
                
                print(f"   {key:20s} | Voto: {voto_stampa:4s} → FV: {fvoto:.1f} | {bonus_str}{top_str}")
                
                payload = {
                    'IDpartita': data['IDpartita'],
                    'IDgiocatore': data['IDgiocatore'],
                    'goal': data['goal'],
                    'rigore_sbagliato': data['rigori sbagliati'],
                    'assist': data['assist'],
                    'gialli': gialli,
                    'rossi': rossi,
                    'voto': voto_per_calcolo,
                    'fvoto': fvoto,
                    'titolare': data['titolare'],
                    'top': data['top']
                }
                statistiche_per_supabase.append(payload)
    
    if giocatori_non_trovati:
        print(f"\n{'='*80}")
        print(f"⚠️  GIOCATORI NON TROVATI NEL DB ({len(giocatori_non_trovati)})")
        print(f"{'='*80}")
        for g in sorted(giocatori_non_trovati):
            print(f"   • {g}")
    
    return statistiche_per_supabase

def insert_statistiche_supabase(supabase, statistiche_list):
    """
    Inserisce le statistiche in Supabase tramite upsert
    Chiave primaria composta: (IDgiocatore, IDpartita)
    ✅ Mostra i duplicati trovati
    """
    try:
        if not statistiche_list:
            print("⚠️  Nessuna statistica da inserire")
            return False
        
        # ✅ DEDUPLICA CON TRACCIAMENTO
        stats_dict = {}
        duplicati = []
        
        for stat in statistiche_list:
            key = (stat['IDpartita'], stat['IDgiocatore'])
            
            if key in stats_dict:
                # ✅ DUPLICATO TROVATO!
                duplicati.append({
                    'IDpartita': stat['IDpartita'],
                    'IDgiocatore': stat['IDgiocatore'],
                    'vecchio': stats_dict[key],
                    'nuovo': stat
                })
            
            stats_dict[key] = stat  # Mantiene l'ultimo
        
        unique_stats = list(stats_dict.values())
        
        print(f"📊 Record originali: {len(statistiche_list)}")
        print(f"🔹 Record unici (dopo deduplica): {len(unique_stats)}")
        
        # ✅ MOSTRA DUPLICATI
        if duplicati:
            print(f"\n⚠️  Trovati {len(duplicati)} duplicati:")
            for dup in duplicati[:10]:  # Mostra max 10
                print(f"\n   🔄 IDgiocatore: {dup['IDgiocatore']}, IDpartita: {dup['IDpartita']}")
                print(f"      VECCHIO → goal: {dup['vecchio'].get('goal')}, assist: {dup['vecchio'].get('assist')}, voto: {dup['vecchio'].get('voto')}")
                print(f"      NUOVO   → goal: {dup['nuovo'].get('goal')}, assist: {dup['nuovo'].get('assist')}, voto: {dup['nuovo'].get('voto')}")
            
            if len(duplicati) > 10:
                print(f"   ... e altri {len(duplicati) - 10} duplicati")
        
        # ✅ BATCH UPSERT
        print(f"\n📥 Inserimento {len(unique_stats)} record...")
        response = supabase.table('Statistiche').upsert(
            unique_stats,
            on_conflict='IDgiocatore,IDpartita'
        ).execute()
        
        print(f"✅ {len(unique_stats)} statistiche inserite/aggiornate in Supabase")
        return True
        
    except Exception as e:
        print(f"❌ Errore durante l'inserimento batch in Supabase: {e}")
        traceback.print_exc()
        return False

# ==================== MAIN ====================

def main():
    print(f"\n{'='*80}")
    print(f"SCRAPER UNIFICATO SERIE A - GIORNATA {TARGET_GIORNATA}")
    print(f"Data: {datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"{'='*80}")
    
    # 0. Inizializzazione Supabase
    supabase = init_supabase()
    if not supabase:
        print("❌ Impossibile continuare senza connessione Supabase.")
        return
        
    # 0.1 Caricamento Mappatura Giocatori 
    giocatori_mapping = fetch_giocatori_mapping(supabase)
    if not giocatori_mapping:
         print("⚠️ Mappatura Giocatori vuota. Le statistiche non verranno caricate.")

    # Fase 1: Recupera lista partite
    print(f"\n{'='*80}")
    print(f"FASE 1: RECUPERO LISTA PARTITE")
    print(f"{'='*80}")
    
    matches = fetch_giornata_matches(TARGET_GIORNATA)
    
    if not matches:
        print("\n❌ Nessuna partita trovata. Interruzione.")
        return
    
    # Fase 2: Scraping di ogni partita
    print(f"\n{'='*80}")
    print(f"FASE 2: ESTRAZIONE DATI PARTITE ({len(matches)} totali)")
    print(f"{'='*80}")
    
    all_data = []
    
    for idx, match in enumerate(matches, 1):
        print(f"\n[{idx}/{len(matches)}] Elaborazione in corso...")
        match_data = scrape_match(match, TARGET_GIORNATA, supabase)
        
        if match_data:
            all_data.append(match_data)
            time.sleep(2)  # Pausa tra richieste
    
    # Fase 3: Pre-elaborazione e caricamento statistiche
    print(f"\n{'='*80}")
    print(f"FASE 3: ELABORAZIONE STATISTICHE")
    print(f"{'='*80}")
    
    statistiche_per_supabase = []
    
    if all_data and giocatori_mapping:
        statistiche_per_supabase = processa_eventi_e_voti(all_data, giocatori_mapping)
        print(f"✅ {len(statistiche_per_supabase)} record di statistiche preparati.")
        
        if statistiche_per_supabase:
            insert_statistiche_supabase(supabase, statistiche_per_supabase)
    else:
        print("⚠️ Nessun dato da elaborare o mappatura mancante.")

    # Salvataggio risultati completi (Opzionale)
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"giornata_{TARGET_GIORNATA}_{timestamp}_complete.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    
    # Riepilogo finale
    print(f"\n{'='*80}")
    print(f"✅ ESTRAZIONE E CARICAMENTO COMPLETATI!")
    print(f"{'='*80}")
    print(f"📁 Dati grezzi salvati: {filename}")
    print(f"📊 Partite elaborate: {len(all_data)}/{len(matches)}")
    
    # Conta partite per stato
    partite_finite = sum(1 for m in all_data if m['match_data']['stato'] == 'F')
    partite_in_corso = sum(1 for m in all_data if m['match_data']['stato'] == 'IC')
    partite_non_giocate = sum(1 for m in all_data if m['match_data']['stato'] == 'NG')
    
    print(f"   • Finite (F): {partite_finite}")
    print(f"   • In corso (IC): {partite_in_corso}")
    print(f"   • Non giocate (NG): {partite_non_giocate}")
    print(f"📊 Record Statistiche caricati: {len(statistiche_per_supabase)}")
    print(f"{'='*80}\n")
  
    # Fase 4: Esecuzione query automatiche
    print(f"\n{'='*80}")
    print(f"FASE 4: ESECUZIONE QUERY AUTOMATICHE")
    print(f"{'='*80}")
  
    try:
        # 1. Update Statistiche
        print("🔄 Esecuzione query: Update Statistiche...")
        supabase.rpc('update_statistiche').execute()
        print("✅ Query 'Update Statistiche' completata")
        
        # 2. Update Prezzo
        print("🔄 Esecuzione query: Update Prezzo...")
        supabase.rpc('update_prezzo').execute()
        print("✅ Query 'Update Prezzo' completata")
        
        print(f"{'='*80}")
        print("✅ TUTTE LE QUERY AUTOMATICHE COMPLETATE!")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"❌ Errore durante l'esecuzione delle query: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERRORE FATALE: {e}")
        traceback.print_exc()
        import sys
        sys.exit(1)

