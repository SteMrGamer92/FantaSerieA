#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from playwright.sync_api import sync_playwright
from playwright._impl._errors import TimeoutError as PlaywrightTimeoutError
from lxml import html
from supabase import create_client, Client 
import time
import re
import json
import datetime
import traceback
import os
import pytz

# ==================== CONFIGURAZIONE GLOBALE ====================
URL_TORNEO = 'https://www.sofascore.com/it/torneo/calcio/italy/serie-a/23#id:76457#tab:matches'
TARGET_GIORNATA = 12

BUTTON_GIORNATA_XPATH = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[2]/div[2]/div[2]/div/div/div[1]/div/div/button"
GIORNATA_LI_SELECTOR_CSS = 'ul.dropdown__list li:has-text("Round {}")'
CONSENT_BUTTON_SELECTOR = 'button:has-text("Acconsento"), button:has-text("Consent")'

# XPath Eventi
XPATH_BASE_EVENTI = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[4]/div/div/div[{n}]"
XPATH_TESTO_1 = "./div/div[2]/div/span[1]"
XPATH_TESTO_2 = "./div/div[2]/div/span[2]"

# XPath Formazioni
BASE_FORMAZIONI = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[2]/div[1]/div/div/div/div[2]/div/div/div[2]/div/div/div[1]/div[2]/div/div/div"
BASE_PANCHINARI = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[2]/div[1]/div/div/div/div[2]/div/div/div[2]/div/div/div[2]/div[1]/div/div[3]/div[2]"

# XPath Info Partita
XPATH_SQUADRA_CASA = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div/div[1]/div/a/div/div/bdi"
XPATH_SQUADRA_TRASFERTA = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div/div[3]/div/a/div/div/bdi"
XPATH_GOAL_CASA = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div[1]/div[2]/div/div/div[1]/span/span[1]"
XPATH_GOAL_TRASFERTA = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div[1]/div[2]/div/div/div[1]/span/span[3]"
XPATH_STATUS = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div[1]/div[2]/div/div/div[2]/div/span/span"
                
# ===== CONFIGURAZIONE SUPABASE =====
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://ipqxjudlxcqacgtmpkzx.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlwcXhqdWRseGNxYWNndG1wa3p4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTEyNjU3OSwiZXhwIjoyMDc0NzAyNTc5fQ.9nMpSeM-p5PvnF3rwMeR_zzXXocyfzYV24vau3AcDso')

ORA_LEGALE_OFFSET = 1

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
        # Seleziona id, nome (cognome) e nomeint (nome completo)
        response = supabase.table('Giocatori').select('id, nome, nomeint').execute()
        
        for record in response.data:
            giocatore_id = record.get('id')
            
            # Campo "nome" = cognome (per titolari)
            nome_cognome = pulisci_nome(record.get('nome', ''))
            if nome_cognome and giocatore_id is not None:
                mapping['cognomi'][nome_cognome] = giocatore_id
            
            # Campo "nomeint" = nome completo (per panchinari)
            nome_completo = pulisci_nome(record.get('nomeint', ''))
            if nome_completo and giocatore_id is not None:
                mapping['nomi_completi'][nome_completo] = giocatore_id
        
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
    
    # 2. ✅ RIMUOVE SOFT HYPHEN (&shy;, &#173;, \u00ad)
    cleaned = cleaned.replace('&shy;', '')
    cleaned = cleaned.replace('&#173;', '')
    cleaned = cleaned.replace('\u00ad', '')  # Unicode soft hyphen
    cleaned = cleaned.replace('\xad', '')     # Byte soft hyphen
    
    # 3. Normalizza spazi multipli
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned.strip()

def get_current_round(page, button_xpath):
    """Estrae il numero del round dal bottone"""
    try:
        text = page.locator(f'xpath={button_xpath}').text_content().strip()
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

# ==================== ESTRAZIONE PARTITE ====================

def fetch_giornata_matches(target_giornata):
    """Scarica la lista delle partite della giornata target"""
    
    print(f"\n{'='*80}")
    print(f"FASE 1: RECUPERO PARTITE GIORNATA {target_giornata}")
    print(f"{'='*80}")
    
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
        
        # Verifica giornata attuale
        current_round = get_current_round(page, BUTTON_GIORNATA_XPATH)
        print(f"📅 Giornata visualizzata: {current_round}")
        
        if current_round != target_giornata:
            print(f"🔄 Cambio giornata da {current_round} a {target_giornata}...")
            
            try:
                page.locator(f'xpath={BUTTON_GIORNATA_XPATH}').click(timeout=30000, force=True)
                time.sleep(1)
                
                giornata_selector = GIORNATA_LI_SELECTOR_CSS.format(target_giornata)
                locator_li = page.locator(giornata_selector)
                locator_li.scroll_into_view_if_needed(timeout=10000)
                time.sleep(1)
                
                element_handle = locator_li.element_handle()
                if element_handle:
                    element_handle.evaluate('el => el.click()')
                    print(f"✅ Giornata {target_giornata} selezionata")
                    
                    try:
                         page.wait_for_function(f"""() => 
                             document.querySelector("button[aria-haspopup='true']") && 
                             document.querySelector("button[aria-haspopup='true']").textContent.includes("Round {target_giornata}")
                         """, timeout=15000)
                         print("✅ Attesa cambio Round verificata.")
                    except PlaywrightTimeoutError:
                         print("⚠️ Timeout nella verifica del cambio Round. Proseguo...")

                    time.sleep(4)
                else:
                    raise Exception("Elemento giornata non trovato")
                    
            except Exception as e:
                print(f"❌ Errore selezione giornata: {e}")
                browser.close()
                return []
        
        # Attesa caricamento partite
        page.wait_for_selector('a[href*="/match/"]', timeout=30000)
        time.sleep(5)
        
        # Scroll e download HTML
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(3)
        html_content = page.content()
        
        browser.close()
        
        # Estrazione href partite
        tree = html.fromstring(html_content)
        xpath = "//div[contains(@class, 'Box')]//a[contains(@href, '/it/football/match/')]"
        elements = tree.xpath(xpath)
        
        matches = []
        for element in elements:
            href = element.get('href', '')
            if href:
                if not href.startswith('https://'):
                    href = f"https://www.sofascore.com{href}"
                
                match_id = re.search(r'#id:(\d+)', href)
                match_id = int(match_id.group(1)) if match_id else 'N/A'
                
                match_name = re.search(r'/match/([^/]+)/', href)
                match_name = match_name.group(1) if match_name else f"match_{match_id}"
                
                matches.append({
                    'id': match_id,
                    'url': href,
                    'name': match_name
                })
        
        matches_unici = {m['id']: m for m in matches if m['id'] != 'N/A'}
        final_matches = list(matches_unici.values())
        
        print(f"\n✅ Trovate {len(final_matches)} partite per la giornata {target_giornata}")
        for m in final_matches:
             print(f"   • {m['name']} (ID: {m['id']})")
        
        return final_matches

# ==================== ESTRAZIONE DATI PARTITA ====================

def extract_match_basic_info(tree):
    """Estrae squadre, data, ora, stato e goal"""
    
    # Squadre
    squadra_casa = None
    squadra_trasferta = None
    
    casa_elements = tree.xpath(XPATH_SQUADRA_CASA)
    if casa_elements:
        squadra_casa = casa_elements[0].text_content().strip()
    
    trasferta_elements = tree.xpath(XPATH_SQUADRA_TRASFERTA)
    if trasferta_elements:
        squadra_trasferta = trasferta_elements[0].text_content().strip()
    
    # Fallback se non trovati
    if not squadra_casa or not squadra_trasferta:
        xpath_teams_fallback = "//a[contains(@href, '/team/')]//bdi"
        team_elements = tree.xpath(xpath_teams_fallback)
        if len(team_elements) >= 2:
            squadra_casa = team_elements[0].text_content().strip()
            squadra_trasferta = team_elements[1].text_content().strip()
    
    # Data e Ora
    data = None
    ora = None
    
    data_xpath = "//span[contains(text(), 'Oggi') or contains(text(), 'Domani') or contains(text(), '/202') or contains(text(), '-202')]"
    data_elements = tree.xpath(data_xpath)
    
    cest = pytz.timezone('Europe/Rome')
    
    for element in data_elements:
        text = element.text_content().strip()
        if text.lower() == 'oggi':
            data = datetime.datetime.now(cest).strftime('%Y-%m-%d')
            break
        elif text.lower() == 'domani':
            data = (datetime.datetime.now(cest) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            break
        elif re.match(r'\d{2}[/-]\d{2}[/-]\d{4}', text):
            parsed_date = datetime.datetime.strptime(text, '%d/%m/%Y') if '/' in text else datetime.datetime.strptime(text, '%d-%m-%Y')
            data = parsed_date.strftime('%Y-%m-%d')
            break
        elif re.match(r'\d{4}-\d{2}-\d{2}', text):
            data = text
            break
    
    ora_xpath = "//span[contains(text(), ':') and string-length(text()) <= 5 and string-length(text()) >= 4]"
    ora_elements = tree.xpath(ora_xpath)
    
    for element in ora_elements:
        text = element.text_content().strip()
        if re.match(r'\d{2}:\d{2}', text):
            ora_parsed = datetime.datetime.strptime(text, '%H:%M')
            ora_corretta = ora_parsed + datetime.timedelta(hours=ORA_LEGALE_OFFSET)
            ora = ora_corretta.strftime('%H:%M') + ":00"
            break
    
    # Stato
    stato = None
    
    if data and ora:
        match_datetime = datetime.datetime.strptime(f"{data} {ora[:5]}", '%Y-%m-%d %H:%M')
        match_datetime = cest.localize(match_datetime)
        now = datetime.datetime.now(cest)
        
        # Verifica se finita
        finita_elements = tree.xpath(XPATH_STATUS)
        is_finished = False
        if finita_elements:
            status_text = finita_elements[0].text_content().strip().lower()
            if 'finita' in status_text or 'finished' in status_text:
                is_finished = True
        
        if is_finished:
            stato = 'F'
        elif now >= match_datetime:
            stato = 'IC'
        else:
            stato = 'NG'
    
    # Goal (solo se F o IC)
    goalcasa = None
    goaltrasferta = None
    
    if stato in ['F', 'IC']:
        goal_casa_elements = tree.xpath(XPATH_GOAL_CASA)
        goal_trasferta_elements = tree.xpath(XPATH_GOAL_TRASFERTA)
        
        if goal_casa_elements and goal_trasferta_elements:
            try:
                goalcasa = int(goal_casa_elements[0].text_content().strip())
                goaltrasferta = int(goal_trasferta_elements[0].text_content().strip())
            except ValueError:
                pass
    
    return {
        'casa': squadra_casa,
        'trasferta': squadra_trasferta,
        'data': data,
        'ora': ora,
        'stato': stato,
        'gcasa': goalcasa,
        'gtrasferta': goaltrasferta
    }

# ==================== ESTRAZIONE EVENTI ====================

def extract_eventi(page):
    """Estrae gli eventi della partita"""
    
    eventi = []
    i = 1
    
    while True:
        current_xpath = XPATH_BASE_EVENTI.format(n=i)
        
        if get_locator_count(page, current_xpath) == 0:
            break
        
        try:
            container = page.locator(f"xpath={current_xpath}").first
            
            contenuto_1 = get_inner_text_safe(container.locator(f"xpath={XPATH_TESTO_1}"))
            contenuto_2 = get_inner_text_safe(container.locator(f"xpath={XPATH_TESTO_2}"))
            text_full = get_text_content_safe(container)
            
            if not contenuto_1 and not contenuto_2 and not text_full:
                i += 1
                continue
            
            # Classificazione evento
            tipo = "ALTRO"
            descrizione = ""
            
            if "GOL" in text_full:
                tipo = "⚽ GOL"
                descrizione = f"{contenuto_1} (Assist: {contenuto_2})" if contenuto_2 else contenuto_1
            elif "CARTELLINO" in text_full or "FALLO" in text_full:
                if "GIALLO" in text_full:
                    tipo = "🟨 CARTELLINO"
                else: 
                    tipo = "🟥 CARTELLINO"
                descrizione = contenuto_1
            elif contenuto_1 and contenuto_2:
                tipo = "🔄 SOSTITUZIONE"
                descrizione = f"Esce: {contenuto_1}, Entra: {contenuto_2}"
            
            if tipo != "ALTRO":
                eventi.append({
                    'tipo': tipo,
                    'descrizione': descrizione
                })
            
            i += 1
            
        except Exception:
            i += 1
            continue
    
    return eventi

# ==================== ESTRAZIONE FORMAZIONI ====================

def extract_formazioni(tree):
    """Estrae le formazioni (titolari + panchinari) e i voti delle due squadre"""
    
    risultati = {"casa": [], "trasferta": []}
    squadre = [("casa", 2, "1"), ("trasferta", 3, "3")]  # (nome, idx_titolari, idx_panchina)
    
    for squadra, idx_titolari, idx_panchina_base in squadre:
        base = f"{BASE_FORMAZIONI}[{idx_titolari}]/div[1]"
        
        # ===== TITOLARI =====
        
        # Portiere (Elemento 1)
        nome_p = tree.xpath(f"{base}/div[1]/div/div/div/div/div[2]/span")
        voto_p = tree.xpath(f"{base}/div[1]/div/div/div/div/div[1]/div[3]/div/div/span/div/span")
        
        if nome_p:
            nome = pulisci_nome(nome_p[0].text_content())
            voto = voto_p[0].text_content().strip().replace(',', '.') if voto_p else "-"
            
            # Filtra solo voti validi
            if voto != '-':
                try:
                    voto_float = float(voto)
                except ValueError:
                    voto_float = None
                
                risultati[squadra].append({"nome": nome, "voto": voto_float, "ruolo": "POR", "titolare": True})
        
        # Altre linee (linea 2 in poi)
        linea = 2
        while True:
            trovati = False
            for slot in range(1, 6): 
                nome_path = f"{base}/div[{linea}]/div/div[{slot}]/div/div/div[2]/span"
                voto_path = f"{base}/div[{linea}]/div/div[{slot}]/div/div/div[1]/div[3]/div/div/span/div/span"
                
                nome_elem = tree.xpath(nome_path)
                if not nome_elem:
                    continue
                
                voto_elem = tree.xpath(voto_path)
                nome = pulisci_nome(nome_elem[0].text_content())
                voto = voto_elem[0].text_content().strip().replace(',', '.') if voto_elem else "-"
                
                # Filtra solo voti validi
                if voto != '-':
                    try:
                        voto_float = float(voto)
                    except ValueError:
                        voto_float = None 
                    
                    risultati[squadra].append({"nome": nome, "voto": voto_float, "ruolo": f"L{linea}", "titolare": True})
                    trovati = True
            
            if not trovati:
                break
            linea += 1
            if linea > 7:  # Safety break
                break
        
        # ===== PANCHINARI =====
        
        idx_panchina = 1
        while True:
            base_giocatore = f"{BASE_PANCHINARI}/div[{idx_panchina_base}]/a[{idx_panchina}]"
            
            nome_path_panchina = f"{base_giocatore}/div/div[3]/div[1]/div[1]/span/span"
            voto_path_panchina = f"{base_giocatore}/div/div[3]/div[2]/div/div"
            
            nome_elem = tree.xpath(nome_path_panchina)
            
            if not nome_elem:
                break
            
            voto_elem = tree.xpath(voto_path_panchina)
            
            nome = pulisci_nome(nome_elem[0].text_content())
            voto = voto_elem[0].text_content().strip().replace(',', '.') if voto_elem else "-"
            
            # Filtra solo voti validi (panchinari che sono entrati)
            if nome and voto != '-':
                try:
                    voto_float = float(voto)
                except ValueError:
                    voto_float = None
                
                risultati[squadra].append({"nome": nome, "voto": voto_float, "ruolo": "SUB", "titolare": False})
            
            idx_panchina += 1
            if idx_panchina > 25:  # Safety break
                break
    
    return risultati

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
            
            # Download HTML
            html_content = page.content()
            tree = html.fromstring(html_content)
            
            # 1. ESTRAZIONE INFO BASE (SEMPRE)
            print("📋 Estrazione info base partita...")
            basic_info = extract_match_basic_info(tree)
            
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
                formazioni = extract_formazioni(tree)
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

def calcola_fantavoto(voto, goal, assist, gialli, rossi):
    """Calcola il Fantavoto"""
    if voto is None:
        return None
    
    try:
        voto_base = float(voto)
    except ValueError:
        return None 
        
    fvoto = voto_base + (goal * 3) + (assist * 1) - (gialli * 0.5) - (rossi * 1)
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

def processa_eventi_e_voti(all_match_data, giocatori_mapping):
    """Processa dati usando cognome per titolari, nome completo per panchinari"""
    statistiche_per_supabase = []
    giocatori_non_trovati = set()
    
    for match in all_match_data:
        # Skip se non ci sono formazioni
        if not match['formazioni']['casa'] and not match['formazioni']['trasferta']:
            continue
        
        match_id = match['match_id']
        
        # Mappa voti
        giocatori_in_campo = {}
        for squadra in ['casa', 'trasferta']:
            for player_data in match['formazioni'][squadra]:
                nome = player_data['nome']
                voto = player_data['voto']
                is_titolare = player_data.get('titolare', True)  # Default True per retrocompatibilità
                
                # ✅ LOGICA MATCHING DIFFERENZIATA
                if is_titolare:
                    # Titolari: usa cognome
                    giocatore_id = giocatori_mapping['cognomi'].get(nome, None)
                else:
                    # Panchinari: usa nome completo
                    giocatore_id = giocatori_mapping['nomi_completi'].get(nome, None)
                
                if giocatore_id is None:
                    giocatori_non_trovati.add(f"{nome} ({'titolare' if is_titolare else 'panchinaro'})")
                
                giocatori_in_campo[nome] = {
                    'IDpartita': match_id,
                    'IDgiocatore': giocatore_id, 
                    'goal': 0,
                    'assist': 0,
                    'gialli': 0,
                    'rossi': 0,
                    'voto': voto
                }
        
        # Aggiorna con eventi
        for evento in match['eventi']:
            tipo = evento['tipo']
            descrizione = evento['descrizione']
            
            if tipo == "⚽ GOL":
                marcatore_match = re.search(r'(.+?)(?: \(Assist: .+\))?$', descrizione)
                if marcatore_match:
                    marcatore_nome = marcatore_match.group(1).strip()
                    if marcatore_nome in giocatori_in_campo:
                        giocatori_in_campo[marcatore_nome]['goal'] += 1
                
                assist_match = re.search(r'\(Assist: (.+?)\)', descrizione)
                if assist_match:
                    assistman_nome = assist_match.group(1).strip()
                    if assistman_nome in giocatori_in_campo:
                        giocatori_in_campo[assistman_nome]['assist'] += 1
                        
            elif tipo == "🟨 CARTELLINO":
                giocatore_nome = descrizione.strip()
                if giocatore_nome in giocatori_in_campo:
                     giocatori_in_campo[giocatore_nome]['gialli'] += 1
            
            elif tipo == "🟥 CARTELLINO":
                giocatore_nome = descrizione.strip()
                if giocatore_nome in giocatori_in_campo:
                     giocatori_in_campo[giocatore_nome]['rossi'] += 1
        
        # Finalizzazione
        for key, data in giocatori_in_campo.items():
            if data['IDgiocatore'] is not None:
                gialli = data['gialli']
                rossi = data['rossi']
                
                if rossi > 0 and gialli > 0:
                    gialli = 0 
                    rossi = 1
                
                fvoto = calcola_fantavoto(
                    voto=data['voto'], 
                    goal=data['goal'], 
                    assist=data['assist'], 
                    gialli=gialli, 
                    rossi=rossi
                )
                
                payload = {
                    'IDpartita': data['IDpartita'],
                    'IDgiocatore': data['IDgiocatore'], 
                    'goal': data['goal'],
                    'assist': data['assist'],
                    'gialli': gialli,
                    'rossi': rossi,
                    'voto': data['voto'],
                    'fvoto': fvoto
                }
                statistiche_per_supabase.append(payload)
    
    if giocatori_non_trovati:
        print(f"\n⚠️ Giocatori trovati nello scraping ma NON nel DB: {', '.join(giocatori_non_trovati)}")
    
    return statistiche_per_supabase

def insert_statistiche_supabase(supabase: Client, data_list):
    """Inserisce o aggiorna le statistiche nella tabella 'Statistiche'"""
    
    print(f"\n{'='*80}")
    print(f"FASE 4: CARICAMENTO STATISTICHE IN SUPABASE")
    print(f"{'='*80}")
    
    if not data_list:
        print("⚠️ Nessun record di statistiche da caricare.")
        return False
    
    try:
        response = supabase.table('Statistiche').upsert(
            data_list, 
            on_conflict="IDpartita, IDgiocatore"
        ).execute()
        
        print(f"✅ Inserimento/Aggiornamento completato per {len(data_list)} record.")
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

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERRORE FATALE: {e}")
        traceback.print_exc()
        import sys
        sys.exit(1)
