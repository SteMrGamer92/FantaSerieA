from playwright.sync_api import sync_playwright
from lxml import html
import time
import re
import os
from datetime import datetime, timedelta
import pytz
from supabase import create_client, Client
import sys
import traceback

# ===== CONFIGURAZIONE SUPABASE =====
SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://ipqxjudlxcqacgtmpkzx.supabase.co')
SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlwcXhqdWRseGNxYWNndG1wa3p4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTEyNjU3OSwiZXhwIjoyMDc0NzAyNTc5fQ.9nMpSeM-p5PvnF3rwMeR_zzXXocyfzYV24vau3AcDso')

def init_supabase():
    """Inizializza il client Supabase"""
    try:
        client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Connesso a Supabase")
        return client
    except Exception as e:
        print(f"❌ Errore connessione Supabase: {e}")
        traceback.print_exc()
        return None
        
def save_html_debug(html_content, filename="debug_match.html"):
    """Salva HTML per debug (funziona su GitHub Actions e locale)"""
    try:
        import os
        
        # Su GitHub Actions usa /tmp, localmente Desktop
        if os.path.exists('/tmp'):
            save_dir = '/tmp'
        else:
            save_dir = os.path.join(os.path.expanduser("~"), "Desktop")
        
        filepath = os.path.join(save_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"💾 HTML salvato: {filepath} ({len(html_content):,} byte)")
        return filepath
    except Exception as e:
        print(f"❌ Errore salvataggio HTML: {e}")
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

def fetch_tournament_page(url, target_giornata=10):
    try:
        with sync_playwright() as p:
            print(f"  Avvio browser...")
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            page = context.new_page()

            print(f"  Caricamento: {url}")
            page.goto(url, wait_until='domcontentloaded', timeout=60000)

            # === BOTTONE TENDINA ===
            button_xpath = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[2]/div[2]/div[2]/div/div/div[1]/div/div/button"
            print("  Apertura tendina...")
            try:
                page.wait_for_selector(f'xpath={button_xpath}', state='visible', timeout=30000)
                page.eval_on_selector(f'xpath={button_xpath}', "el => el.click()")
                print("  Tendina aperta")
            except Exception as e:
                print(f"  BOTTONE NON TROVATO: {e}")
                page.screenshot(path="/tmp/debug_button.png", full_page=True)
                browser.close()
                return None

            # === ATTESA PORTAL (2 secondi) ===
            print("  Attesa portal menu (2s)...")
            time.sleep(2)

            # === GIORNATA 10 ===
            giornata_selector = f'ul.dropdown__list li:has-text("Round {target_giornata}")'
            print(f"  Selezione Round {target_giornata}...")
            try:
                locator = page.locator(giornata_selector)
                locator.wait_for(state='visible', timeout=20000)
                locator.click(force=True)
                print(f"  Cliccato Round {target_giornata}")
            except Exception as e:
                print(f"  GIORNATA NON TROVATA: {e}")
                page.screenshot(path="/tmp/debug_giornata_fail.png", full_page=True)
                browser.close()
                return None

            # === ATTESA PARTITE ===
            print("  Attesa partite...")
            page.wait_for_selector('a[href*="/match/"]', timeout=60000)  # ← 60s invece di 40s
            time.sleep(5)  # ← Attesa fissa invece di networkidle
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(3)

            html_content = page.content()
            print(f"  HTML scaricato: {len(html_content):,} byte")
            page.screenshot(path="/tmp/debug_final.png", full_page=True)

            browser.close()
            return html_content

    except Exception as e:
        print(f"ERRORE: {e}")
        traceback.print_exc()
        return None

def fetch_match_page(url):
    """Recupera HTML partita (SENZA quote)"""
    try:
        with sync_playwright() as p:
            print(f"  Avvio browser per partita: {url}")
            browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
            
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            page = context.new_page()
            page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            match_id = url.split('#id:')[1] if '#id:' in url else 'unknown'
            
            time.sleep(3)  # Attesa base
            
            html_content = page.content()
            print(f"  📄 HTML: {len(html_content):,} byte")
            
            browser.close()
            return html_content
            
    except Exception as e:
        print(f"❌ ERRORE PARTITA: {e}")
        return None

def extract_match_id_from_url(href):
    """Estrae l'ID della partita dall'URL"""
    try:
        match = re.search(r'#id:(\d+)', href)
        return int(match.group(1)) if match else None
    except Exception as e:
        print(f"⚠️ Errore estrazione ID: {e}")
        return None

def extract_match_hrefs(html_content, target_giornata=None):
    if not html_content:
        return []
    
    try:
        tree = html.fromstring(html_content)
        results = []

        # Verifica giornata corrente
        giornata_xpath = "//div[contains(@class, 'Box')]//button/span[contains(text(), 'Round')]"
        giornata_elements = tree.xpath(giornata_xpath)
        current_giornata = None
        
        if giornata_elements:
            match = re.search(r'\d+', giornata_elements[0].text_content().strip())
            current_giornata = int(match.group()) if match else None
        
        print(f"Giornata visualizzata: {current_giornata}")
        
        if target_giornata and current_giornata != target_giornata:
            print(f"ATTENZIONE: Aspettato {target_giornata}, ma è {current_giornata}")
            return []

        xpath = "//div[contains(@class, 'Box')]//a[contains(@href, '/it/football/match/')]"
        elements = tree.xpath(xpath)
        print(f"Trovate {len(elements)} partite")

        for element in elements:
            href = element.get('href', '')
            if href and not href.startswith('https://'):
                href = f"https://www.sofascore.com{href}"
            if href:
                results.append((current_giornata, href))
        
        return results
        
    except Exception as e:
        print(f"Errore extract_match_hrefs: {e}")
        traceback.print_exc()
        return []

def extract_team_names(tree):
    """Estrae i nomi delle squadre"""
    squadra_casa = None
    squadra_trasferta = None
    
    try:
        xpath_casa_ng = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div/div[1]/div/a/div/div/bdi"
        xpath_trasferta_ng = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div/div[3]/div/a/div/div/bdi"
        
        casa_elements = tree.xpath(xpath_casa_ng)
        if casa_elements:
            squadra_casa = casa_elements[0].text_content().strip()
            print(f"    🏠 Casa: {squadra_casa}")
        
        trasferta_elements = tree.xpath(xpath_trasferta_ng)
        if trasferta_elements:
            squadra_trasferta = trasferta_elements[0].text_content().strip()
            print(f"    ✈️  Trasferta: {squadra_trasferta}")
        
        if not squadra_casa or not squadra_trasferta:
            print("    ⚠️  XPath assoluti falliti, provo fallback...")
            xpath_teams_fallback = "//a[contains(@href, '/team/')]//bdi"
            team_elements = tree.xpath(xpath_teams_fallback)
            
            if len(team_elements) >= 2:
                squadra_casa = team_elements[0].text_content().strip()
                squadra_trasferta = team_elements[1].text_content().strip()
                print(f"    🏠 Casa (fallback): {squadra_casa}")
                print(f"    ✈️  Trasferta (fallback): {squadra_trasferta}")
        
    except Exception as e:
        print(f"    ❌ Errore extract_team_names: {e}")
        traceback.print_exc()
    
    return squadra_casa, squadra_trasferta

def extract_match_info(tree):
    """Estrae data, ora e stato della partita"""
    data = None
    ora = None
    stato = None
    
    try:
        data_xpath = "//span[contains(text(), 'Oggi') or contains(text(), 'Domani') or contains(text(), '/202') or contains(text(), '-202')]"
        data_elements = tree.xpath(data_xpath)
        
        for element in data_elements:
            text = element.text_content().strip()
            if text.lower() == 'oggi':
                data = datetime.now(pytz.timezone('Europe/Rome')).strftime('%Y-%m-%d')
                break
            elif text.lower() == 'domani':
                data = (datetime.now(pytz.timezone('Europe/Rome')) + timedelta(days=1)).strftime('%Y-%m-%d')
                break
            elif re.match(r'\d{2}[/-]\d{2}[/-]\d{4}', text):
                parsed_date = datetime.strptime(text, '%d/%m/%Y') if '/' in text else datetime.strptime(text, '%d-%m-%Y')
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
                ora = text + ":00"
                break
        
        if data and ora:
            match_datetime = datetime.strptime(f"{data} {ora[:5]}", '%Y-%m-%d %H:%M')
            cest = pytz.timezone('Europe/Rome')
            match_datetime = cest.localize(match_datetime)
            now = datetime.now(cest)
            stato = 'NG' if now < match_datetime else 'F'
        
        if data and ora:
            print(f"    📅 Data: {data} {ora}")
            print(f"    🔔 Stato: {stato}")
    
    except Exception as e:
        print(f"    ❌ Errore extract_match_info: {e}")
        traceback.print_exc()
    
    return data, ora, stato

def extract_goals(tree, stato):
    """Estrae i goal solo se la partita è finita - 4 metodi"""
    if stato != 'F':
        return None, None
    
    goalcasa = None
    goaltrasferta = None
    
    try:
        # METODO 1: XPath assoluto 
        xpath_goalcasa = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div[1]/div[2]/div/div/div[1]/span/span[1]"
        xpath_goaltrasferta = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div[1]/div[2]/div/div/div[1]/span/span[3]"

        # Estrazione diretta degli elementi
        element_casa = tree.xpath(xpath_goalcasa)
        element_trasferta = tree.xpath(xpath_goaltrasferta)

        # Verifica che entrambi gli elementi esistano
        if element_casa and element_trasferta:
            goalcasa_text = element_casa[0].text_content().strip()
            goaltrasferta_text = element_trasferta[0].text_content().strip()
            
            # Conversione in interi (con gestione errori opzionale)
            try:
                goalcasa = int(goalcasa_text)
                goaltrasferta = int(goaltrasferta_text)
                print(f"    ⚽ Goal (XPath esatti): {goalcasa}-{goaltrasferta}")
                return goalcasa, goaltrasferta
            except ValueError:
                print("    Errore: uno dei valori non è un numero valido.")
                return None, None
        else:
            print("    Elementi non trovati con gli XPath specificati.")
            return None, None

        
        # METODO 2: Cerca elementi che contengono solo numeri singoli
        single_numbers_xpath = "//span[string-length(text()) <= 2 and string-length(text()) >= 1 and not(contains(text(), '-'))]"
        number_elements = tree.xpath(single_numbers_xpath)
        
        scores = []
        for element in number_elements:
            text = element.text_content().strip()
            try:
                score = int(text)
                if 0 <= score <= 99:  # Solo numeri ragionevoli per un punteggio
                    scores.append(score)
            except ValueError:
                continue
        
        # Prendi i primi 2 numeri trovati
        if len(scores) >= 2:
            goalcasa = scores[0]
            goaltrasferta = scores[1]
            print(f"    ⚽ Goal (numeri singoli): {goalcasa}-{goaltrasferta}")
            return goalcasa, goaltrasferta
        
        # METODO 3: Cerca span con pattern X-Y
        score_full_xpath = "//span[contains(text(), '-')]"
        elements = tree.xpath(score_full_xpath)
        
        for element in elements:
            text = element.text_content().strip()
            # Match pattern tipo "3 - 1" o "3-1"
            if re.match(r'^\d{1,2}\s*-\s*\d{1,2}$', text):
                parts = text.split('-')
                goalcasa = int(parts[0].strip())
                goaltrasferta = int(parts[1].strip())
                print(f"    ⚽ Goal (pattern X-Y): {goalcasa}-{goaltrasferta}")
                return goalcasa, goaltrasferta
        
        # METODO 4: Regex su tutto il testo (ultimo tentativo)
        full_text = tree.text_content()
        # Cerca pattern X-Y circondato da spazi o inizio/fine stringa
        score_match = re.search(r'(?:^|\s)(\d{1,2})\s*-\s*(\d{1,2})(?:\s|$)', full_text)
        if score_match:
            goalcasa = int(score_match.group(1))
            goaltrasferta = int(score_match.group(2))
            print(f"    ⚽ Goal (regex globale): {goalcasa}-{goaltrasferta}")
            return goalcasa, goaltrasferta
        
        print(f"    ⚠️ Goal non trovati (partita finita ma punteggio non visibile)")
    
    except Exception as e:
        print(f"    ❌ Errore extract_goals: {e}")
        traceback.print_exc()
    
    return goalcasa, goaltrasferta

def main():
    print("=" * 60)
    print("🚀 Avvio scraping Serie A")
    print("=" * 60)
    
    supabase = init_supabase()
    if not supabase:
        print("Impossibile connettersi a Supabase")
        sys.exit(1)
    
    # SCEGLI TU LA GIORNATA
    TARGET_GIORNATA = 10
    tournament_url = 'https://www.sofascore.com/it/torneo/calcio/italy/serie-a/23#id:76457#tab:matches'

    html_content = fetch_tournament_page(tournament_url, target_giornata=TARGET_GIORNATA)
    if not html_content:
        sys.exit(1)

    data = extract_match_hrefs(html_content, target_giornata=TARGET_GIORNATA)
    if not data:
        sys.exit(1)

    success_count = 0
    error_count = 0

    for idx, (giornata, href) in enumerate(data, 1):
        print(f"[{idx}/{len(data)}] " + "=" * 50)
        
        # === ESTRAI ID PARTITA ===
        match_id = extract_match_id_from_url(href)
        if not match_id:
            print(f"  ⚠️  SKIP: ID non valido")
            error_count += 1
            continue
        
        print(f"  🆔 ID Partita: {match_id}")
        
        # === CARICA PAGINA PARTITA ===
        odds_url = href
        odds_html = fetch_match_page(odds_url)
        
        if not odds_html:
            print(f"  ❌ Errore caricamento pagina")
            error_count += 1
            continue
        
        tree = html.fromstring(odds_html)
        
        # === ESTRAI DATI ===
        squadra_casa, squadra_trasferta = extract_team_names(tree)
        if not squadra_casa or not squadra_trasferta:
            print(f"  ⚠️  SKIP: Impossibile estrarre squadre")
            error_count += 1
            continue
        
        data_match, ora, stato = extract_match_info(tree)
        goalcasa, goaltrasferta = extract_goals(tree, stato)
        
        match_data = {
            'id': match_id,
            'giornata': giornata,
            'casa': squadra_casa,
            'trasferta': squadra_trasferta,
            'data': data_match,
            'ora': ora,
            'stato': stato,
            'gcasa': goalcasa,
            'gtrasferta': goaltrasferta,
            'href': href
        }
        
        # === SALVA SU SUPABASE ===
        if insert_or_update_match(supabase, match_data):
            success_count += 1
        else:
            error_count += 1
        
        time.sleep(2)
    
    print("=" * 60)
    print("✅ Sincronizzazione completata")
    print("=" * 60)
    print(f"📊 Partite elaborate: {len(data)}")
    print(f"✅ Successi: {success_count}")
    print(f"❌ Errori: {error_count}")
    print("=" * 60)
    
    sys.exit(0 if error_count == 0 else 1)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERRORE FATALE: {e}")
        traceback.print_exc()
        sys.exit(1)













