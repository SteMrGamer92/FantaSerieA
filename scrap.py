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

def fetch_page(url):
    """Recupera il contenuto HTML usando Playwright con scroll multipli"""
    try:
        with sync_playwright() as p:
            print(f"  🌐 Avvio browser...")
            
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--disable-gpu',
                    '--disable-dev-shm-usage',
                    '--disable-setuid-sandbox',
                    '--no-sandbox',
                    '--disable-web-security'
                ]
            )
            
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            )
            
            page = context.new_page()
            
            print(f"  📡 Caricamento pagina...")
            page.goto(url, wait_until='domcontentloaded', timeout=60000)
            
            print("  ⏳ Attesa elementi...")
            
            try:
                page.wait_for_selector('div[class*="Box"]', timeout=45000)
                print("  ✅ Elementi trovati")
                
                print("  📜 Scroll pagina per caricare elementi...")
                
                for i in range(5):
                    page.evaluate(f"window.scrollTo(0, {(i + 1) * 500})")
                    time.sleep(0.5)
                
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                
                page.evaluate("window.scrollTo(0, 0)")
                time.sleep(1)
                
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                
                time.sleep(5)
                
                page.wait_for_load_state('networkidle', timeout=30000)
                print("  ✅ Rete stabile")
                
            except Exception as e:
                print(f"  ⚠️ Timeout attesa elementi")
                time.sleep(5)
            
            html_content = page.content()
            
            print(f"  ✅ HTML scaricato ({len(html_content)} bytes)")
            
            page.close()
            context.close()
            browser.close()
            
            return html_content
            
    except Exception as e:
        print(f"❌ Errore recupero pagina: {e}")
        traceback.print_exc()
        return None

def extract_match_id_from_url(href):
    """Estrae l'ID della partita dall'URL"""
    try:
        match = re.search(r'#id:(\d+)', href)
        return int(match.group(1)) if match else None
    except Exception as e:
        print(f"⚠️ Errore estrazione ID: {e}")
        return None

def extract_match_hrefs(html_content):
    """Estrae gli href delle partite dalla pagina del torneo"""
    if not html_content:
        print("❌ Nessun contenuto HTML ricevuto")
        return []
    
    try:
        tree = html.fromstring(html_content)
        results = []
        
        giornata_xpath = "//div[contains(@class, 'Box')]//button/span[contains(text(), 'Round')]"
        giornata_elements = tree.xpath(giornata_xpath)
        giornata_number = None
        
        if giornata_elements:
            match = re.search(r'\d+', giornata_elements[0].text_content().strip())
            giornata_number = int(match.group()) if match else None
        
        if not giornata_number:
            print("⚠️ Giornata non trovata")
            return []
        
        print(f"📅 Giornata estratta: {giornata_number}")
        
        xpath = "//div[contains(@class, 'Box')]//a[contains(@href, '/it/football/match/')]"
        elements = tree.xpath(xpath)
        print(f"🔍 Trovate {len(elements)} partite")
        
        for element in elements:
            href = element.get('href', '')
            if href and not href.startswith('https://'):
                href = f"https://www.sofascore.com{href}"
            if href:
                results.append((giornata_number, href))
        
        return results
        
    except Exception as e:
        print(f"❌ Errore extract_match_hrefs: {e}")
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

def extract_odds(tree):
    """Estrae le quote 1, X, 2"""
    quote1 = None
    quotex = None
    quote2 = None
    
    try:
        xpath_quote1 = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[1]/div/div[2]/div/a[1]/div/span"
        xpath_quotex = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[1]/div/div[2]/div/a[2]/div/span"
        xpath_quote2 = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[1]/div/div[2]/div/a[3]/div/span"
        
        q1_elements = tree.xpath(xpath_quote1)
        if q1_elements:
            text = q1_elements[0].text_content().strip()
            if re.match(r'^\d+\.\d+$', text):
                quote1 = float(text)
                print(f"    1️⃣  Quota 1: {quote1}")
        
        qx_elements = tree.xpath(xpath_quotex)
        if qx_elements:
            text = qx_elements[0].text_content().strip()
            if re.match(r'^\d+\.\d+$', text):
                quotex = float(text)
                print(f"    ❌ Quota X: {quotex}")
        
        q2_elements = tree.xpath(xpath_quote2)
        if q2_elements:
            text = q2_elements[0].text_content().strip()
            if re.match(r'^\d+\.\d+$', text):
                quote2 = float(text)
                print(f"    2️⃣  Quota 2: {quote2}")
        
        if not all([quote1, quotex, quote2]):
            print("    ⚠️  XPath assoluti falliti, provo fallback...")
            xpath_fallback = "//div[contains(text(), 'Esito finale') or contains(text(), '1X2')]/following-sibling::div//span[contains(text(), '.')]"
            elements = tree.xpath(xpath_fallback)
            
            quotes = []
            for element in elements[:3]:
                text = element.text_content().strip()
                if re.match(r'^\d+\.\d+$', text):
                    quotes.append(float(text))
            
            if len(quotes) >= 3:
                quote1, quotex, quote2 = quotes[0], quotes[1], quotes[2]
                print(f"    1️⃣  Quota 1 (fallback): {quote1}")
                print(f"    ❌ Quota X (fallback): {quotex}")
                print(f"    2️⃣  Quota 2 (fallback): {quote2}")
    
    except Exception as e:
        print(f"    ❌ Errore extract_odds: {e}")
        traceback.print_exc()
    
    return quote1, quotex, quote2

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
        # METODO 1: XPath assoluto per il punteggio principale
        score_xpath_abs = "/html/body/div[1]/main/div[2]/div/div/div[1]/div[3]/div/div[2]/div/div/div[2]/div/span"
        score_elements = tree.xpath(score_xpath_abs)
        
        if score_elements:
            text = score_elements[0].text_content().strip()
            if re.match(r'^\d+\s*-\s*\d+$', text):
                parts = text.split('-')
                goalcasa = int(parts[0].strip())
                goaltrasferta = int(parts[1].strip())
                print(f"    ⚽ Goal (XPath assoluto): {goalcasa}-{goaltrasferta}")
                return goalcasa, goaltrasferta
        
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
        print("❌ Impossibile connettersi a Supabase")
        sys.exit(1)
    
    tournament_url = 'https://www.sofascore.com/it/torneo/calcio/italy/serie-a/23#id:76457#tab:matches'
    
    print("\n📡 Recupero pagina torneo...")
    html_content = fetch_page(tournament_url)
    if not html_content:
        print("❌ Impossibile recuperare contenuto pagina")
        sys.exit(1)
    
    data = extract_match_hrefs(html_content)
    
    if not data:
        print("❌ Nessuna partita trovata")
        sys.exit(1)
    
    print(f"\n🔄 Elaborazione {len(data)} partite...\n")
    
    success_count = 0
    error_count = 0
    
    for idx, (giornata, href) in enumerate(data, 1):
        print(f"[{idx}/{len(data)}] " + "=" * 50)
        
        match_id = extract_match_id_from_url(href)
        if not match_id:
            print(f"  ⚠️  SKIP: ID non valido")
            error_count += 1
            continue
        
        print(f"  🆔 ID Partita: {match_id}")
        
        odds_url = f"{href},tab:additional_odds"
        odds_html = fetch_page(odds_url)
        
        if not odds_html:
            print(f"  ❌ Errore caricamento pagina")
            error_count += 1
            continue
        
        tree = html.fromstring(odds_html)
        
        squadra_casa, squadra_trasferta = extract_team_names(tree)
        
        if not squadra_casa or not squadra_trasferta:
            print(f"  ⚠️  SKIP: Impossibile estrarre squadre")
            error_count += 1
            continue
        
        data_match, ora, stato = extract_match_info(tree)
        goalcasa, goaltrasferta = extract_goals(tree, stato)
        
        quote1, quotex, quote2 = None, None, None
        if stato == 'NG':
            quote1, quotex, quote2 = extract_odds(tree)
            if quote1 and quotex and quote2:
                print(f"    ℹ️  Partita futura: quote estratte")
            else:
                print(f"    ⚠️  Quote non disponibili, SKIP")
                error_count += 1
                continue
        else:
            print(f"    ℹ️  Partita {stato}: solo dati partita (no quote)")
        
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
        
        if quote1 and quotex and quote2:
            match_data['quota1'] = quote1
            match_data['quotax'] = quotex
            match_data['quota2'] = quote2
        
        if insert_or_update_match(supabase, match_data):
            success_count += 1
        else:
            error_count += 1
        
        print()
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
