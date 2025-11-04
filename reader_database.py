from supabase import create_client, Client
from typing import Optional, List, Dict, Any
import os

class DatabaseReader:
    """Gestisce tutte le operazioni di LETTURA dal database Supabase"""
    
    def __init__(self):
        """Inizializza il client Supabase"""
        self.supabase_url = os.getenv('SUPABASE_URL', 'https://ipqxjudlxcqacgtmpkzx.supabase.co')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlwcXhqdWRseGNxYWNndG1wa3p4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTEyNjU3OSwiZXhwIjoyMDc0NzAyNTc5fQ.9nMpSeM-p5PvnF3rwMeR_zzXXocyfzYV24vau3AcDso')
        self.client = create_client(self.supabase_url, self.supabase_key)
    
    def get_user_team(self, username: str) -> List[Dict[str, Any]]:
        """Recupera la squadra di un utente"""
        try:
            # Recupera l'ID della squadra dell'utente
            team_response = self.client.table('Squadre').select('id').eq('owner', username).execute()
            if not team_response.data:
                return []
            team_id = team_response.data[0].get('id')
            # Recupera i giocatori della squadra
            players_response = self.client.table('Giocatori').select('players(name, role)').eq('team_id', team_id).execute()
            return [player['players'] for player in players_response.data] if players_response.data else []
        except Exception as e:
            print(f"Errore get_user_team: {e}")
            return []
    
    def get_all_players(self) -> List[Dict[str, Any]]:
        """Recupera tutti i giocatori disponibili"""
        try:
            response = self.client.table('Giocatori').select('*').execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_all_players: {e}")
            return []
    
    def get_matches(self, status: Optional[str] = None, giornata: Optional[int] = None) -> List[Dict[str, Any]]:
        """Recupera le partite dal database"""
        try:
            query = self.client.table('Partite').select('*')
            
            if status:
                query = query.eq('stato', status)
            
            if giornata:
                query = query.eq('giornata', giornata)
            
            response = query.execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_matches: {e}")
            return []
    
    def get_match_details(self, match_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli completi di una partita"""
        try:
            response = self.client.table('Partite').select('*').eq('id', match_id).single().execute()
            return response.data if response.data else None
        except Exception as e:
            print(f"Errore get_match_details: {e}")
            return None
    
    def get_user_bets(self, username: str) -> List[Dict[str, Any]]:
        """Recupera le scommesse di un utente"""
        try:
            response = self.client.table('Scommesse').select('*').eq('username', username).execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_user_bets: {e}")
            return []
    
    def get_ranking(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Recupera la classifica generale"""
        try:
            response = self.client.table('Squadre').select('owner, name, points').order('points', desc=True).limit(limit).execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_ranking: {e}")
            return []
            
    def get_all_teams(self) -> List[Dict[str, Any]]:
        """Recupera tutte le squadre"""
        try:
            response = self.client.table('Squadre').select('*').execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_all_teams: {e}")
            return []
            
    def get_player_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Recupera le statistiche di un singolo giocatore"""
        try:
            response = self.client.table('Statistiche').select('*').eq('player_id', player_id).single().execute()
            return response.data if response.data else None
        except Exception as e:
            print(f"Errore get_player_stats: {e}")
            return None
    
    def search_players(self, search_term: str) -> List[Dict[str, Any]]:
        """Cerca giocatori per nome"""
        try:
            response = self.client.table('Giocatori').select('*').ilike('name', f'%{search_term}%').execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore search_players: {e}")
            return []

    def check_user_exists(self, username: str) -> bool:
        """Verifica se un utente esiste già"""
        try:
            response = self.client.table('Utenti').select('nome').eq('nome', username).execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Errore check_user_exists: {e}")
            return False
    
    def verify_user_login(self, username: str, password: str) -> bool:
        """Verifica le credenziali di login"""
        try:
            response = self.client.table('Utenti').select('password').eq('nome', username).single().execute()
            if response.data:
                # Confronta la password
                return response.data.get('password') == password
            return False
        except Exception as e:
            print(f"Errore verify_user_login: {e}")
            return False
    
    def get_user_id(self, username: str) -> Optional[int]:
        """Recupera l'ID di un utente dal nome"""
        try:
            response = self.client.table('Utenti').select('id').eq('nome', username).single().execute()
            if response.data:
                return response.data.get('id')
            return None
        except Exception as e:
            print(f"Errore get_user_id: {e}")
            return None

    def get_ranking(self, giornata: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Recupera la classifica dalla tabella Schedine
        Aggrega i punti per utente
        
        Args:
            giornata: Numero della giornata (None = classifica totale)
        
        Returns:
            Lista di dict con {username, punti_totali}
        """
        try:
            query = self.client.table('Schedine').select('IDutente, punti, giornata')
            
            if giornata:
                query = query.eq('giornata', giornata)
            
            response = query.execute()
            
            if not response.data:
                return []
            
            # Aggrega i punti per utente
            punti_per_utente = {}
            for row in response.data:
                user_id = row.get('IDutente')
                punti = row.get('punti', 0) or 0  # Gestisci None
                
                if user_id not in punti_per_utente:
                    punti_per_utente[user_id] = 0
                punti_per_utente[user_id] += punti
            
            # Converti in lista e aggiungi username
            ranking = []
            for user_id, punti_totali in punti_per_utente.items():
                # Prendi il nome utente dalla tabella Utenti
                user = self.client.table('Utenti').select('nome').eq('id', user_id).single().execute()
                username = user.data.get('nome', f'User_{user_id}') if user.data else f'User_{user_id}'
                
                ranking.append({
                    'user_id': user_id,
                    'username': username,
                    'punti': punti_totali
                })
            
            # Ordina per punti decrescenti
            ranking.sort(key=lambda x: x['punti'], reverse=True)
            
            return ranking
        except Exception as e:
            print(f"Errore get_ranking: {e}")
            return []
    
    def get_available_giornate_partite(self) -> List[int]:
        """Recupera tutte le giornate disponibili dalla tabella Partite"""
        try:
            response = self.client.table('Partite').select('giornata').execute()
            
            if response.data:
                # Estrai valori unici, rimuovi None, e ordina
                giornate = sorted(set(
                    row['giornata'] 
                    for row in response.data 
                    if row.get('giornata') is not None
                ))
                return giornate
            return []
        except Exception as e:
            print(f"Errore get_available_giornate_partite: {e}")
            return []

    def get_user_schedine(self, user_id: int, giornata: Optional[int] = None) -> List[Dict[str, Any]]:
        try:
            query = self.client.table('Schedine').select('IDpartita, scelta, punti, giornata, puntata')
            query = query.eq('IDutente', user_id)
            
            if giornata:
                query = query.eq('giornata', giornata)
            
            response = query.execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_user_schedine: {e}")
            return []

    def get_available_players(self) -> List[Dict[str, Any]]:
        """
        Recupera tutti i giocatori disponibili per l'acquisto
        con nome breve, squadra, goal, assist e prezzo
        """
        try:
            response = self.client.table('Giocatori').select(
                'id, nomebreve, squadra, goal, assist, prezzo'
            ).execute()
            
            if response.data:
                # Imposta prezzo a 1 se non presente
                for player in response.data:
                    if player.get('prezzo') is None:
                        player['prezzo'] = 1.0
                return response.data
            return []
        except Exception as e:
            print(f"Errore get_available_players: {e}")
            return []
