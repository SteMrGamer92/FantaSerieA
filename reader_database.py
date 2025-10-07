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
            team_response = self.client.table('teams').select('id').eq('owner', username).execute()
            if not team_response.data:
                return []
            team_id = team_response.data[0].get('id')
            # Recupera i giocatori della squadra
            players_response = self.client.table('team_players').select('players(name, role)').eq('team_id', team_id).execute()
            return [player['players'] for player in players_response.data] if players_response.data else []
        except Exception as e:
            print(f"Errore get_user_team: {e}")
            return []
    
    def get_all_players(self) -> List[Dict[str, Any]]:
        """Recupera tutti i giocatori disponibili"""
        try:
            response = self.client.table('players').select('*').execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_all_players: {e}")
            return []
    
    def get_matches(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Recupera le partite dal database"""
        try:
            query = self.client.table('matches').select('*')
            if status:
                query = query.eq('status', status)
            response = query.execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_matches: {e}")
            return []
    
    def get_match_details(self, match_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli completi di una partita"""
        try:
            response = self.client.table('matches').select('*').eq('id', match_id).single().execute()
            return response.data if response.data else None
        except Exception as e:
            print(f"Errore get_match_details: {e}")
            return None
    
    def get_user_bets(self, username: str) -> List[Dict[str, Any]]:
        """Recupera le scommesse di un utente"""
        try:
            response = self.client.table('bets').select('*').eq('username', username).execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_user_bets: {e}")
            return []
    
    def get_ranking(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Recupera la classifica generale"""
        try:
            response = self.client.table('teams').select('owner, name, points').order('points', desc=True).limit(limit).execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_ranking: {e}")
            return []
    def get_all_teams(self) -> List[Dict[str, Any]]:
        """Recupera tutte le squadre"""
        try:
            response = self.client.table('teams').select('*').execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore get_all_teams: {e}")
            return []
            
    def get_player_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Recupera le statistiche di un singolo giocatore"""
        try:
            response = self.client.table('player_stats').select('*').eq('player_id', player_id).single().execute()
            return response.data if response.data else None
        except Exception as e:
            print(f"Errore get_player_stats: {e}")
            return None
    
    def search_players(self, search_term: str) -> List[Dict[str, Any]]:
        """Cerca giocatori per nome"""
        try:
            response = self.client.table('players').select('*').ilike('name', f'%{search_term}%').execute()
            return response.data if response.data else []
        except Exception as e:
            print(f"Errore search_players: {e}")
            return []
