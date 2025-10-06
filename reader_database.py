# reader_database.py
import requests
from typing import Optional, List, Dict, Any

class DatabaseReader:
    """Gestisce tutte le operazioni di LETTURA tramite API backend"""
    
    def __init__(self, base_url: str, api_key: str):
        """
        Args:
            base_url: URL del backend (es. http://18.156.158.53)
            api_key: API key per autenticarsi
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json'
        }
    
    def _get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """Helper per richieste GET"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Errore GET {endpoint}: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_user_team(self, username: str) -> List[Dict[str, Any]]:
        """Recupera la squadra di un utente"""
        result = self._get(f'/api/squadre/{username}')
        if result.get('success') and result.get('data'):
            team_id = result['data'].get('id')
            if team_id:
                players_result = self._get(f'/api/team/{team_id}/giocatori')
                return players_result.get('data', []) if players_result.get('success') else []
        return []
    
    def get_all_players(self) -> List[Dict[str, Any]]:
        """Recupera tutti i giocatori disponibili"""
        result = self._get('/api/giocatori')
        return result.get('data', []) if result.get('success') else []
    
    def get_matches(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Recupera le partite dal server
        Args:
            status: Filtro opzionale ('scheduled', 'in_progress', 'completed')
        """
        params = {'status': status} if status else None
        result = self._get('/api/partite', params=params)
        return result.get('data', []) if result.get('success') else []
    
    def get_match_details(self, match_id: int) -> Optional[Dict[str, Any]]:
        """Recupera i dettagli completi di una partita"""
        result = self._get(f'/api/partite/{match_id}')
        return result.get('data') if result.get('success') else None
    
    def get_user_bets(self, username: str) -> List[Dict[str, Any]]:
        """Recupera le scommesse di un utente"""
        result = self._get(f'/api/scommesse/{username}')
        return result.get('data', []) if result.get('success') else []
    
    def get_ranking(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Recupera la classifica generale"""
        result = self._get('/api/classifica', params={'limit': limit})
        return result.get('data', []) if result.get('success') else []
    
    def get_player_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Recupera le statistiche di un singolo giocatore"""
        result = self._get(f'/api/giocatori/{player_id}/stats')
        return result.get('data') if result.get('success') else None
    
    def search_players(self, search_term: str) -> List[Dict[str, Any]]:
        """Cerca giocatori per nome"""
        result = self._get('/api/giocatori/search', params={'q': search_term})
        return result.get('data', []) if result.get('success') else []
