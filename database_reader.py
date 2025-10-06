#database_reader.py
import asyncio
from supabase import create_client, Client
from typing import Optional, List, Dict, Any


class DatabaseReader:
    """Gestisce tutte le operazioni di lettura dal database Supabase"""
    
    def __init__(self):
        """Inizializza il client Supabase"""
        # TODO: Sostituire con le tue credenziali Supabase
        self.supabase_url = "https://your-project.supabase.co"
        self.supabase_key = "your-anon-key"
        
        self.client: Optional[Client] = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Crea la connessione al database"""
        try:
            self.client = create_client(self.supabase_url, self.supabase_key)
            print("Database reader connesso con successo")
        except Exception as e:
            print(f"Errore connessione database reader: {e}")
            self.client = None
    
    async def get_user_team(self, username: str) -> List[Dict[str, Any]]:
        """
        Recupera la squadra di un utente
        
        Args:
            username: Nome utente
            
        Returns:
            Lista di giocatori della squadra
        """
        await asyncio.sleep(0)  # Yield per non bloccare
        
        try:
            if not self.client:
                print("Client non inizializzato")
                return []
            
            # Query: SELECT * FROM teams WHERE owner = username
            response = self.client.table('teams').select('*').eq('owner', username).execute()
            
            if response.data:
                team_id = response.data[0].get('id')
                
                # Recupera i giocatori della squadra
                players_response = self.client.table('team_players')\
                    .select('*, players(*)')\
                    .eq('team_id', team_id)\
                    .execute()
                
                return players_response.data if players_response.data else []
            
            return []
            
        except Exception as e:
            print(f"Errore get_user_team: {e}")
            return []
    
    async def get_all_players(self) -> List[Dict[str, Any]]:
        """
        Recupera tutti i giocatori disponibili
        
        Returns:
            Lista di tutti i giocatori
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return []
            
            response = self.client.table('players').select('*').execute()
            return response.data if response.data else []
            
        except Exception as e:
            print(f"Errore get_all_players: {e}")
            return []
    
    async def get_matches(self, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Recupera le partite, opzionalmente filtrate per stato
        
        Args:
            status: Stato delle partite ('scheduled', 'in_progress', 'completed')
            
        Returns:
            Lista di partite
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return []
            
            query = self.client.table('matches').select('*')
            
            if status:
                query = query.eq('status', status)
            
            response = query.order('date', desc=False).execute()
            return response.data if response.data else []
            
        except Exception as e:
            print(f"Errore get_matches: {e}")
            return []
    
    async def get_user_bets(self, username: str) -> List[Dict[str, Any]]:
        """
        Recupera le scommesse di un utente
        
        Args:
            username: Nome utente
            
        Returns:
            Lista di scommesse
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return []
            
            response = self.client.table('bets')\
                .select('*, matches(*)')\
                .eq('username', username)\
                .execute()
            
            return response.data if response.data else []
            
        except Exception as e:
            print(f"Errore get_user_bets: {e}")
            return []
    
    async def get_ranking(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Recupera la classifica generale
        
        Args:
            limit: Numero massimo di risultati
            
        Returns:
            Lista ordinata di squadre per punteggio
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return []
            
            response = self.client.table('teams')\
                .select('*')\
                .order('points', desc=True)\
                .limit(limit)\
                .execute()
            
            return response.data if response.data else []
            
        except Exception as e:
            print(f"Errore get_ranking: {e}")
            return []
    
    async def get_player_stats(self, player_id: int) -> Optional[Dict[str, Any]]:
        """
        Recupera le statistiche di un singolo giocatore
        
        Args:
            player_id: ID del giocatore
            
        Returns:
            Statistiche del giocatore o None
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return None
            
            response = self.client.table('player_stats')\
                .select('*')\
                .eq('player_id', player_id)\
                .execute()
            
            return response.data[0] if response.data else None
            
        except Exception as e:
            print(f"Errore get_player_stats: {e}")
            return None
    
    async def get_match_details(self, match_id: int) -> Optional[Dict[str, Any]]:
        """
        Recupera i dettagli completi di una partita
        
        Args:
            match_id: ID della partita
            
        Returns:
            Dettagli della partita o None
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return None
            
            response = self.client.table('matches')\
                .select('*, home_team(*), away_team(*)')\
                .eq('id', match_id)\
                .execute()
            
            return response.data[0] if response.data else None
            
        except Exception as e:
            print(f"Errore get_match_details: {e}")
            return None
    
    async def search_players(self, search_term: str) -> List[Dict[str, Any]]:
        """
        Cerca giocatori per nome
        
        Args:
            search_term: Termine di ricerca
            
        Returns:
            Lista di giocatori che corrispondono alla ricerca
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return []
            
            response = self.client.table('players')\
                .select('*')\
                .ilike('name', f'%{search_term}%')\
                .execute()
            
            return response.data if response.data else []
            
        except Exception as e:
            print(f"Errore search_players: {e}")
            return []