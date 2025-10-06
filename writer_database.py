import asyncio
from supabase import create_client, Client
from typing import Optional, Dict, Any, List
from datetime import datetime


class DatabaseWriter:
    """Gestisce tutte le operazioni di scrittura sul database Supabase"""
    
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
            print("Database writer connesso con successo")
        except Exception as e:
            print(f"Errore connessione database writer: {e}")
            self.client = None
    
    async def create_team(self, username: str, team_name: str) -> Optional[int]:
        """
        Crea una nuova squadra
        
        Args:
            username: Proprietario della squadra
            team_name: Nome della squadra
            
        Returns:
            ID della squadra creata o None
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return None
            
            data = {
                'owner': username,
                'name': team_name,
                'points': 0,
                'budget': 500,  # Budget iniziale esempio
                'created_at': datetime.now().isoformat()
            }
            
            response = self.client.table('teams').insert(data).execute()
            
            if response.data:
                print(f"Squadra '{team_name}' creata con successo")
                return response.data[0].get('id')
            
            return None
            
        except Exception as e:
            print(f"Errore create_team: {e}")
            return None
    
    async def add_player_to_team(self, team_id: int, player_id: int) -> bool:
        """
        Aggiunge un giocatore a una squadra
        
        Args:
            team_id: ID della squadra
            player_id: ID del giocatore
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            data = {
                'team_id': team_id,
                'player_id': player_id,
                'added_at': datetime.now().isoformat()
            }
            
            response = self.client.table('team_players').insert(data).execute()
            
            if response.data:
                print(f"Giocatore {player_id} aggiunto alla squadra {team_id}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Errore add_player_to_team: {e}")
            return False
    
    async def remove_player_from_team(self, team_id: int, player_id: int) -> bool:
        """
        Rimuove un giocatore da una squadra
        
        Args:
            team_id: ID della squadra
            player_id: ID del giocatore
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            response = self.client.table('team_players')\
                .delete()\
                .eq('team_id', team_id)\
                .eq('player_id', player_id)\
                .execute()
            
            print(f"Giocatore {player_id} rimosso dalla squadra {team_id}")
            return True
            
        except Exception as e:
            print(f"Errore remove_player_from_team: {e}")
            return False
    
    async def create_bet(self, username: str, match_id: int, 
                        bet_type: str, amount: float, odds: float) -> Optional[int]:
        """
        Crea una nuova scommessa
        
        Args:
            username: Utente che fa la scommessa
            match_id: ID della partita
            bet_type: Tipo di scommessa ('1', 'X', '2', 'over', 'under', etc.)
            amount: Importo scommesso
            odds: Quota della scommessa
            
        Returns:
            ID della scommessa o None
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return None
            
            data = {
                'username': username,
                'match_id': match_id,
                'bet_type': bet_type,
                'amount': amount,
                'odds': odds,
                'status': 'pending',
                'created_at': datetime.now().isoformat()
            }
            
            response = self.client.table('bets').insert(data).execute()
            
            if response.data:
                bet_id = response.data[0].get('id')
                print(f"Scommessa {bet_id} creata con successo")
                return bet_id
            
            return None
            
        except Exception as e:
            print(f"Errore create_bet: {e}")
            return None
    
    async def update_team_points(self, team_id: int, points: int) -> bool:
        """
        Aggiorna i punti di una squadra
        
        Args:
            team_id: ID della squadra
            points: Nuovi punti (somma o valore assoluto in base alla logica)
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            response = self.client.table('teams')\
                .update({'points': points})\
                .eq('id', team_id)\
                .execute()
            
            if response.data:
                print(f"Punti squadra {team_id} aggiornati a {points}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Errore update_team_points: {e}")
            return False
    
    async def update_match_result(self, match_id: int, 
                                  home_score: int, away_score: int) -> bool:
        """
        Aggiorna il risultato di una partita
        
        Args:
            match_id: ID della partita
            home_score: Gol squadra casa
            away_score: Gol squadra trasferta
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            data = {
                'home_score': home_score,
                'away_score': away_score,
                'status': 'completed',
                'completed_at': datetime.now().isoformat()
            }
            
            response = self.client.table('matches')\
                .update(data)\
                .eq('id', match_id)\
                .execute()
            
            if response.data:
                print(f"Risultato partita {match_id} aggiornato: {home_score}-{away_score}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Errore update_match_result: {e}")
            return False
    
    async def update_player_stats(self, player_id: int, stats: Dict[str, Any]) -> bool:
        """
        Aggiorna le statistiche di un giocatore
        
        Args:
            player_id: ID del giocatore
            stats: Dizionario con le statistiche (goals, assists, yellow_cards, etc.)
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            stats['updated_at'] = datetime.now().isoformat()
            
            response = self.client.table('player_stats')\
                .upsert(stats)\
                .eq('player_id', player_id)\
                .execute()
            
            if response.data:
                print(f"Statistiche giocatore {player_id} aggiornate")
                return True
            
            return False
            
        except Exception as e:
            print(f"Errore update_player_stats: {e}")
            return False
    
    async def settle_bet(self, bet_id: int, won: bool) -> bool:
        """
        Chiude una scommessa con l'esito
        
        Args:
            bet_id: ID della scommessa
            won: True se vinta, False se persa
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            status = 'won' if won else 'lost'
            data = {
                'status': status,
                'settled_at': datetime.now().isoformat()
            }
            
            response = self.client.table('bets')\
                .update(data)\
                .eq('id', bet_id)\
                .execute()
            
            if response.data:
                print(f"Scommessa {bet_id} chiusa come {status}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Errore settle_bet: {e}")
            return False
    
    async def update_team_budget(self, team_id: int, new_budget: float) -> bool:
        """
        Aggiorna il budget di una squadra
        
        Args:
            team_id: ID della squadra
            new_budget: Nuovo budget
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            response = self.client.table('teams')\
                .update({'budget': new_budget})\
                .eq('id', team_id)\
                .execute()
            
            if response.data:
                print(f"Budget squadra {team_id} aggiornato a {new_budget}")
                return True
            
            return False
            
        except Exception as e:
            print(f"Errore update_team_budget: {e}")
            return False
    
    async def delete_team(self, team_id: int) -> bool:
        """
        Elimina una squadra
        
        Args:
            team_id: ID della squadra da eliminare
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            # Prima elimina i giocatori della squadra
            self.client.table('team_players')\
                .delete()\
                .eq('team_id', team_id)\
                .execute()
            
            # Poi elimina la squadra
            response = self.client.table('teams')\
                .delete()\
                .eq('id', team_id)\
                .execute()
            
            print(f"Squadra {team_id} eliminata")
            return True
            
        except Exception as e:
            print(f"Errore delete_team: {e}")
            return False
    
    async def batch_update_players(self, updates: List[Dict[str, Any]]) -> bool:
        """
        Aggiorna multipli giocatori in batch
        
        Args:
            updates: Lista di dizionari con gli aggiornamenti
            
        Returns:
            True se successo, False altrimenti
        """
        await asyncio.sleep(0)
        
        try:
            if not self.client:
                return False
            
            for update in updates:
                update['updated_at'] = datetime.now().isoformat()
            
            response = self.client.table('players')\
                .upsert(updates)\
                .execute()
            
            if response.data:
                print(f"{len(updates)} giocatori aggiornati in batch")
                return True
            
            return False
            
        except Exception as e:
            print(f"Errore batch_update_players: {e}")
            return False
