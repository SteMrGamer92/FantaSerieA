# writer_database.py
from supabase import create_client, Client
from typing import Optional, Dict, Any, List
from datetime import datetime

class DatabaseWriter:
    """Gestisce tutte le operazioni di scrittura sul database Supabase"""
    
    def __init__(self):
        """Inizializza il client Supabase"""
        self.supabase_url = "https://ipqxjudlxcqacgtmpkzx.supabase.co"
        self.supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlwcXhqdWRseGNxYWNndG1wa3p4Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTEyNjU3OSwiZXhwIjoyMDc0NzAyNTc5fQ.9nMpSeM-p5PvnF3rwMeR_zzXXocyfzYV24vau3AcDso"
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
    
    def create_team(self, username: str, team_name: str) -> Optional[int]:
        """
        Crea una nuova squadra
        """
        try:
            if not self.client:
                return None
            data = {
                'owner': username,
                'name': team_name,
                'points': 0,
                'budget': 500,
                'created_at': datetime.now().isoformat()
            }
            response = self.client.table('Squadre').insert(data).execute()
            if response.data:
                print(f"Squadra '{team_name}' creata con successo")
                return response.data[0].get('id')
            return None
        except Exception as e:
            print(f"Errore create_team: {e}")
            return None
            
    def update_team(self, team_id: int, updates: Dict[str, Any]) -> bool:
        """
        Aggiorna i dati di una squadra
        """
        try:
            if not self.client:
                return False
            updates['updated_at'] = datetime.now().isoformat()
            response = self.client.table('Squadre').update(updates).eq('id', team_id).execute()
            if response.data:
                print(f"Squadra {team_id} aggiornata con successo")
                return True
            return False
        except Exception as e:
            print(f"Errore update_team: {e}")
            return False
        
    def add_player_to_team(self, team_id: int, player_id: int) -> bool:
        """
        Aggiunge un giocatore a una squadra
        """
        try:
            if not self.client:
                return False
            data = {
                'team_id': team_id,
                'player_id': player_id,
                'added_at': datetime.now().isoformat()
            }
            response = self.client.table('Giocatori').insert(data).execute()
            if response.data:
                print(f"Giocatore {player_id} aggiunto alla squadra {team_id}")
                return True
            return False
        except Exception as e:
            print(f"Errore add_player_to_team: {e}")
            return False
    
    def remove_player_from_team(self, team_id: int, player_id: int) -> bool:
        """
        Rimuove un giocatore da una squadra
        """
        try:
            if not self.client:
                return False
            response = self.client.table('Giocatori').delete().eq('team_id', team_id).eq('player_id', player_id).execute()
            print(f"Giocatore {player_id} rimosso dalla squadra {team_id}")
            return True
        except Exception as e:
            print(f"Errore remove_player_from_team: {e}")
            return False
    
    def create_bet(self, username: str, match_id: int, bet_type: str, amount: float, odds: float) -> Optional[int]:
        """
        Crea una nuova scommessa
        """
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
            response = self.client.table('Scommesse').insert(data).execute()
            if response.data:
                bet_id = response.data[0].get('id')
                print(f"Scommessa {bet_id} creata con successo")
                return bet_id
            return None
        except Exception as e:
            print(f"Errore create_bet: {e}")
            return None
            
    def create_schedina(self, user_id: int, scommesse: List[Dict[str, Any]], importo: float = 10.0) -> Optional[int]:
        """
        Crea una nuova schedina con multiple scommesse
        
        Args:
            user_id: ID dell'utente
            scommesse: Lista di dict con {match_id, bet_type, quota}
            importo: Importo giocato (default 10€)
        
        Returns:
            ID della schedina creata o None
        """
        try:
            if not self.client:
                return None
            
            # Calcola quota totale
            quota_totale = 1.0
            for scommessa in scommesse:
                quota_totale *= scommessa['quota']
            
            # Calcola vincita potenziale
            vincita_potenziale = importo * quota_totale
            
            # Crea la schedina principale
            schedina_data = {
                'user_id': user_id,
                'importo': importo,
                'quota_totale': quota_totale,
                'vincita_potenziale': vincita_potenziale,
                'stato': 'pending',
                'created_at': datetime.now().isoformat()
            }
            
            response = self.client.table('Schedine').insert(schedina_data).execute()
            
            if response.data:
                schedina_id = response.data[0].get('id')
                print(f"Schedina {schedina_id} creata con successo")
                
                # Crea le singole scommesse collegate alla schedina
                for scommessa in scommesse:
                    dettaglio_data = {
                        'schedina_id': schedina_id,
                        'match_id': scommessa['match_id'],
                        'bet_type': scommessa['bet_type'],
                        'quota': scommessa['quota'],
                        'created_at': datetime.now().isoformat()
                    }
                    self.client.table('Schedine_Dettagli').insert(dettaglio_data).execute()
                
                print(f"Aggiunte {len(scommesse)} scommesse alla schedina {schedina_id}")
                return schedina_id
            
            return None
        except Exception as e:
            print(f"Errore create_schedina: {e}")
            return None
            
    def update_team_points(self, team_id: int, points: int) -> bool:
        """
        Aggiorna i punti di una squadra
        """
        try:
            if not self.client:
                return False
            response = self.client.table('Squadre').update({'points': points}).eq('id', team_id).execute()
            if response.data:
                print(f"Punti squadra {team_id} aggiornati a {points}")
                return True
            return False
        except Exception as e:
            print(f"Errore update_team_points: {e}")
            return False
    
    def update_match_result(self, match_id: int, home_score: int, away_score: int) -> bool:
        """
        Aggiorna il risultato di una partita
        """
        try:
            if not self.client:
                return False
            data = {
                'home_score': home_score,
                'away_score': away_score,
                'status': 'completed',
                'completed_at': datetime.now().isoformat()
            }
            response = self.client.table('Partite').update(data).eq('id', match_id).execute()
            if response.data:
                print(f"Risultato partita {match_id} aggiornato: {home_score}-{away_score}")
                return True
            return False
        except Exception as e:
            print(f"Errore update_match_result: {e}")
            return False
    
    def update_player_stats(self, player_id: int, stats: Dict[str, Any]) -> bool:
        """
        Aggiorna le statistiche di un giocatore
        """
        try:
            if not self.client:
                return False
            stats['updated_at'] = datetime.now().isoformat()
            response = self.client.table('Statistiche').upsert(stats).eq('player_id', player_id).execute()
            if response.data:
                print(f"Statistiche giocatore {player_id} aggiornate")
                return True
            return False
        except Exception as e:
            print(f"Errore update_player_stats: {e}")
            return False
    
    def settle_bet(self, bet_id: int, won: bool) -> bool:
        """
        Chiude una scommessa con l'esito
        """
        try:
            if not self.client:
                return False
            status = 'won' if won else 'lost'
            data = {
                'status': status,
                'settled_at': datetime.now().isoformat()
            }
            response = self.client.table('Scommesse').update(data).eq('id', bet_id).execute()
            if response.data:
                print(f"Scommessa {bet_id} chiusa come {status}")
                return True
            return False
        except Exception as e:
            print(f"Errore settle_bet: {e}")
            return False
    
    def update_team_budget(self, team_id: int, new_budget: float) -> bool:
        """
        Aggiorna il budget di una squadra
        """
        try:
            if not self.client:
                return False
            response = self.client.table('Squadre').update({'budget': new_budget}).eq('id', team_id).execute()
            if response.data:
                print(f"Budget squadra {team_id} aggiornato a {new_budget}")
                return True
            return False
        except Exception as e:
            print(f"Errore update_team_budget: {e}")
            return False
    
    def delete_team(self, team_id: int) -> bool:
        """
        Elimina una squadra
        """
        try:
            if not self.client:
                return False
            self.client.table('Giocatori').delete().eq('team_id', team_id).execute()
            response = self.client.table('teams').delete().eq('id', team_id).execute()
            print(f"Squadra {team_id} eliminata")
            return True
        except Exception as e:
            print(f"Errore delete_team: {e}")
            return False
    
    def batch_update_players(self, updates: List[Dict[str, Any]]) -> bool:
        """
        Aggiorna multipli giocatori in batch
        """
        try:
            if not self.client:
                return False
            for update in updates:
                update['updated_at'] = datetime.now().isoformat()
            response = self.client.table('Giocatori').upsert(updates).execute()
            if response.data:
                print(f"{len(updates)} giocatori aggiornati in batch")
                return True
            return False
        except Exception as e:
            print(f"Errore batch_update_players: {e}")
            return False
