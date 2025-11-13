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
            
    def create_schedina(self, user_id: int, scommesse: List[Dict[str, Any]]) -> bool:
        try:
            if not self.client:
                return False
            
            for scommessa in scommesse:
                match_id = scommessa['match_id']
                bet_type = scommessa['bet_type']
                quota = scommessa['quota']
                puntata = scommessa.get('puntata', 10.0)  # ✅ Usa puntata dal frontend o default 10
                
                # Controlla se esiste già una scommessa per questo utente e partita
                existing = self.client.table('Schedine').select('id').eq('IDutente', user_id).eq('IDpartita', match_id).execute()
                
                if existing.data and len(existing.data) > 0:
                    # Scommessa esistente → AGGIORNA
                    schedina_id = existing.data[0]['id']
                    
                    update_data = {
                        'scelta': bet_type,
                        'puntata': puntata, 
                    }
                    
                    response = self.client.table('Schedine').update(update_data).eq('id', schedina_id).execute()
                    
                    if response.data:
                        print(f"✅ Scommessa aggiornata: Partita {match_id}, Scelta {bet_type}, Puntata €{puntata}")
                    else:
                        print(f"⚠️ Errore aggiornamento scommessa partita {match_id}")
                        return False
                else:
                    # Scommessa nuova → INSERISCI
                    insert_data = {
                        'IDpartita': match_id,
                        'IDutente': user_id,
                        'puntata': puntata,
                        'scelta': bet_type,
                    }
                    
                    response = self.client.table('Schedine').insert(insert_data).execute()
                    
                    if response.data:
                        print(f"✅ Nuova scommessa inserita: Partita {match_id}, Scelta {bet_type}, Puntata €{puntata}")
                    else:
                        print(f"⚠️ Errore inserimento scommessa partita {match_id}")
                        return False
            
            print(f"✅ Schedina completata: {len(scommesse)} scommesse processate")
            return True
            
        except Exception as e:
            print(f"❌ Errore create_schedina: {e}")
            return False

    def create_user(self, username: str, password: str) -> Optional[int]:
        """
        Crea un nuovo utente
        
        Args:
            username: Nome utente
            password: Password (in chiaro per ora, poi vedremo hash)
        
        Returns:
            ID dell'utente creato o None
        """
        try:
            if not self.client:
                return None
            
            data = {
                'nome': username,
                'password': password,
            }
            
            response = self.client.table('Utenti').insert(data).execute()
            
            if response.data:
                user_id = response.data[0].get('id')
                print(f"✅ Utente '{username}' creato con ID {user_id}")
                return user_id
            
            return None
        except Exception as e:
            print(f"❌ Errore create_user: {e}")
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
    
    def update_team_budget(self, team_id: int, new_budget: int) -> bool:
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

    def delete_schedina(self, user_id: int, match_id: int) -> bool:
        """
        Elimina una scommessa specifica dalla tabella Schedine
        
        Args:
            user_id: ID dell'utente
            match_id: ID della partita
        
        Returns:
            True se eliminazione riuscita, False altrimenti
        """
        try:
            if not self.client:
                return False
            
            # Elimina la riga dove IDutente = user_id AND IDpartita = match_id
            response = self.client.table('Schedine').delete().eq('IDutente', user_id).eq('IDpartita', match_id).execute()
            
            if response.data:
                print(f"Scommessa eliminata: User {user_id}, Partita {match_id}")
                return True
            else:
                print(f"Scommessa non trovata: User {user_id}, Partita {match_id}")
                return False
                
        except Exception as e:
            print(f"Errore delete_schedina: {e}")
            return False

    def buy_player(self, user_id: int, player_id: int, prezzo: int) -> bool:
        """
        Registra l'acquisto di un giocatore nella tabella Rose
        E sottrae il prezzo dai crediti dell'utente
        
        Args:
            user_id: ID dell'utente
            player_id: ID del giocatore
            prezzo: Prezzo di acquisto
        
        Returns:
            True se acquisto riuscito, False altrimenti
        """
        try:
            if not self.client:
                return False
            
            # 1. Verifica crediti sufficienti
            user_response = self.client.table('Utenti').select('crediti').eq('id', user_id).single().execute()
            
            if not user_response.data:
                print(f"⚠️ Utente {user_id} non trovato")
                return False
            
            crediti_attuali = user_response.data.get('crediti', 0) or 0
            
            if crediti_attuali < prezzo:
                print(f"⚠️ Crediti insufficienti: {crediti_attuali} < {prezzo}")
                return False
            
            # 2. Verifica che il giocatore non sia già nella rosa
            existing = self.client.table('Rose').select('IDgiocatore').eq('IDutente', user_id).eq('IDgiocatore', player_id).execute()
            
            if existing.data and len(existing.data) > 0:
                print(f"⚠️ Giocatore {player_id} già nella rosa dell'utente {user_id}")
                return False
            
            # 3. Inserisci nella tabella Rose
            insert_data = {
                'IDutente': user_id,
                'IDgiocatore': player_id,
                'prezzo': prezzo
            }
            
            rosa_response = self.client.table('Rose').insert(insert_data).execute()
            
            if not rosa_response.data:
                print(f"⚠️ Errore inserimento giocatore {player_id} nella rosa")
                return False
            
            # 4. Sottrai crediti (CONVERTI IN INT)
            nuovi_crediti = int(crediti_attuali - prezzo)  # ← FIX: Converti in int
            
            credits_response = self.client.table('Utenti').update({
                'crediti': nuovi_crediti  # Ora è int, non float
            }).eq('id', user_id).execute()
            
            if not credits_response.data:
                print(f"⚠️ Errore aggiornamento crediti")
                # Rollback: rimuovi il giocatore dalla rosa
                self.client.table('Rose').delete().eq('IDutente', user_id).eq('IDgiocatore', player_id).execute()
                return False
            
            print(f"✅ Giocatore {player_id} acquistato dall'utente {user_id} per €{prezzo}")
            print(f"💰 Crediti aggiornati: {crediti_attuali} → {nuovi_crediti}")
            return True
                
        except Exception as e:
            print(f"❌ Errore buy_player: {e}")
            return False
    
    def sell_player(self, user_id: int, player_id: int, prezzo: int) -> bool:
        """
        Vende un giocatore dalla tabella Rose
        E aggiunge il prezzo ai crediti dell'utente
        
        Args:
            user_id: ID dell'utente
            player_id: ID del giocatore
            prezzo: Prezzo di vendita (prezzo di acquisto)
        
        Returns:
            True se vendita riuscita, False altrimenti
        """
        try:
            if not self.client:
                return False
            
            # 1. Verifica che il giocatore sia nella rosa
            existing = self.client.table('Rose').select('IDgiocatore').eq('IDutente', user_id).eq('IDgiocatore', player_id).execute()
            
            if not existing.data or len(existing.data) == 0:
                print(f"⚠️ Giocatore {player_id} non trovato nella rosa dell'utente {user_id}")
                return False
            
            # 2. Elimina dalla tabella Rose
            delete_response = self.client.table('Rose').delete().eq('IDutente', user_id).eq('IDgiocatore', player_id).execute()
            
            if not delete_response.data:
                print(f"⚠️ Errore eliminazione giocatore {player_id} dalla rosa")
                return False
            
            # 3. Recupera crediti attuali
            user_response = self.client.table('Utenti').select('crediti').eq('id', user_id).single().execute()
            
            if not user_response.data:
                print(f"⚠️ Utente {user_id} non trovato")
                # Rollback: reinserisci il giocatore
                self.client.table('Rose').insert({
                    'IDutente': user_id,
                    'IDgiocatore': player_id,
                    'prezzo': prezzo
                }).execute()
                return False
            
            crediti_attuali = user_response.data.get('crediti', 0) or 0
            
            # 4. Aggiungi crediti (CONVERTI IN INT)
            nuovi_crediti = int(crediti_attuali + prezzo)  # ← FIX: Converti in int
            
            credits_response = self.client.table('Utenti').update({
                'crediti': nuovi_crediti  # Ora è int, non float
            }).eq('id', user_id).execute()
            
            if not credits_response.data:
                print(f"⚠️ Errore aggiornamento crediti")
                # Rollback: reinserisci il giocatore
                self.client.table('Rose').insert({
                    'IDutente': user_id,
                    'IDgiocatore': player_id,
                    'prezzo': prezzo
                }).execute()
                return False
            
            print(f"✅ Giocatore {player_id} venduto dall'utente {user_id} per €{prezzo}")
            print(f"💰 Crediti aggiornati: {crediti_attuali} → {nuovi_crediti}")
            return True
                
        except Exception as e:
            print(f"❌ Errore sell_player: {e}")
            return False

    def convert_currency(self, user_id: int, direction: str, amount: int) -> bool:
        try:
            if not self.client:
                return False
            
            # 1. Recupera saldi attuali
            user_response = self.client.table('Utenti').select('crediti, crediti_scommesse').eq('id', user_id).single().execute()
            
            if not user_response.data:
                print(f"⚠️ Utente {user_id} non trovato")
                return False
            
            crediti_attuali = user_response.data.get('crediti', 0) or 0
            crediti_scommesse_attuali = user_response.data.get('crediti_scommesse', 0) or 0
            
            # 2. Calcola nuovi saldi in base alla direzione
            if direction == 'credits_to_bets':
                # Crediti → C.Scommesse (10:1)
                if crediti_attuali < amount:
                    print(f"⚠️ Crediti insufficienti: {crediti_attuali} < {amount}")
                    return False
                
                nuovi_crediti = int(crediti_attuali - amount)
                nuovi_crediti_scommesse = float(crediti_scommesse_attuali + (amount / 10.0))
            
            elif direction == 'bets_to_credits':
                # C.Scommesse → Crediti (1:10)
                if crediti_scommesse_attuali < amount:
                    print(f"⚠️ C.Scommesse insufficienti: {crediti_scommesse_attuali} < {amount}")
                    return False
                
                nuovi_crediti = int(crediti_attuali + (amount * 10))
                nuovi_crediti_scommesse = float(crediti_scommesse_attuali - amount)
            
            else:
                print(f"❌ Direzione non valida: {direction}")
                return False
            
            # 3. Aggiorna il database
            update_response = self.client.table('Utenti').update({
                'crediti': nuovi_crediti,
                'crediti_scommesse': nuovi_crediti_scommesse
            }).eq('id', user_id).execute()
            
            if not update_response.data:
                print(f"⚠️ Errore aggiornamento saldi")
                return False
            
            print(f"✅ Conversione completata per utente {user_id}")
            print(f"💰 Crediti: {crediti_attuali} → {nuovi_crediti}")
            print(f"🎲 C.Scommesse: {crediti_scommesse_attuali} → {nuovi_crediti_scommesse}")
            return True
                
        except Exception as e:
            print(f"❌ Errore convert_currency: {e}")
            return False

    def save_formazione(self, user_id: int, giornata: int, formazione: Dict[str, Any]) -> bool:
        """
        Salva la formazione di un utente per una giornata specifica
        
        Args:
            user_id: ID dell'utente
            giornata: Numero della giornata
            formazione: Dict con struttura {'titolari': {...}, 'panchina': {...}}
        
        Returns:
            True se salvataggio riuscito, False altrimenti
        """
        try:
            if not self.client:
                return False
            
            # 1. Elimina formazione esistente per questa giornata
            self.client.table('Formazioni').delete().eq('IDutente', user_id).eq('giornata', giornata).execute()
            
            # 2. Prepara i dati da inserire
            rows_to_insert = []
            
            for categoria in ['titolari', 'panchina']:
                for ruolo, giocatori in formazione[categoria].items():
                    for idx, giocatore in enumerate(giocatori):
                        if giocatore:  # Solo se c'è un giocatore assegnato
                            rows_to_insert.append({
                                'IDutente': user_id,
                                'IDgiocatore': giocatore.get('id'),
                                'giornata': giornata,
                                'posizione': f'{categoria}_{ruolo}_{idx}'
                            })
            
            # 3. Inserisci tutti i giocatori
            if rows_to_insert:
                response = self.client.table('Formazioni').insert(rows_to_insert).execute()
                
                if response.data:
                    print(f"✅ Formazione salvata: {len(rows_to_insert)} giocatori per giornata {giornata}")
                    return True
                else:
                    print(f"⚠️ Errore inserimento formazione")
                    return False
            else:
                print(f"⚠️ Nessun giocatore da salvare")
                return False
                
        except Exception as e:
            print(f"❌ Errore save_formazione: {e}")
            return False

