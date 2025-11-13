from flask import Flask, request, jsonify
from flask_cors import CORS
from reader_database import DatabaseReader
from writer_database import DatabaseWriter
import os
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | SERVER | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Inizializza reader e writer
API_KEY = os.getenv('API_SECRET_KEY', 'tua_chiave_segreta')
db_reader = DatabaseReader()
db_writer = DatabaseWriter()

def require_api_key(f):
    """Middleware per verificare API key"""
    def wrapper(*args, **kwargs):
        api_key = request.headers.get('X-API-Key')
        if not api_key or api_key != API_KEY:
            return jsonify({'success': False, 'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

# ===== HEALTH CHECK =====
@app.route('/')
def home():
    """Health check"""
    return jsonify({
        'status': 'online',
        'message': 'Fantacalcio Server v1.0'
    })

@app.route('/health')
def health():
    """Verifica connessione database"""
    return jsonify({'status': 'ok', 'database': 'connected'})

# ===== PARTITE (LETTURA) =====
@app.route('/api/partite', methods=['GET'])
@require_api_key
def get_partite():
    """Recupera tutte le partite"""
    try:
        status = request.args.get('stato')
        giornata = request.args.get('giornata', type=int)  # ✅ AGGIUNGI
        partite = db_reader.get_matches(status, giornata)  # ✅ AGGIUNGI giornata
        return jsonify({
            'success': True,
            'data': partite,
            'count': len(partite)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/partite/giornate', methods=['GET'])
@require_api_key
def get_giornate_partite():
    """Recupera tutte le giornate disponibili per le partite"""
    try:
        giornate = db_reader.get_available_giornate_partite()
        return jsonify({
            'success': True,
            'data': giornate
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/partite/<int:match_id>', methods=['GET'])
@require_api_key
def get_partita(match_id):
    """Recupera dettagli di una partita specifica"""
    try:
        partita = db_reader.get_match_details(match_id)
        if partita:
            return jsonify({'success': True, 'data': partita})
        return jsonify({'success': False, 'error': 'Partita non trovata'}), 404
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== SQUADRE (LETTURA) =====
@app.route('/api/squadre', methods=['GET'])
@require_api_key
def get_squadre():
    """Recupera tutte le squadre"""
    try:
        squadre = db_reader.get_all_teams()
        return jsonify({
            'success': True,
            'data': squadre,
            'count': len(squadre)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/squadre/<username>', methods=['GET'])
@require_api_key
def get_squadra_utente(username):
    """Recupera la squadra di un utente"""
    try:
        squadra = db_reader.get_user_team(username)
        return jsonify({'success': True, 'data': squadra})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== GIOCATORI (LETTURA) =====
@app.route('/api/giocatori', methods=['GET'])
@require_api_key
def get_giocatori():
    """Recupera tutti i giocatori"""
    try:
        giocatori = db_reader.get_all_players()
        return jsonify({
            'success': True,
            'data': giocatori,
            'count': len(giocatori)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/giocatori/search', methods=['GET'])
@require_api_key
def search_giocatori():
    """Cerca giocatori per nome"""
    try:
        search_term = request.args.get('q', '')
        giocatori = db_reader.search_players(search_term)
        return jsonify({
            'success': True,
            'data': giocatori,
            'count': len(giocatori)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== SCOMMESSE (LETTURA) =====
@app.route('/api/scommesse/<username>', methods=['GET'])
@require_api_key
def get_scommesse_utente(username):
    """Recupera le scommesse di un utente"""
    try:
        scommesse = db_reader.get_user_bets(username)
        return jsonify({'success': True, 'data': scommesse})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== SCRITTURA - SQUADRE =====
@app.route('/api/squadre', methods=['POST'])
@require_api_key
def create_squadra():
    """Crea una nuova squadra"""
    try:
        data = request.get_json()
        if not data.get('owner') or not data.get('name'):
            return jsonify({
                'success': False,
                'error': 'owner e name obbligatori'
            }), 400
        team_id = db_writer.create_team(
            data['owner'],
            data['name']
        )
        if team_id:
            return jsonify({
                'success': True,
                'data': {'id': team_id}
            }), 201
        return jsonify({'success': False, 'error': 'Errore creazione'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/squadre/<int:team_id>', methods=['PUT'])
@require_api_key
def update_squadra(team_id):
    """Aggiorna una squadra"""
    try:
        data = request.get_json()
        success = db_writer.update_team(team_id, data)
        if success:
            return jsonify({'success': True, 'data': {'id': team_id}})
        return jsonify({'success': False, 'error': 'Errore aggiornamento'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== SCRITTURA - GIOCATORI IN SQUADRA =====
@app.route('/api/team/<int:team_id>/giocatori', methods=['POST'])
@require_api_key
def add_giocatore_squadra(team_id):
    """Aggiunge un giocatore a una squadra"""
    try:
        data = request.get_json()
        player_id = data.get('player_id')
        if not player_id:
            return jsonify({
                'success': False,
                'error': 'player_id obbligatorio'
            }), 400
        success = db_writer.add_player_to_team(team_id, player_id)
        if success:
            return jsonify({'success': True}), 201
        return jsonify({'success': False, 'error': 'Errore aggiunta'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/giocatori/disponibili', methods=['GET'])
@require_api_key
def get_giocatori_disponibili():
    """Recupera tutti i giocatori disponibili per l'acquisto"""
    try:
        giocatori = db_reader.get_available_players()
        return jsonify({
            'success': True,
            'data': giocatori,
            'count': len(giocatori)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== SCRITTURA - SCOMMESSE =====
@app.route('/api/scommesse', methods=['POST'])
@require_api_key
def create_scommessa():
    """Crea una nuova scommessa"""
    try:
        data = request.get_json()
        required = ['username', 'match_id', 'bet_type', 'amount', 'odds']
        for field in required:
            if field not in data:
                return jsonify({
                    'success': False,
                    'error': f'{field} obbligatorio'
                }), 400
        bet_id = db_writer.create_bet(
            data['username'],
            data['match_id'],
            data['bet_type'],
            data['amount'],
            data['odds']
        )
        if bet_id:
            return jsonify({
                'success': True,
                'data': {'id': bet_id}
            }), 201
        return jsonify({'success': False, 'error': 'Errore creazione'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== SCRITTURA - SCHEDINE =====
@app.route('/api/schedine', methods=['POST'])
@require_api_key
def create_schedina():
    """Crea una nuova schedina (una riga per ogni scommessa)"""
    try:
        data = request.get_json()
        
        # Validazione campi obbligatori
        if not data.get('user_id') or not data.get('scommesse'):
            return jsonify({
                'success': False,
                'error': 'user_id e scommesse obbligatori'
            }), 400
        
        if not isinstance(data['scommesse'], list) or len(data['scommesse']) == 0:
            return jsonify({
                'success': False,
                'error': 'scommesse deve essere una lista non vuota'
            }), 400
        
        success = db_writer.create_schedina(
            data['user_id'],
            data['scommesse']
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': f"{len(data['scommesse'])} scommesse salvate"
            }), 201
        
        return jsonify({'success': False, 'error': 'Errore creazione schedina'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== LETTURA SCHEDINE UTENTE =====
@app.route('/api/schedine/<int:user_id>', methods=['GET'])
@require_api_key
def get_user_schedine(user_id):
    """Recupera le schedine di un utente"""
    try:
        giornata = request.args.get('giornata', type=int)
        schedine = db_reader.get_user_schedine(user_id, giornata)
        return jsonify({
            'success': True,
            'data': schedine,
            'count': len(schedine)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
        
# ===== AUTENTICAZIONE =====
@app.route('/api/auth/check-user', methods=['POST'])
@require_api_key
def check_user():
    """Verifica se un username esiste già"""
    try:
        data = request.get_json()
        username = data.get('username')
        
        if not username:
            return jsonify({'success': False, 'error': 'username obbligatorio'}), 400
        
        exists = db_reader.check_user_exists(username)
        return jsonify({'success': True, 'exists': exists})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/auth/register', methods=['POST'])
@require_api_key
def register_user():
    """Registra un nuovo utente"""
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return jsonify({'success': False, 'error': 'username e password obbligatori'}), 400
        
        # Verifica che l'utente non esista già
        if db_reader.check_user_exists(username):
            return jsonify({'success': False, 'error': 'Username già esistente'}), 409
        
        # Crea l'utente
        user_id = db_writer.create_user(username, password)
        
        if user_id:
            return jsonify({
                'success': True,
                'data': {'id': user_id, 'username': username}
            }), 201
        
        return jsonify({'success': False, 'error': 'Errore creazione utente'}), 500
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/auth/login', methods=['POST'])
@require_api_key
def login_user():
    """Effettua il login"""
    try:
        data = request.get_json()
        username = data.get('username')
        password = data.get('password')
        
        if not username or not password:
            return jsonify({'success': False, 'error': 'username e password obbligatori'}), 400
        
        # Verifica credenziali
        if db_reader.verify_user_login(username, password):
            user_id = db_reader.get_user_id(username)
            return jsonify({
                'success': True,
                'data': {'id': user_id, 'username': username}
            })
        
        return jsonify({'success': False, 'error': 'Credenziali non valide'}), 401
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
        
# ===== CLASSIFICA =====
@app.route('/api/classifica', methods=['GET'])
@require_api_key
def get_classifica():
    """Recupera la classifica"""
    try:
        giornata = request.args.get('giornata', type=int)
        classifica = db_reader.get_ranking(giornata)
        return jsonify({
            'success': True,
            'data': classifica,
            'count': len(classifica)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/classifica/giornate', methods=['GET'])
@require_api_key
def get_giornate():
    """Recupera tutte le giornate disponibili"""
    try:
        giornate = db_reader.get_available_giornate()
        return jsonify({
            'success': True,
            'data': giornate
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
        
# ===== ERROR HANDLERS =====
@app.errorhandler(404)
def not_found(error):
    return jsonify({'success': False, 'error': 'Endpoint non trovato'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'success': False, 'error': 'Errore server'}), 500
        
@app.route('/api/schedine/<int:user_id>/<int:match_id>', methods=['DELETE'])
@require_api_key
def delete_schedina(user_id, match_id):
    """
    Elimina una scommessa specifica
    
    Args:
        user_id: ID dell'utente
        match_id: ID della partita
    """
    try:
        success = db_writer.delete_schedina(user_id, match_id)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Scommessa eliminata per partita {match_id}'
            })
        
        return jsonify({
            'success': False, 
            'error': 'Scommessa non trovata o errore eliminazione'
        }), 404
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== ROSA (LETTURA) =====
@app.route('/api/rosa/<int:user_id>', methods=['GET'])
@require_api_key
def get_user_rosa(user_id):
    """Recupera la rosa dell'utente"""
    try:
        rosa = db_reader.get_user_rosa(user_id)
        return jsonify({
            'success': True,
            'data': rosa,
            'count': len(rosa)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/rosa/vendi', methods=['POST'])
@require_api_key
def vendi_giocatore():
    """Vende un giocatore e lo rimuove dalla rosa"""
    try:
        data = request.get_json()
        
        # Validazione campi obbligatori
        if not data.get('user_id') or not data.get('player_id') or not data.get('prezzo'):
            return jsonify({
                'success': False,
                'error': 'user_id, player_id e prezzo obbligatori'
            }), 400
        
        success = db_writer.sell_player(
            data['user_id'],
            data['player_id'],
            data['prezzo']
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Giocatore venduto con successo'
            }), 200
        
        return jsonify({
            'success': False, 
            'error': 'Errore vendita giocatore'
        }), 500
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== GIOCATORI DISPONIBILI (ESCLUDE ROSA UTENTE) =====
@app.route('/api/giocatori/disponibili/<int:user_id>', methods=['GET'])
@require_api_key
def get_giocatori_disponibili_utente(user_id):
    """Recupera tutti i giocatori disponibili per l'acquisto (esclusi quelli già nella rosa)"""
    try:
        giocatori = db_reader.get_available_players(user_id)
        return jsonify({
            'success': True,
            'data': giocatori,
            'count': len(giocatori)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== ACQUISTO GIOCATORE (SCRITTURA) =====
@app.route('/api/rosa/acquista', methods=['POST'])
@require_api_key
def acquista_giocatore():
    """Acquista un giocatore e lo aggiunge alla rosa"""
    try:
        data = request.get_json()
        
        # Validazione campi obbligatori
        if not data.get('user_id') or not data.get('player_id') or not data.get('prezzo'):
            return jsonify({
                'success': False,
                'error': 'user_id, player_id e prezzo obbligatori'
            }), 400
        
        success = db_writer.buy_player(
            data['user_id'],
            data['player_id'],
            data['prezzo']
        )
        
        if success:
            return jsonify({
                'success': True,
                'message': 'Giocatore acquistato con successo'
            }), 201
        
        return jsonify({
            'success': False, 
            'error': 'Errore acquisto giocatore'
        }), 500
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)

# ===== VALUTE - LETTURA =====
@app.route('/api/utenti/<int:user_id>/currencies', methods=['GET'])
@require_api_key
def get_user_currencies(user_id):
    """Recupera crediti e crediti_scommesse di un utente"""
    try:
        currencies = db_reader.get_user_currencies(user_id)
        
        if currencies is not None:
            return jsonify({
                'success': True,
                'data': currencies
            })
        
        return jsonify({
            'success': False, 
            'error': 'Utente non trovato'
        }), 404
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== VALUTE - CONVERSIONE =====
@app.route('/api/valute/converti', methods=['POST'])
@require_api_key
def convert_currency():
    """Converte valute tra Crediti e C.Scommesse"""
    try:
        data = request.get_json()
        
        # Validazione campi obbligatori
        if not data.get('user_id') or not data.get('direction') or not data.get('amount'):
            return jsonify({
                'success': False,
                'error': 'user_id, direction e amount obbligatori'
            }), 400
        
        user_id = data['user_id']
        direction = data['direction']
        amount = data['amount']
        
        # Validazione direzione
        if direction not in ['credits_to_bets', 'bets_to_credits']:
            return jsonify({
                'success': False,
                'error': 'direction deve essere credits_to_bets o bets_to_credits'
            }), 400
        
        # Validazione amount
        try:
            amount = int(amount)
            if amount <= 0:
                raise ValueError("Amount deve essere positivo")
        except (ValueError, TypeError):
            return jsonify({
                'success': False,
                'error': 'amount deve essere un numero intero positivo'
            }), 400
        
        # Esegui conversione
        success = db_writer.convert_currency(user_id, direction, amount)
        
        if success:
            # Prepara messaggio di successo
            if direction == 'credits_to_bets':
                converted = amount / 10.0
                message = f'Convertiti €{amount} in {converted:.1f} C.Scommesse'
            else:
                converted = amount * 10
                message = f'Convertiti {amount} C.Scommesse in €{converted}'
            
            return jsonify({
                'success': True,
                'message': message
            }), 200
        
        return jsonify({
            'success': False, 
            'error': 'Saldo insufficiente o errore conversione'
        }), 400
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== FORMAZIONE - LETTURA =====
@app.route('/api/formazione/<int:user_id>/<int:giornata>', methods=['GET'])
@require_api_key
def get_formazione(user_id, giornata):
    """Recupera la formazione di un utente per una giornata"""
    try:
        formazione = db_reader.get_user_formazione(user_id, giornata)
        
        return jsonify({
            'success': True,
            'data': formazione,
            'count': len(formazione)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

# ===== FORMAZIONE - SALVATAGGIO =====
@app.route('/api/formazione', methods=['POST'])
@require_api_key
def save_formazione():
    """Salva la formazione di un utente per una giornata"""
    try:
        data = request.get_json()
        
        # Validazione campi obbligatori
        if not data.get('user_id') or not data.get('giornata') or not data.get('formazione'):
            return jsonify({
                'success': False,
                'error': 'user_id, giornata e formazione obbligatori'
            }), 400
        
        user_id = data['user_id']
        giornata = data['giornata']
        formazione = data['formazione']
        
        # Salva formazione
        success = db_writer.save_formazione(user_id, giornata, formazione)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Formazione salvata per giornata {giornata}'
            }), 200
        
        return jsonify({
            'success': False, 
            'error': 'Errore salvataggio formazione'
        }), 500
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
    













