from flask import Flask, request, jsonify
from flask_cors import CORS
from reader_database import DatabaseReader
from writer_database import DatabaseWriter
import os

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
        status = request.args.get('status')
        partite = db_reader.get_matches(status)
        return jsonify({
            'success': True,
            'data': partite,
            'count': len(partite)
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

# ===== CLASSIFICA (LETTURA) =====
@app.route('/api/classifica', methods=['GET'])
@require_api_key
def get_classifica():
    """Recupera la classifica"""
    try:
        limit = request.args.get('limit', 50, type=int)
        classifica = db_reader.get_ranking(limit)
        return jsonify({'success': True, 'data': classifica})
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

# ===== ERROR HANDLERS =====
@app.errorhandler(404)
def not_found(error):
    return jsonify({'success': False, 'error': 'Endpoint non trovato'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'success': False, 'error': 'Errore server'}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 10000))
    app.run(host='0.0.0.0', port=port, debug=False)

