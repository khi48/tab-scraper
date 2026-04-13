from flask import Flask, jsonify
from flask_cors import CORS
import threading
import time

app = Flask(__name__)
CORS(app)

# Global state
state = {
    'trigger': False,
    'horse_number': None,
    'meeting_name': None,
    'race_id': None,
    'success': -1,
    'additional_data': {}
}

@app.route('/trigger')
def get_trigger():
    return jsonify({
        'trigger': state['trigger'],
        'horseNumber': state['horse_number'],
        'meetingName': state['meeting_name'],
        'raceId': state['race_id'],
    })

@app.route('/data')
def get_data():
    return jsonify({
        'horseNumber': state['horse_number'],
        'meeting': state['meeting_name'],
        'raceId': state['race_id'],
        'success': state['success'],
        'additionalData': state['additional_data']
    })

def reset_trigger():
    """Reset trigger state after a delay"""
    time.sleep(1)
    state['trigger'] = False

@app.route('/activate/<horse_number>/<meeting_name>/<race_id>/', methods=['POST'])
def activate_trigger(horse_number, meeting_name, race_id):
    state['trigger'] = True
    state['horse_number'] = horse_number
    state['meeting_name'] = meeting_name
    state['race_id'] = race_id
    threading.Thread(target=reset_trigger).start()
    return jsonify({'status': 'success', 'message': f'Trigger activated for horse: {horse_number} with meeting_name: {meeting_name} and race_id: {race_id}'})

@app.route('/success/<success_state>/', methods=['POST'])
def success(success_state):
    state['success'] = int(success_state)
    return jsonify({'status': success_state})

def main():
    try:
        # Changed host to '0.0.0.0' to allow external access
        app.run(host='0.0.0.0', port=3000)
    except Exception as e:
        print(f"Server error: {e}")

if __name__ == '__main__':
    main()