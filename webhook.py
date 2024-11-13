from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    # Assuming the webhook sends a JSON payload
    data = request.get_json()

    # Handle the incoming data
    print(f"Received data: {data}")

    # Respond back with a success message
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5001)
