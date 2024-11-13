import requests

def trigger_webhook(webhook_url, data):
    if webhook_url:
        try:
            requests.post(webhook_url, json=data)
        except requests.RequestException:
            print("Failed to send webhook.")
