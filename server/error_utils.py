import requests
import json
from config import CONFIG

def send_error_to_slack(error_message: str, title: str):
    """
    Sends an error message with a title to Slack via an incoming webhook URL.

    Args:
        error_message (str): The error details to send.
        title (str): A short title for the error.
    """
    # Replace this with your actual Slack webhook URL
    webhook_url = CONFIG.slack_webhook_url

    # Format the payload with your title and error message
    payload = {
        "text": f"*{title}*\n{error_message}"
    }

    # Send the POST request to Slack
    response = requests.post(
        webhook_url,
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"}
    )

    # Slack returns "ok" if the request was successful
    if response.status_code != 200 or response.text != "ok":
        raise Exception(
            f"Failed to send message to Slack. "
            f"Status code: {response.status_code}, Response: {response.text}"
        )

# Example usage:
if __name__ == "__main__":
    send_error_to_slack(
        error_message="Something unexpected happened!",
        title="Server Warning"
    )