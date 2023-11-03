import sys
import requests
import json
import os

def send_slack_alert(error):
    print("inside alert")
    url = os.environ.get('slack_url', 'Specified environment variable is not set.')
    message = str(error)
    title = (f"Error Alert")
    slack_data = {
        "username": "ErrorAlertBot",
        "icon_emoji": ":satellite:",
        #"channel" : "#de-pipeline-alerts",
        "attachments": [
            {
                "color": "#9733EE",
                "fields": [
                    {
                        "title": title,
                        "value": message,
                        "short": "false",
                    }
                ]
            }
        ]
    }
    byte_length = str(sys.getsizeof(slack_data))
    headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
    response = requests.post(url, data=json.dumps(slack_data), headers=headers)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

