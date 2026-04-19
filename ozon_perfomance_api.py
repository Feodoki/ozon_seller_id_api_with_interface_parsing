import traceback
from config import OZON_PERFORMANCE_CLIENT_ID, OZON_PERFORMANCE_CLIENT_SECRET
import requests

class OzonPerfomanceAPI:
    def __init__(self):
        self.OZON_PERFORMANCE_CLIENT_ID = OZON_PERFORMANCE_CLIENT_ID
        self.OZON_PERFORMANCE_CLIENT_SECRET = OZON_PERFORMANCE_CLIENT_SECRET

    def _get_access_token(self):
        url = "https://api-performance.ozon.ru/api/client/token"
        payload = {
            "client_id": OZON_PERFORMANCE_CLIENT_ID,
            "client_secret": OZON_PERFORMANCE_CLIENT_SECRET,
            "grant_type": "client_credentials"
        }
        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)
        try:
            data = response.json()
            return data["access_token"]
        except:
            print(traceback.format_exc())
            print(response.status_code)
            print(response.text)



if __name__ == "__main__":
    api = OzonPerfomanceAPI()
    res = api._get_access_token()
    print(res)