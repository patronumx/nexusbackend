import requests

url = "http://localhost:8000/api/token/"
data = {
    "username": "employee_ops",
    "password": "Staff_Access_2026!"
}

try:
    response = requests.post(url, json=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response Body: {response.json()}")
except Exception as e:
    print(f"Error: {e}")
