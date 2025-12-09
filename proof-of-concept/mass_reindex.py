import requests
import sys
import random
import uuid

URL_TEMPLATE = "http://localhost:5005/myjob/{}"
NUM_REQUESTS = 1000

def send_request(i):
    entity_id = str(uuid.uuid4())
    priority = random.randint(1, 3)
    url = f"{URL_TEMPLATE.format(entity_id)}?priority={priority}"
    
    try:
        response = requests.get(url, timeout=5)
        if response.status_code < 203:
            data = response.json()
            job_id = data.get("job_id")
            return f"Request {i} succeeded: job_id={job_id}, entity_id={entity_id}, priority={priority}"
        else:
            return f"Request {i} failed with status {response.status_code}. {response.text}"
    except Exception as e:
        return f"Request {i} exception: {e}"

def main():
    for i in range(NUM_REQUESTS):
        print(send_request(i))

if __name__ == "__main__":
    main()