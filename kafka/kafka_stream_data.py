import requests
import json
from kafka import KafkaProducer


LAST_PROCESSED_FILE = './data/last_processed.json'

def get_last_processed_date():
    try:
        with open(LAST_PROCESSED_FILE, 'r') as f:
            data = json.load(f)
        return data.get("last_processed", "1970-01-01T00:00:00Z")
    except FileNotFoundError:
        return "1970-01-01T00:00:00Z"

# Update the last date in the JSON file
def update_last_processed_date(date):
    with open(LAST_PROCESSED_FILE, 'w') as f:
        json.dump({"last_processed": date}, f)






# Function for extracting hydrometric data via the Hub'Eau API and sending it to Kafka
def stream_hydrometrie_data():
    last_processed = get_last_processed_date()
    print(f"Data extraction from {last_processed}")

    # Calculate deadline (1 month before current date)
    one_month_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%dT%H:%M:%S')

    # If the last processed data is older than a month, replace it by deadline limit
    if last_processed < one_month_ago:
        print(f"La date de début des observations est trop ancienne, ajustée à {one_month_ago}")
        last_processed = one_month_ago
    
    # Get current date for end date
    date_fin_obs = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')

    url = f"https://hubeau.eaufrance.fr/api/v1/hydrometrie/observations_tr?size=100&date_debut_obs={last_processed}&date_fin_obs={date_fin_obs}&sort=asc"
    response = requests.get(url)
    data = response.json()

    # Setting up the Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Send each record to the Kafka topic 'hydrometrie_topic'
    if data.get('data'):
        for record in data['data']:
            producer.send('hydrometrie_topic', record)
            
        latest_obs_date = data['data'][0]['date_obs']
        update_last_processed_date(latest_obs_date)
        print(f"Updated from last processing date to {latest_obs_date}")
    else : 
        print("Data hasn't been sent by the producer")
    producer.flush()