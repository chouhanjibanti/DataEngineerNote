"""
============================================================
Kafka Producer — CoinGecko Live Cryptocurrency Data
============================================================

PROJECT FLOW:
CoinGecko API -> Python Producer -> Apache Kafka Topic

WHAT DOES THIS FILE DO?

1. Fetch cryptocurrency market data from CoinGecko API.
2. Convert Python data into JSON format and then into bytes.
3. Send each cryptocurrency as a separate message to Kafka.
4. Wait 30 seconds and repeat the entire process.

IMPORTANT CONCEPTS:
- API: Used to fetch cryptocurrency market data.
- Producer: A program that sends messages to Kafka.
- Broker: The Kafka server that receives and stores messages.
- Topic: A logical channel where Kafka messages are stored.
- Message: A single record containing cryptocurrency information.
- JSON: A format used to represent structured data.
- Serialization: Converting data into a format suitable for transmission.

INSTALL DEPENDENCIES:
python -m pip install kafka-python requests

RUN THIS FILE:
python kafka/kafka_producer1.py

REQUIREMENTS:
1. Kafka must be running at localhost:9092.
2. The Kafka topic "crypto-prices" must exist, or the broker
   must be configured to create topics automatically.
============================================================
"""

# ============================================================
# PART 1: IMPORT REQUIRED LIBRARIES
# ============================================================

# json:
# Converts Python dictionaries into JSON strings.
# Kafka messages need to be serialized into bytes before sending.
import json

# time:
# Provides the sleep() function.
# We use it to pause the program between API requests.
import time

# requests:
# Sends HTTP requests to the CoinGecko REST API.
# We use it to fetch the latest cryptocurrency market data.
import requests

# KafkaProducer:
# A class provided by kafka-python.
# It allows our Python program to publish messages to Kafka.
from kafka import KafkaProducer


# ============================================================
# PART 2: CONFIGURATION
# ============================================================

# Address of the Kafka broker.
# localhost means Kafka is running on the same computer.
# Port 9092 is a common Kafka client-listener port.
KAFKA_BROKER = "localhost:9092"

# Name of the Kafka topic.
# A topic is a logical channel that stores related messages.
# Our cryptocurrency records will be published to this topic.
KAFKA_TOPIC = "crypto-prices"

# Number of seconds to wait between successive API requests.
# After fetching and sending one batch, the program sleeps
# for 30 seconds before starting the next batch.
FETCH_INTERVAL = 30

# CoinGecko's public REST API endpoint for market information.
# This endpoint returns information such as coin name, price,
# market capitalization and 24-hour price change.
API_URL = "https://api.coingecko.com/api/v3/coins/markets"


# ============================================================
# PART 3: CONNECT TO KAFKA
# ============================================================

# KafkaProducer creates a producer object.
# This object manages the connection and sends messages to Kafka.
producer = KafkaProducer(

    # bootstrap_servers tells the producer where to initially
    # connect to discover the Kafka cluster.
    bootstrap_servers=KAFKA_BROKER,

    # Kafka sends message values as bytes.
    # value_serializer automatically converts each Python
    # dictionary into a JSON string and then UTF-8 encoded bytes.
    #
    # Example:
    # Python dictionary -> JSON string -> bytes
    #
    # {"name": "Bitcoin"}
    #         |
    #         v
    # '{"name": "Bitcoin"}'
    #         |
    #         v
    # b'{"name": "Bitcoin"}'
    value_serializer=lambda data: json.dumps(data).encode("utf-8"),

    # key_serializer is not provided because we manually convert
    # each message key into bytes inside send_to_kafka().
)

# Display the connection configuration in the terminal.
print(f"Connected to Kafka at {KAFKA_BROKER}")

# Display the destination topic for our messages.
print(f"Target Kafka topic: '{KAFKA_TOPIC}'")

# Display the API polling interval.
print(f"Fetching data every {FETCH_INTERVAL} seconds\n")


# ============================================================
# PART 4: FETCH DATA FROM COINGECKO API
# ============================================================

def fetch_coins():
    """
    Fetch cryptocurrency market data from CoinGecko.

    Returns:
        A list of dictionaries, with each dictionary representing
        one cryptocurrency.
    """

    # Query parameters define which data the API should return.
    # They are sent as URL query parameters, not as a JSON body.
    params = {

        # Return cryptocurrency prices in US dollars.
        "vs_currency": "usd",

        # Sort results by market capitalization, highest first.
        "order": "market_cap_desc",

        # Request up to 100 cryptocurrency records per page.
        "per_page": 100,

        # Fetch the first page of results.
        "page": 1,

        # Do not include sparkline price-history data.
        # This reduces unnecessary response data.
        "sparkline": False,
    }

    # Send an HTTP GET request to the CoinGecko API.
    # params automatically adds the query parameters to the URL.
    # timeout=15 means the request will not wait indefinitely.
    response = requests.get(
        API_URL,
        params=params,
        timeout=15
    )

    # Raise an exception for HTTP errors, such as 429 or 500.
    # This prevents us from silently processing an error response.
    response.raise_for_status()

    # Convert the API's JSON response into Python objects.
    # For this endpoint, the successful response is normally
    # a list of dictionaries, one dictionary per cryptocurrency.
    coins = response.json()

    # Return the fetched data to the calling function.
    return coins


# ============================================================
# PART 5: SEND CRYPTOCURRENCY DATA TO KAFKA
# ============================================================

def send_to_kafka(coins):
    """
    Send each cryptocurrency as an individual Kafka message.

    Args:
        coins: A list of cryptocurrency dictionaries.
    """

    # Loop through the list one cryptocurrency at a time.
    for coin in coins:

        # Each iteration processes one cryptocurrency record.
        #
        # coin["id"] is a unique identifier, for example "bitcoin".
        # encode("utf-8") converts the ID string into bytes because
        # Kafka message keys must be serialized to bytes.
        #
        # key = identifies the message by cryptocurrency.
        # value = contains the cryptocurrency's market information.
        producer.send(

            # Destination topic where the message will be published.
            KAFKA_TOPIC,

            # Message key: unique cryptocurrency ID in bytes.
            # Using the same key helps Kafka route the same coin
            # to the same partition under the default partitioner,
            # provided the topic's partition configuration is stable.
            key=coin["id"].encode("utf-8"),

            # Message value: the complete Python dictionary.
            # The configured value_serializer converts it to JSON
            # bytes automatically before transmission.
            value=coin
        )

    # flush() waits for buffered producer messages to be sent.
    # Kafka acknowledgements and producer configuration determine
    # the level of delivery confirmation.
    #
    # Without waiting for buffered messages, the program could
    # move ahead while some messages are still being transmitted.
    producer.flush()

    # Print the number of records submitted to Kafka.
    print(
        f"Sent {len(coins)} coins to Kafka topic '{KAFKA_TOPIC}'"
    )


# ============================================================
# PART 6: MAIN EXECUTION LOOP
# ============================================================

# This counter tracks how many collection cycles have started.
# One batch means one API fetch cycle.
batch = 0

# Print a heading so students can identify the producer's logs.
print("=" * 50)
print("  PRODUCER STARTED — Ctrl+C to stop")
print("=" * 50)


# while True creates an infinite loop.
# The fetch-send-wait cycle repeats until the program is stopped
# or an unhandled exception terminates it.
while True:

    # Increase the batch number by one at the beginning
    # of every new collection cycle.
    batch += 1

    # Display the current batch number.
    print(f"\n[Batch #{batch}]")

    # --------------------------------------------------------
    # STEP 1: EXTRACT DATA
    # --------------------------------------------------------

    # Call fetch_coins() to retrieve the latest market data.
    # This is the extraction stage of our data pipeline.
    print("Fetching from CoinGecko API...")

    coins = fetch_coins()

    # len(coins) gives the number of cryptocurrency records
    # returned by the API.
    print(f"Got {len(coins)} coins")

    # --------------------------------------------------------
    # STEP 2: PREVIEW THE FETCHED DATA
    # --------------------------------------------------------

    # Display only the first three records.
    # coins[:3] is Python list slicing:
    # it returns items at index 0, 1 and 2, if available.
    #
    # This preview is only for monitoring.
    # It does not limit the number of records sent to Kafka.
    print("Preview (top 3):")

    # Iterate over the first three cryptocurrency dictionaries.
    for coin in coins[:3]:

        # Print the coin name, current USD price and 24-hour change.
        #
        # {coin['name']:15s}
        #   Displays the name in a field at least 15 characters wide.
        #
        # ${coin['current_price']:>12,.2f}
        #   >12 = right-align in a field 12 characters wide.
        #   ,   = add thousands separators.
        #   .2f = display two digits after the decimal point.
        #
        # {coin['price_change_percentage_24h']:+.2f}
        #   +   = display the sign for positive and negative values.
        #   .2f = display two digits after the decimal point.
        #
        # These formatting instructions change how values are
        # displayed; they do not modify the original data.
        print(
            f"    {coin['name']:15s}  "
            f"${coin['current_price']:>12,.2f}  "
            f"({coin['price_change_percentage_24h']:+.2f}% 24h)"
        )

    # --------------------------------------------------------
    # STEP 3: LOAD / PUBLISH DATA TO KAFKA
    # --------------------------------------------------------

    # Send the complete list of fetched records to Kafka.
    # Each coin is sent as a separate message.
    #
    # For example, if the API returns 100 coins, this function
    # attempts to publish 100 individual Kafka messages.
    send_to_kafka(coins)

    # --------------------------------------------------------
    # STEP 4: WAIT BEFORE THE NEXT COLLECTION CYCLE
    # --------------------------------------------------------

    # Display how long the producer will pause.
    print(f"Sleeping {FETCH_INTERVAL} seconds...")

    # Pause the current Python thread for 30 seconds.
    # After the delay, the while loop starts the next batch.
    time.sleep(FETCH_INTERVAL)
