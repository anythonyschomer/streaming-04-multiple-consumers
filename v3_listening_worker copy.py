import pika
import sys
import datetime
import csv
from util_logger import setup_logger  # Import the setup_logger function from util_logger.py

# Set up the logger
logger, log_file_name = setup_logger(__file__)

# Define the callback function to handle incoming messages
def callback(ch, method, properties, body):
    """Callback function to handle incoming messages."""
    try:
        # Decode the message body
        message = body.decode()
        
        # Print the received message along with the timestamp
        logger.info(f"[x] Received '{message}' at {datetime.datetime.now()}")

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"An error occurred while processing the message: {e}")

# Define the main function
def main(hn: str = "localhost", qn: str = "task_queue"):
    """Continuously listen for task messages on a named queue."""
    try:
        # Create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    except Exception as e:
        logger.error("ERROR: connection to RabbitMQ server failed.")
        logger.error(f"Verify the server is running on host={hn}.")
        logger.error(f"The error says: {e}")
        sys.exit(1)

    try:
        # Use the connection to create a communication channel
        channel = connection.channel()

        # Declare a durable queue
        channel.queue_declare(queue=qn, durable=True)

        # Configure the channel to listen on a specific queue,
        # Use the callback function named callback,
        # And do not auto-acknowledge the message (let the callback handle it)
        channel.basic_qos(prefetch_count=1)  # Limit to one message at a time per consumer
        channel.basic_consume(queue=qn, on_message_callback=callback)

        # Print a message to the console for the user
        logger.info(" [*] Ready for work. To exit press CTRL+C")

        # Start consuming messages via the communication channel
        channel.start_consuming()

    except Exception as e:
        logger.error("ERROR: something went wrong.")
        logger.error(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        logger.info("\nClosing connection. Goodbye.\n")
        connection.close()


# Execute the main function if this script is run directly
if __name__ == "__main__":
    main("localhost", "task_queue2")