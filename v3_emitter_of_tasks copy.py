"""
    This program sends messages from a CSV file to a queue on the RabbitMQ server.

    Author: Your Name
    Date: Current Date

"""

import pika
import csv
import webbrowser
import time

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_messages(host: str, queue_name: str, file_path: str, delay: float):
    """
    Reads tasks from a CSV file and sends them as messages to the queue.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        file_path (str): the path to the CSV file containing tasks
        delay (float): the delay in seconds between sending each message
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)

        # open the CSV file and read tasks
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                message = row[0]  # assuming the task is in the first column
                # use the channel to publish a message to the queue
                ch.basic_publish(exchange="", routing_key=queue_name, body=message)
                print(f" [x] Sent {message}")
                time.sleep(delay)  # delay before sending the next message

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
    finally:
        # close the connection to the server
        conn.close()

# Standard Python idiom to indicate main program entry point
if __name__ == "__main__":
    # ask the user if they'd like to open the RabbitMQ Admin site
    offer_rabbitmq_admin_site()
    # send the messages from the CSV file with a delay of 1 second between each message
    send_messages("localhost", "task_queue2", "tasks.csv", 1.0)