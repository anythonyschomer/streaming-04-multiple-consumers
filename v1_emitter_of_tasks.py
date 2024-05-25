#Anthony Schomer Running Version 1

import pika
import csv

# create a blocking connection to the RabbitMQ server
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
# use the connection to create a communication channel
channel = connection.channel()
# use the channel to declare a durable queue
# a durable queue will survive a RabbitMQ server restart
# and help ensure messages are processed in order
# messages will not be deleted until the consumer acknowledges
channel.queue_declare(queue="task_queue", durable=True)

# create a list of 6 tasks
tasks = [
    "First task...",
    "Second task...",
    "Third task...",
    "Fourth task...",
    "Fifth task...",
    "Sixth task..."
]

# open the CSV file for writing
with open("tasks.csv", mode="w", newline="") as file:
    writer = csv.writer(file)

    # publish each task to the queue and write it to the CSV file
    for task in tasks:
        message = task
        channel.basic_publish(
            exchange="",
            routing_key="task_queue",
            body=message,
            properties=pika.BasicProperties(delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE),
        )
        print(f" [x] Sent {message}")
        writer.writerow([message])  # Write the task to the CSV file

# close the connection to the server
connection.close()