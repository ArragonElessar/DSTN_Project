import os
import subprocess
import time
from topics import check_if_kafka_is_running, get_all_topics, describe_topic, create_topic, delete_topic


ZOOKEEPER_SERVER_PORT = 2181
KAFKA_SERVER_PORT = 9092
BASE_DIR = "/mnt/e/"

def start_kafka_producer(topic_name, server_name="localhost"):

    os.chdir(BASE_DIR + "kafka_2.13-3.5.0")

    print(f"Starting Kafka Producer on topic: {topic_name}")

    # Create a subprocess and set up the pipe from the file to its stdin
    process = subprocess.Popen(
        ['bash', 'bin/kafka-console-producer.sh', '--bootstrap-server',
                            f"{server_name}:{KAFKA_SERVER_PORT}", '--topic', topic_name],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Read the contents of the file and write them to the subprocess's stdin
    input_data = "Sample Data\n".encode()
    process.stdin.write(input_data)
    process.stdin.write(f"Written data once. Sending Again\n".encode())
    process.stdin.write(input_data)

    process.stdin.close()  # Close stdin to signal the end of input

    print("Transferred all the data to the topic, closing the producer.")    


def main():
    # check if the kafka server is running
    check_if_kafka_is_running()
    start_kafka_producer("pranav")

    # delete the test topic
if __name__ == "__main__":
    main()
