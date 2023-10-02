import os
import subprocess

ZOOKEEPER_SERVER_PORT = 2181
KAFKA_SERVER_PORT = 9092
BASE_DIR = "/mnt/e/"

def check_if_kafka_is_running():
    kafka_running = False
    zookeeper_running = False

    p1 = subprocess.run(["fuser", f"{KAFKA_SERVER_PORT}/tcp"], capture_output=True, text=True)
    if p1.stdout != "":
        kafka_running = True
    
    p2 = subprocess.run(["fuser", f"{ZOOKEEPER_SERVER_PORT}/tcp"], capture_output=True, text=True) 
    if p2.stdout != "":
        zookeeper_running = True
    
    if kafka_running and zookeeper_running:
        print("Kafka and Zookeeper are running, ready to go.")
        return True
    else:
        print("Kafka and Zookeeper are not running, please start them first.")
        exit(0)

def get_all_topics(server_name="localhost", print_topics=False):
    # change the thread working directory to kafka
    os.chdir(BASE_DIR + "kafka_2.13-3.5.0")
    
    # bash script to get all the topics
    p1 = subprocess.run(['bash', "bin/kafka-topics.sh", "--bootstrap-server",f"{server_name}:{KAFKA_SERVER_PORT}","--list"], capture_output=True, text=True)
    
    if not print_topics:
        return p1.stdout.split("\n")
    else:
        print("Topics: ", end=" ")
        for topic in p1.stdout.split("\n"):
            print(topic, end=" ")
        print()
    
def describe_topic(topic_name="", server_name="localhost"):

    args = ["bash", "bin/kafka-topics.sh", "--bootstrap-server", f"{server_name}:{KAFKA_SERVER_PORT}","--describe"]

    if topic_name != "" and topic_name != "__consumer_offsets":
        args.append("--topic")
        args.append(topic_name)

    os.chdir(BASE_DIR + "kafka_2.13-3.5.0")
    
    p1 = subprocess.run(args, capture_output=True, text=True)
    
    if p1.stdout == "":
        print("No such topic found")
        print(p1.stderr)

    else:
        print(p1.stdout)

def create_topic(topic_name, partitions=1, replication_factor=1,server_name="localhost"):
    
    # first check if the topic already exists
    if topic_name in get_all_topics(server_name):
        print("Topic already exists")
        return


    os.chdir(BASE_DIR + "kafka_2.13-3.5.0")
    p1 = subprocess.run(
        ['bash', 'bin/kafka-topics.sh', '--bootstrap-server', f"{server_name}:{KAFKA_SERVER_PORT}", '--create', '--topic', topic_name, '--partitions', f"{partitions}", '--replication-factor', f"{replication_factor}"]
    , capture_output=True, text=True)

    if p1.stdout == "":
        print("Error While Creating Topic")
        print(p1.stderr)

    else:
        print(p1.stdout)

def delete_topic(topic_name, server_name="localhost"):
    os.chdir(BASE_DIR + "kafka_2.13-3.5.0")
    p1 = subprocess.run(
        ['bash', 'bin/kafka-topics.sh', '--bootstrap-server', f"{server_name}:{KAFKA_SERVER_PORT}", '--delete', '--topic', topic_name]
    , capture_output=True, text=True)

    if p1.stdout == "":
        print(f"Topic {topic_name} successfully deleted")
    else:
        print(p1.stderr)