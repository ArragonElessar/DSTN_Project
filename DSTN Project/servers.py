import os
import subprocess
import time

# Step 1. Start the zookeeper server
# Step 2. Start the kafka server
# Step 3. Create a topic
# Step 4. Start the producer
# Step 5. Start the consumer

ZOOKEEPER_SERVER_PORT = 2181
KAFKA_SERVER_PORT = 9092
SIGTERM = 9
SIGINT = 2

# a dictionary to store the process ids, so that OS can manually fuck them off
pgid = {}

def clear_tcp_port(port):
    p1 = subprocess.run(['fuser', f"{port}/tcp", "-k"], capture_output=True, text=True)
    print(f"Zookeeper Port {port} open for use")

def start_zookeeper_server():

    print(f"{os.getpid()}: Starting Zookeeper Server")

    # Here is the child code
    with open("Logs/zookeeper.log", "w") as f:

        # change the thread working directory to kafka
        os.chdir("../")

        p1 = subprocess.run(['bash', "bin/zookeeper-server-start.sh", "config/zookeeper.properties"], stdout=f, stderr=f)

    print("Exiting start_zookeeper_server()")
        
def start_kafka_server():

    print(f"{os.getpid()}: Starting Kafka Server")

    with open("Logs/kafka.log", "w") as f:

        # change the thread working directory to kafka
        os.chdir("../")
        
        p1 = subprocess.run(['bash', 'bin/kafka-server-start.sh', 'config/server.properties'], stdout=f, stderr=f)

    print("Exiting start_kafka_server()")

def stop_zookeeper_server():
    # change the thread working directory to kafka
    os.chdir("../")
    p1 = subprocess.run(['bash', "bin/zookeeper-server-stop.sh"], capture_output=True, text=True)
    print(f"Zookeeper Server Stopped: {p1.stdout}")

def stop_kafka_server():
    # change the thread working directory to kafka
    os.chdir("../")
    p1 = subprocess.run(['bash', "bin/kafka-server-stop.sh"], capture_output=True, text=True)
    print(f"Kafka Server Stopped: {p1.stdout}")

def initiate_kafka():

    print()

    # clear tcp port
    clear_tcp_port(ZOOKEEPER_SERVER_PORT)
    clear_tcp_port(KAFKA_SERVER_PORT)

    print()

    # store the parent process's pid
    pgid['parent-process'] = os.getpid()

    # fork to create a new process for zookeeper server
    zookeeper_process = os.fork()
    if zookeeper_process == 0:
        # child process, start the zookeeper server
        start_zookeeper_server()
    
    elif zookeeper_process > 0:
        # Here is the parent code
        pgid['zookeeper-server-process'] = zookeeper_process
        print(f"{os.getpid()}: Created a child pid: {zookeeper_process}, which is running zookeeper server")


        # now create the kafka server
        kafka_process = os.fork()
        if kafka_process == 0:
            # Here call the kafka server
            start_kafka_server()

        elif kafka_process > 0:
            # Here is the parent code
            pgid['kafka-server-process'] = kafka_process

            # Update the status
            print(f"{os.getpid()}: Created a child pid: {kafka_process}, which is running kafka server")

            # Sleep for 5s
            time.sleep(5)

            # Wait for keyboard input to exit
            key = input("Press q to quit: ")
            while key != "q":
                key = input()

            # stop both servers
            stop_kafka_server()
            stop_zookeeper_server()

           
            # Kill all the processes created by this program
            for key in pgid:
                # don't kill the parent process
                if key == 'parent-process':
                    continue

                os.kill(pgid[key], SIGTERM)


            print("Exiting the program.")


if __name__ == "__main__":
    initiate_kafka()