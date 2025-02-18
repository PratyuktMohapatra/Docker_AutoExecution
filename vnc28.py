import threading
import logging
import subprocess
import time
import os
import psutil
import paramiko
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import mysql.connector
import concurrent.futures
import paramiko
from datetime import datetime

# Configuration
DOCKER_COMPOSE_DIR = '/home/apmosys/Desktop/Docker_autoexecution'  # Docker Compose directory
LOCAL_FOLDER = '/home/apmosys/Desktop/Docker_autoexecution/Smaas_Jar'  # Local folder to copy inside the container
BATCH_FILE = '/home/apmosys/Desktop/Docker_autoexecution/batch.txt'  # Path to batch.txt
LOG_FILE_PATH = '/home/apmosys/Desktop/Docker_autoexecution/api_logs.log'
LOCAL_LOG_FOLDER = '/home/apmosys/Desktop/Docker_autoexecution/LOG'
HAR_DATA_FOLDER = '/home/apmosys/Desktop/Docker_autoexecution/Har_Data'
CHROME_DRIVER = '/home/apmosys/Desktop/Docker_autoexecution/Smaas_Jar/chromedriver'
BATCH_CONTAINER_FILE = '/home/apmosys/Desktop/Docker_autoexecution/batch_container.txt'
CLIENT_ID = 116
FREQUENCY = 5

# Database Configuration
DB_URL = "192.168.9.16"
DB_NAME = "nonsbi_final_15jan2025"
DB_USER = "root"
DB_PASSWORD = "Welcome@2023"
DB_QUERY = "SELECT MAX(batch_no) FROM execution_order"

# Static Host IP
HOST_IP = "192.168.14.46"

# Locks for thread safety
port_lock = threading.Lock()
file_lock = threading.Lock()
used_ports = set()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE_PATH),
        logging.StreamHandler()
    ]
)

# Function to execute Remmina on a remote system
def execute_remmina_remotely(vnc_port):
    remote_ip = "192.168.3.239"
    username = "pratyukt"
    password = "pro@2024"
    command = f"remmina -c vnc://192.168.14.46:{vnc_port}"

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=remote_ip, username=username, password=password)

        stdin, stdout, stderr = ssh.exec_command(command)
        output = stdout.read().decode()
        error = stderr.read().decode()

        logging.info("Remmina Command Output:")
        logging.info(output)

        if error:
            logging.error("Remmina Command Error:")
            logging.error(error)

        ssh.close()
    except Exception as e:
        logging.error(f"An error occurred while executing Remmina remotely: {e}")

def get_available_port(start_port=5901, end_port=65535):
    """Return a thread-safe available port in the specified range."""
    global used_ports
    with port_lock:
        used_ports.update(conn.laddr.port for conn in psutil.net_connections())
        for port in range(start_port, end_port):
            if port not in used_ports:
                used_ports.add(port)
                return port
    raise Exception("No available port in the specified range.")

def generate_unique_container_name():
    """Generate a unique container name."""
    return f"container-{random.randint(1000, 9999)}"

def fetch_batch_number_from_db():
    """Fetch the batch number from the database and update the batch file."""
    try:
        connection = mysql.connector.connect(
            host=DB_URL,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        if connection.is_connected():
            logging.info("Connected to the database.")
            cursor = connection.cursor()
            cursor.execute(DB_QUERY)
            result = cursor.fetchone()

            if result and result[0]:
                max_batch_no = result[0]
                batch_numbers = list(range(1, max_batch_no + 1))

                with open(BATCH_FILE, 'w') as file:
                    file.write(','.join(map(str, batch_numbers)))
                logging.info(f"Batch file updated with numbers: {batch_numbers}")
            else:
                logging.error("No batch numbers found in the database.")

    except mysql.connector.Error as e:
        logging.error(f"Error connecting to the database: {str(e)}")
    finally:
        if connection.is_connected():
            connection.close()
            logging.info("Database connection closed.")

def update_database(batch_no, container_ip):
    """Update the execution_order table with batch_no and container_ip."""
    try:
        connection = mysql.connector.connect(
            host=DB_URL,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if connection.is_connected():
            cursor = connection.cursor()
            update_query = """
                UPDATE execution_order
                SET container_ip = %s
                WHERE batch_no = %s
            """
            cursor.execute(update_query, (container_ip, batch_no))
            connection.commit()
            logging.info(f"Updated database for batch_no {batch_no} with container_ip {container_ip}")

    except mysql.connector.Error as e:
        logging.error(f"Database update failed for batch {batch_no}: {str(e)}")
    finally:
        if connection.is_connected():
            connection.close()
            logging.info("Database connection closed after update.")

def update_database2(batch_no, browser_container_ip):
    """Update the execution_order table with batch_no and the full URL for browser access."""
    try:
        connection = mysql.connector.connect(
            host=DB_URL,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        if connection.is_connected():
            cursor = connection.cursor()
            browser_url = f"http://{browser_container_ip}"  # Construct the full URL
            update_query = """
                UPDATE execution_order
                SET browser_container_ip = %s
                WHERE batch_no = %s
            """
            cursor.execute(update_query, (browser_url, batch_no))  # Save the full URL
            connection.commit()
            logging.info(f"Updated database for batch_no {batch_no} with URL {browser_url}")

    except mysql.connector.Error as e:
        logging.error(f"Database update failed for batch {batch_no}: {str(e)}")
    finally:
        if connection.is_connected():
            connection.close()
            logging.info("Database connection closed after update.")

def process_batch_file():
    """Read numbers from the batch.txt file."""
    try:
        with open(BATCH_FILE, 'r') as file:
            content = file.read().strip()
            numbers = content.split(',')
            return [num.strip() for num in numbers if num.strip().isdigit()]
    except FileNotFoundError:
        logging.error(f"Batch file not found: {BATCH_FILE}")
        return []

def process_container(number):
    """Process a single container for a given batch number."""
    try:
        container_name = generate_unique_container_name()
        os.chdir(DOCKER_COMPOSE_DIR)

        vnc_port = get_available_port(5901, 5950)
        web_port = get_available_port(6080, 65535)
        logging.info(f"Batch {number} | VNC: {vnc_port}, Web: {web_port}")

        env = os.environ.copy()
        env['VNC_PORT'] = str(vnc_port)
        env['WEB_PORT'] = str(web_port)
        env['CONTAINER_NAME'] = container_name

        subprocess.run(["docker-compose", "-f", "docker-compose.yml", "-p", container_name, "up", "-d"], env=env, check=True)
        time.sleep(15)

        # Write batchNumber and hostIP:port to batch_container.txt
        with file_lock:
            with open(BATCH_CONTAINER_FILE, 'a') as batch_file:
                batch_file.write(f"{number}-{HOST_IP}:{vnc_port}\n")
                logging.info(f"Stored batch {number} with VNC port {vnc_port} in {BATCH_CONTAINER_FILE}")

        # Open Remmina to connect to the VNC server using the dynamic VNC port
        execute_remmina_remotely(vnc_port)
        
        container_ip_port = f"{HOST_IP}:{vnc_port}"
        update_database(number, container_ip_port)  # Update database with batch_no and IP:vnc_Port

        browser_container_ip = f"{HOST_IP}:{web_port}"
        update_database2(number, browser_container_ip )  # Update database with batch_no and IP:web_Port

        subprocess.run(f"docker cp {LOCAL_FOLDER} {container_name}:/root/", shell=True, check=True)
        logging.info(f"Copied folder {LOCAL_FOLDER} into container {container_name}:/root/")

        subprocess.run(f"docker cp {CHROME_DRIVER} {container_name}:/root/", shell=True, check=True)
        logging.info(f"Copied folder {CHROME_DRIVER} into container {container_name}:/root/")

        java_home_path = "/usr/lib/jvm/jdk-19.0.2"
        current_timezone = "Asia/Kolkata"
        subprocess.run(
            f"docker exec {container_name} bash -c 'export JAVA_HOME={java_home_path} && export PATH=$JAVA_HOME/bin:$PATH && nohup java -Duser.timezone={current_timezone} -jar /root/Smaas_Jar/framework-api-0.0.1-SNAPSHOT.jar > /root/Logs/output.txt 2>&1 &'",
            shell=True, check=True
        )
        logging.info(f"Executed JAR file inside container: {container_name} in the background")

        # Wait for port 8082 to be in LISTEN state
        max_retries = 30
        for attempt in range(max_retries):
            result = subprocess.run(
                f"docker exec {container_name} netstat -tuln | grep 8082",
                shell=True,
                capture_output=True,
                text=True
            )
            if "LISTEN" in result.stdout:
                logging.info(f"Port 8082 is listening inside container {container_name}.")
                break
            time.sleep(4)
        else:
            logging.error(f"Port 8082 did not start listening after {max_retries * 2} seconds in container {container_name}.")
            return

        api_url = f"http://localhost:8082/automation/execute/{number}/{CLIENT_ID}/{FREQUENCY}"
        logging.info(f"This API_URL: {api_url} is hit on {container_name}")
        api_response = subprocess.run(
            f"docker exec {container_name} curl -X GET {api_url}",
            shell=True, check=True, capture_output=True
        )
        logging.info(f"API Response: {api_response.stdout.decode()}")

        if api_response.returncode == 0 and "Execution Completed" in api_response.stdout.decode():
            logging.info(f"API call successful for {number}.")

        # Save the Har_Data folder after execution
        result = subprocess.run(f"docker exec {container_name} test -d /root/Har_Data", shell=True)
        if result.returncode == 0:  # 'Har_Data' folder exists
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            har_folder_path = os.path.join(HAR_DATA_FOLDER, f"Har_Data_{timestamp}")

            # Check if the Har_Data folder exists on the server, create it if not
            if not os.path.exists(HAR_DATA_FOLDER):
                os.makedirs(HAR_DATA_FOLDER)
                logging.info(f"Created Har_Data folder at {HAR_DATA_FOLDER}")

            subprocess.run(f"docker cp {container_name}:/root/Har_Data {har_folder_path}", shell=True, check=True)
            logging.info(f"Saved HAR data folder to {har_folder_path}")
        else:
            logging.error(f"Har_Data folder does not exist in container {container_name}.")

        subprocess.run(["docker", "stop", container_name], check=True)
        subprocess.run(["docker", "rm", container_name], check=True)
        logging.info(f"Stopped and removed container: {container_name}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Command failed for {number}: {str(e)}")
    except Exception as e:
        logging.error(f"Error for {number}: {str(e)}")

def run_sequence():
    """Run the sequence for all numbers in the batch file in parallel."""
    fetch_batch_number_from_db()  # Update batch file with database numbers
    batch_numbers = process_batch_file()
    if not batch_numbers:
        logging.error("No valid numbers in batch file.")
        return

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_container, number): number for number in batch_numbers}
        for future in as_completed(futures):
            number = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error processing container for {number}: {str(e)}")

    subprocess.run(["docker", "network", "prune", "-f"], check=True)
    logging.info("Unused Docker networks pruned.")

if __name__ == '__main__':
    logging.info("Starting the process.")
    run_sequence()
