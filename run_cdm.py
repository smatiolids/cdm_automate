import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import pytz
import subprocess
import logging
import time
from logging.handlers import RotatingFileHandler

load_dotenv(override=True)

# Configure logging
def setup_logging():
    # Create logs directory if it doesn't exist
    os.makedirs('cdm_automate_logs', exist_ok=True)
    
    # Get run ID from environment or use timestamp
    run_id = os.getenv('RUN_ID', datetime.now().strftime('%Y%m%d%H%M%S'))
    
    # Configure logging format
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Create file handler
    log_file = f'cdm_automate_logs/cdm_{run_id}.log'
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logging.info(f"=== CDM Run Started at {datetime.now()} ===")
    logging.info(f"Logging to file: {log_file}")
    
    return logger

PARAMETERS = {
    'start_token': -9223372036854775808,
    'end_token': 9223372036854775807,
    'token_increment': 1844674407370955,
    'last_end_token': -9223372036854775808,
    'num_partitions': 5000,
    'read_rate_limit': 40000,
    'write_rate_limit': 120000
}

CDM_COMMAND = """./spark-3.5.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --properties-file cdm-spark.properties \
  --conf spark.executor.extraJavaOptions='-Dlog4j.configurationFile=log4j2.properties' \
  --conf spark.driver.extraJavaOptions='-Dlog4j.configurationFile=log4j2.properties' \
  --driver-memory 25G \
  --executor-memory 25G \
  --num-executors 32 \
  --conf spark.cdm.filter.cassandra.partition.min='<start_token>' \
  --conf spark.cdm.filter.cassandra.partition.max='<end_token>' \
  --conf spark.cdm.perfops.numParts='<num_partitions>' \
  --conf spark.cdm.perfops.ratelimit.origin='<read_rate_limit>' \
  --conf spark.cdm.perfops.ratelimit.target='<write_rate_limit>' \
  --conf spark.cdm.perfops.consistency.read='LOCAL_ONE' \
  --master "local[*]" \
  --class com.datastax.cdm.job.Migrate cassandra-data-migrator-5.2.3-SNAPSHOT_getrak.jar"""


def connect_to_astra():
    # Get environment variables
    secure_bundle_path = os.getenv('ASTRA_DB_SECURE_BUNDLE_PATH')
    client_id = os.getenv('ASTRA_DB_CLIENT_ID')
    client_secret = os.getenv('ASTRA_DB_CLIENT_SECRET')
    keyspace = os.getenv('ASTRA_DB_KEYSPACE')

    # Validate environment variables
    if not all([secure_bundle_path, client_id, client_secret, keyspace]):
        raise ValueError(
            "Missing required environment variables. Please ensure the following are set:\n"
            "- ASTRA_DB_SECURE_BUNDLE_PATH\n"
            "- ASTRA_DB_CLIENT_ID\n"
            "- ASTRA_DB_CLIENT_SECRET\n"
            "- ASTRA_DB_KEYSPACE"
        )

    # Ensure secure connect bundle exists
    if not Path(secure_bundle_path).exists():
        raise FileNotFoundError(f"Secure connect bundle not found at: {secure_bundle_path}")

    # Suppress Cassandra driver warnings
    logging.getLogger('cassandra').setLevel(logging.ERROR)
    logging.getLogger('cassandra.connection').setLevel(logging.ERROR)
    logging.getLogger('cassandra.protocol').setLevel(logging.ERROR)

    # Create auth provider
    auth_provider = PlainTextAuthProvider(client_id, client_secret)

    # Connect to the cluster
    cluster = Cluster(
        cloud={
            'secure_connect_bundle': secure_bundle_path
        },
        auth_provider=auth_provider
    )

    # Create session
    session = cluster.connect(keyspace)
    logging.info(f"Successfully connected to keyspace: {keyspace}")
    return session, cluster

def update_parameters(session):
    rows = session.execute("SELECT parameter_id, parameter_value FROM cdm_run_parameters")
    for row in rows:
        param_id = row.parameter_id
        param_value = row.parameter_value
        PARAMETERS[param_id] = int(param_value)    
    logging.info("Updated parameters:")
    # for key, value in PARAMETERS.items():
    #     print(f"{key}: {value}")  

def run_next_interval_token():
    # for each interval get the current parameters
    run_id = os.getenv('RUN_ID')
    session, cluster = connect_to_astra()
    logging.info("Connected to Astra DB")
    logging.info("Getting parameters from Astra DB")
    update_parameters(session)

    if PARAMETERS['last_end_token'] > PARAMETERS['end_token']:
        logging.info("#"*50)
        logging.info("Start token is greater than the end token")
        logging.info("FINISHED")
        logging.info("#"*50)
        return False
    
    # Get current hour in UTC-03:00 timezone
    brazil_tz = pytz.timezone('America/Sao_Paulo')  # This timezone uses UTC-03:00
    current_time = datetime.now(brazil_tz)
    current_round_hour = f"{current_time.hour:02d}00"
    read_rate_limit = PARAMETERS.get(f"read_rate_limit_{current_round_hour}",40000)
    logging.info(f"Current round hour: {current_round_hour} - Using read rate limit: {read_rate_limit}")
    
    end_token = PARAMETERS['last_end_token'] + PARAMETERS['token_increment']
    
    cdm_command = CDM_COMMAND.replace('<start_token>', str(PARAMETERS['last_end_token']))
    cdm_command = cdm_command.replace('<end_token>', str(end_token))
    cdm_command = cdm_command.replace('<num_partitions>', str(PARAMETERS['num_partitions']))
    cdm_command = cdm_command.replace('<write_rate_limit>', str(PARAMETERS['write_rate_limit']))
    cdm_command = cdm_command.replace('<read_rate_limit>', str(read_rate_limit))
    
    logging.info(f"CDM command: {cdm_command}")
    
    status = "RUNNING"
    start_time = datetime.now()
    try:
        logging.info(f"Inserting cdm_run_interval for run_id: {run_id}, start_time: {start_time}, last_end_token: {PARAMETERS['last_end_token']}, end_token: {end_token}, token_increment: {PARAMETERS['token_increment']}, read_rate_limit: {read_rate_limit}, write_rate_limit: {PARAMETERS['write_rate_limit']}, status: {status}")
        session.execute("""INSERT INTO cdm_run_interval (run_id, start_time, 
                        start_token, end_token, token_increment, 
                        read_rate_limit, write_rate_limit, status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    [run_id, start_time, 
                     PARAMETERS['last_end_token'], end_token, PARAMETERS['token_increment'], 
                     read_rate_limit, PARAMETERS['write_rate_limit'], status])
    except Exception as e:
        logging.error(f"Error inserting cdm_run_interval: {e}")
        return False
    
    # Close the session
    session.shutdown()
    cluster.shutdown()
    
    if os.getenv('DRY_RUN') != '1':
        logging.info("Running CDM Job")
        logging.info(f"Start time: {datetime.now()}")
        log_filename = f"cdm_automate_logs/cdm_job_{run_id}_{PARAMETERS['last_end_token']}_{end_token}.log"
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(log_filename), exist_ok=True)
        
        log_file = None
        try:
            log_file = open(log_filename, 'w')
            process = subprocess.Popen(cdm_command, shell=True, stdout=log_file, stderr=subprocess.STDOUT)
            start = time.time()
            logging.info(f"Process started at {start} - PID: {process.pid}")
            while process.poll() is None:
                logging.info(f"Process running for {time.time() - start} seconds")
                if time.time() - start > 3600:  # 1 hour in seconds
                    logging.warning("Process took more than 1 hour, killing and restarting...")
                    process.kill()
                    process.wait()
                    # Restart the process
                    process = subprocess.Popen(cdm_command, shell=True, stdout=log_file, stderr=subprocess.STDOUT)
                    logging.info(f"Process restarted at {time.time()} - New PID: {process.pid}")
                    start = time.time()
                time.sleep(30)  # Check every 30 seconds
            process.wait()  # Wait for the final process to complete
            logging.info(f"Process completed at {time.time()} - PID: {process.pid}")
            status = "SUCCESS"
        except Exception as e:
            status = "FAILED"
            logging.error(f"Error running CDM Job: {e}")
        finally:
            if log_file:
                log_file.flush()  # Ensure all data is written
                log_file.close()  # Close the file
        logging.info(f"End time: {datetime.now()}")
    else:
        status = "SUCCESS"
        logging.info("DRY RUN Mode")
    
    # Reconnect to Astra DB
    session, cluster = connect_to_astra()
    if status == "SUCCESS":
        try:
            elapsed_time = int((datetime.now() - start_time).total_seconds())
            logging.info(f"Updating cdm_run_interval for run_id: {run_id}, start_time: {start_time}, end_time: {datetime.now()}, status: {status}, elapsed_time: {elapsed_time}")
            session.execute("""UPDATE cdm_run_interval SET end_time = %s, 
                            status = %s, elapsed_time = %s WHERE run_id = %s and start_time = %s""",
                            [datetime.now(), status, elapsed_time, run_id, start_time])
            estimate_time_to_finish(PARAMETERS['start_token'], PARAMETERS['end_token'], PARAMETERS['last_end_token'], end_token, elapsed_time)
        except Exception as e:
            logging.error(f"Error updating cdm_run_interval: {e}")
            return False
        
        # update the last_end_token
        try:
            session.execute("""UPDATE cdm_run_parameters 
                            SET parameter_value = %s 
                            WHERE parameter_id = 'last_end_token'""",
                            [str(end_token)])
        except Exception as e:
            logging.error(f"Error updating cdm_run_parameters: {e}")
            return False
    else:
        try:
            elapsed_time = int((datetime.now() - start_time).total_seconds())
            session.execute("""UPDATE cdm_run_interval SET status = %s, elapsed_time = %s WHERE run_id = %s and start_time = %s""",
                            [status, elapsed_time, run_id, start_time])
        except Exception as e:
            logging.error(f"Error updating cdm_run_interval: {e}")
            return False
    
    session.shutdown()
    cluster.shutdown()
    return True

def estimate_time_to_finish(start_token, end_token, range_start_token, range_end_token, elapsed_time):
    complete_loaded_percentage = ((range_end_token - start_token) / (end_token - start_token)) * 100
    logging.info(f"Loaded {complete_loaded_percentage}% of the token range")
    
    loaded_percentage = ((range_end_token - range_start_token) / (end_token - start_token)) * 100
    not_loaded_percentage = 100 - loaded_percentage
    estimated_total_time_seconds = (elapsed_time / loaded_percentage) * 100
    # Calculate the remaining time in seconds
    remaining_time_seconds = estimated_total_time_seconds - elapsed_time
    # Convert remaining time to hours
    remaining_time_hours = remaining_time_seconds / 3600
    logging.info(f"Last run loaded  {loaded_percentage}% of the token range")
    logging.info(f"Estimated hours to complete the not loaded percentage: {remaining_time_hours:.2f} hours")

if __name__ == "__main__":
    try:
        # Setup logging first
        logger = setup_logging()
        
        while True:
            if not run_next_interval_token():
                break
                
        logging.info(f"=== CDM Run Completed at {datetime.now()} ===")
    except Exception as e:
        logging.error(f"Error connecting to Astra DB: {str(e)}")
