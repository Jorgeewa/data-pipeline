import boto3
import json, os
import pandas as pd
from io import StringIO
import datetime
import traceback
import time as timer
from dotenv import load_dotenv
load_dotenv(".env")


class Transform:
    
    def __init__(self):
        self.profile = os.getenv("profile")
        self.region = os.getenv("region")
        boto3.setup_default_session(profile_name=self.profile)
        self.s3 = boto3.client('s3')
        self.sqs = boto3.client('sqs')
        self.glue_client = boto3.client("glue", self.region)
        self.event_uri = os.getenv("event_queue_uri")
        self.job_name = os.getenv("glue_job_name")
        
        
    def download_from_s3(self, bucket: str, key: str):
        return self.s3.get_object(
            Bucket=bucket,
            Key=key
        )
        
    def put_in_s3(self, data, bucket, key):
        self.s3.put_object(Body=data, Bucket=bucket, Key=key)
        
    def parse(self, response):
        data = response["Body"].read().decode('utf-8').replace("}{","}\n{")
        data = StringIO(data)
        
        df = pd.read_csv(data, names=list('m'), sep='\n')
        df = df['m'].apply(lambda x: pd.Series(json.loads(x)))
        
        df = df.astype({'x': int, 'y': int})
        
        humidity = df[['x', 'y', 'timestamp', 'humidity', 'robot_name']]
        humidity = humidity.rename(columns = {"humidity": "value"})
        humidity["observable_name"] = "humidity"
        temperature = df[['x', 'y', 'timestamp', 'temperature', 'robot_name']]
        temperature = temperature.rename(columns={"temperature": "value"})
        temperature["observable_name"] = "temperature"
        return humidity, temperature
    
    def check_and_send_events(self, df: pd.DataFrame) -> None:
        row, _ = df.shape
        # round completed?
        x = df['x'] == -1
        if x.any():
            print("************************ Round found ***************************")
            self.run_job()
            robot_name = df.loc[row-1, 'robot_name']
            observable_name = df.loc[row-1, 'observable_name']
            time = df.loc[row-1, 'timestamp']
            event = "new_round"
            self.send_message(robot_name, observable_name, event, time)

    def send_message(self, robot_name, observable_name, event, time):

        message = {
            "robot_name": robot_name,
            "observable_name": observable_name,
            "event": event,
            "time": time
        }
        #job takes about a minute
        timer.sleep(60 * 2)
        print(f"About to upload message {message}")
        response = self.sqs.send_message(
            QueueUrl=self.event_uri,
            MessageBody=json.dumps(message)
        )
        
        
    def run_job(self):
        
        job_name = self.job_name
        try:
            job_run_id = self.glue_client.start_job_run(JobName=job_name)
        except:
            error = traceback.format_exc()
            print(f"The job run error is {error}")


def main():
    # Create SQS client
    profile = os.getenv("profile")
    boto3.setup_default_session(profile_name=profile)
    sqs = boto3.client('sqs')
    queue_url = os.getenv("transform_queue_uri")
    transform = Transform()

    # Long poll for message on provided SQS queue
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
            WaitTimeSeconds=20
        )
        try:
            if "Messages" in response:
                for message in response['Messages']:
                
                    response = json.loads(message['Body'])
                    receipt = message['ReceiptHandle']
                    bucket = response['Records'][0]['s3']['bucket']['name']
                    key = response['Records'][0]['s3']['object']['key']
                    response = transform.download_from_s3(bucket, key)
                    humidity, temperature = transform.parse(response)
                    
                    bucket = 'robot-sensor-data-transformed'
                    date_now = datetime.datetime.now()
                    key = f'humidity/{date_now.date()}/{date_now}.csv'
                    humidity_csv = humidity.to_csv(index=False)
                    transform.put_in_s3(humidity_csv, bucket, key)
                    
                    key = f'temperature/{date_now.date()}/{date_now}.csv'
                    temperature_csv = temperature.to_csv(index=False)
                    transform.put_in_s3(temperature_csv, bucket, key)
                    
                    transform.check_and_send_events(humidity)
                    transform.check_and_send_events(temperature)
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        except:
            error = traceback.format_exc()
            print(f"Failed because of {error}")
            continue
        
if __name__ == "__main__":
    main()
                
    
    
    




    
    
    
    