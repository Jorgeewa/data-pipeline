import boto3
import json
import pandas as pd
from io import StringIO
import datetime
import traceback
import time


class Transform:
    
    def __init__(self):
        boto3.setup_default_session(profile_name='private')
        self.region = "eu-central-1"
        self.s3 = boto3.client('s3')
        self.sqs = boto3.client('sqs')
        self.glue_client = boto3.client("glue", self.region)
        
        
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
        humidity = humidity.rename({'humidity': "value"})
        humidity["obsrvable_name"] = "humidity"
        temperature = df[['x', 'y', 'timestamp', 'temperature', 'robot_name']]
        temperature = temperature.rename({'temperature': "value"})
        temperature["observable_name"] = "temperature"
        return humidity, temperature
    
    def check_and_send_events(self, df: pd.DataFrame) -> None:
        row, _ = df.shape
        # round completed?
        x = df['x'] == -1
        if x.any():
            self.run_job()
            robot_name = df.loc[row-1, 'robot_name']
            time = df.loc[row-1, 'timestamp']
            event = "new_round"
            self.send_message(robot_name, event, time)

    def send_message(self, robot_name, event, time):

        message = {
            "robot_name": robot_name,
            "event": event,
            "time": time
        }
        
        #job takes about a minute
        time.sleep(60 * 2)
        response = self.sqs_client.send_message(
            QueueUrl="https://sqs.eu-central-1.amazonaws.com/759163837233/events",
            MessageBody=json.dumps(message)
        )
        
        
    def run_job(self):
        
        job_name = "s3-to-redshift"
        try:
            job_run_id = self.glue_client.start_job_run(JobName=job_name)
        except:
            error = traceback.format_exc()
            print(f"The job run error is {error}")


def main():
    # Create SQS client
    boto3.setup_default_session(profile_name='private')
    sqs = boto3.client('sqs')
    queue_url = 'https://sqs.eu-central-1.amazonaws.com/759163837233/transformation'
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
            for message in response['Messages']:
            
                response = json.loads(message['Body'])
                receipt = message['ReceiptHandle']
                bucket = response['Records'][0]['s3']['bucket']['name']
                key = response['Records'][0]['s3']['object']['key']
                response = transform.download_from_s3(bucket, key)
                humidity, temperature = transform.parse(response)
                
                
                bucket = 'robot-sensor-data-transformed'
                date_now = datetime.datetime.now()
                key = f'data/{date_now.date()}/{date_now}.csv'
                humidity = humidity.to_csv(index=False)
                transform.put_in_s3(humidity, bucket, key)
                
                key = f'temperature/{date_now.date()}/{date_now}.csv'
                temperature = temperature.to_csv(index=False)
                transform.put_in_s3(temperature, bucket, key)
                transform.check_and_send_events(temperature)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        except:
            error = traceback.format_exc()
            print(f"Failed because of {error}")
            continue
                
    
    
    




    
    
    
    