import boto3
import json, os
from typing import Dict, List, Any
import traceback
import pandas as pd
import numpy as np
import datetime
import time as timer
from decimal import Decimal
from dotenv import load_dotenv
load_dotenv(".env")

class RobotConfiguration:
    
    def __init__(self, x, y):
        self.x = x + 1
        self.y = y + 1
        
def return_robot_config(robot_name: str):
    
    get_robot = {
        "robot_1" : RobotConfiguration(10, 10),
        "robot_2" : RobotConfiguration(20, 20),
    }
    
    return get_robot[robot_name]

class DynamoDb:
    
    def __init__(self, robot_name, observable_name):
        self.profile = os.getenv("profile")
        self.region = os.getenv("region")
        boto3.setup_default_session(profile_name=self.profile)
        self.dynamodb = boto3.resource("dynamodb", region_name=self.region)
        self.dynamo_table = os.getenv("dynamo_table")
        self.table = self.dynamodb.Table(self.dynamo_table)
        self.observable_name = observable_name
        self.robot_name = robot_name
        
    def upload(self, data: Dict):
        data = json.loads(json.dumps(data).replace('NaN', 'null'), parse_float=Decimal)
        self.table.put_item(
            Item=data
        )
        
    def dynamo_db_fetch_item(self, item: str) -> Dict[str, Any]:
        response = self.table.get_item(
            Key={
                'robot_name': item,
            }
        )
        return response
    
    def check_if_exists(self) -> bool:
        response = self.dynamo_db_fetch_item(self.robot_name)
        return "Item" in response.keys()
    
    def fetch_time(self) -> str:
        item =  self.dynamo_db_fetch_item(self.robot_name)
        if "Item" in item:
            if self.observable_name in item["Item"]["round"]:
                return item["Item"]["round"][self.observable_name]["last_time"]
            else:
                # Doing too much here but a work around an dynamodb insertion issue
                self.add()
                return "1970-01-01"
        else:
            return "1970-01-01"
    
    def update_latest_time(self, last_time_val: str) -> None:

        return self.table.update_item(  
            Key={  
                "robot_name": self.robot_name  
            },  
            UpdateExpression="SET round.#observable_name.last_time = :last_time_val", 
            ExpressionAttributeNames={
                "#observable_name" : self.observable_name
            },  
            ExpressionAttributeValues={  
                ":last_time_val": last_time_val  
            }  
        )
    
    def add(self) -> None:
        round = {}
        item = {
            "last_time": None,
            "graph": {
                "x": [],
                "y": []
            },
            "map": {
                "z": {}
            }
        }
        round[self.observable_name] = item
        
        return self.table.update_item(  
            Key={  
                "robot_name": self.robot_name  
            },  
            UpdateExpression="SET round.#observable_name = :round",
            ExpressionAttributeNames={
                "#observable_name" : self.observable_name
            },
            ExpressionAttributeValues={  
                ":round": item,                     
            }  
        )    
    
    def update_graph(self, x: str, y: float) -> None:
        return self.table.update_item(  
            Key={  
                "robot_name": self.robot_name  
            },  
            UpdateExpression="SET round.#observable_name.graph.x = list_append(round.#observable_name.graph.x, :x), round.#observable_name.graph.y = list_append(round.#observable_name.graph.y, :y)",  
            ExpressionAttributeNames={
                "#observable_name" : self.observable_name
            },
            ExpressionAttributeValues={  
                ":x": [x],
                ":y": [Decimal(y)]  
            }  
        )
    
    def update_map(self,  key: str, data: Dict[str, List[List]]) -> None:
        data = json.loads(json.dumps(data).replace('NaN', 'null'), parse_float=Decimal)
        return self.table.update_item(  
            Key={  
                "robot_name": self.robot_name  
            },  
            UpdateExpression="SET round.#observable_name.#map.z.#key = :data",  
            ExpressionAttributeNames={
                "#observable_name" : self.observable_name,
                "#map" : "map",
                "#key" : key
            },
            ExpressionAttributeValues={  
                ":data": data  
            }  
        )


class RedShift():
    
    
    def __init__(self):
        self.profile = os.getenv("profile")
        boto3.setup_default_session(profile_name=self.profile)
        self.redshift_client = boto3.client('redshift-data')
        self.secret = os.getenv("redshift_secret")
        self.clusterId = os.getenv("redshift_cluster")
        self.db = os.getenv("redshift_db")
        
        
    def fetch_map_data(self, robot_name, observable_name, last_time, time):
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.clusterId,
            Database=self.db,
            SecretArn=self.secret,
            Sql=f"select x, y, avg(value) as value from data where observable_name='{observable_name}' and robot_name='{robot_name}' and timestamp>'{last_time}' and timestamp<='{time}' and x != -1 group by x, y order by x, y"
        )
        if "Id" in response:
            id = response['Id']
            
            while True:
                response = self.redshift_client.describe_statement(Id=id)
                if response["Status"] == "FINISHED":
                    print(f'Query status - {response["Status"]}')
                    break
                else:
                    print(f'Query status - {response["Status"]}')
                    timer.sleep(2)
            response = self.redshift_client.get_statement_result(
                Id=id
            )
            return response["Records"]
    
    def fetch_graph_data(self, robot_name, observable_name, last_time, time):
        response = self.redshift_client.execute_statement(
            ClusterIdentifier=self.clusterId,
            Database=self.db,
            SecretArn=self.secret,
            Sql=f"select avg(value) as value from data where observable_name='{observable_name}'and robot_name='{robot_name}' and timestamp>'{last_time}' and timestamp<='{time}' and x != -1",
        )
        print("This is the redshift query")
        print(f"select avg(value) as value from data where observable_name='{observable_name}'and robot_name='{robot_name}' and timestamp>'{last_time}' and timestamp<='{time}' and x != -1")
        if "Id" in response:
            id = response['Id']
            
            while True:
                response = self.redshift_client.describe_statement(Id=id)
                if response["Status"] == "FINISHED":
                    print(f'Query status - {response["Status"]}')
                    break
                else:
                    print(f'Query status - {response["Status"]}')
                    timer.sleep(2)
            response = self.redshift_client.get_statement_result(
                Id=id
            )
            return response["Records"]


class Graph():

    def __init__(self):
        pass
        
    def parse(self, data):
        value = data[0][0]["stringValue"]
        return value
    
    
class Heatmap():
    
    def __init__(self, x, y):
        self.x = x
        self.y = y
        
        
    def parse(self, data):
        # x, y, value
        x, y, value = [], [], []
        
        for values in data:
            x.append(values[0]['longValue'])
            y.append(values[1]['longValue'])
            value.append(values[2]['stringValue'])
        
        df = pd.DataFrame({"x": x, "y": y, "value": value})
        df = df.astype({"value": str})
        print("df should not be empty")
        print(df)

        x = df.loc[:, 'x'].values
        y = df.loc[:, 'y'].values
        v = df.loc[:, 'value'].values
        out = np.full([self.x, self.y], np.nan, dtype=np.float64)
        out[x, y] = v
        return out.tolist()


def default_data(robot_name, observable_name, time, graph, map):
    round = {}
    heatmap = {}
    
    heatmap[time] = map
    round[observable_name] = {
        "last_time": time,
        "graph": {
            "x": [time],
            "y": [graph]
        },
        "map": {
            "z": heatmap
        }
    }
    
    default = {
        "robot_name": robot_name,
        "round": round
    }
    
    return default

def main():
    # Create SQS client
    profile = os.getenv("profile")
    boto3.setup_default_session(profile_name="private")
    sqs = boto3.client('sqs')


    queue_url = os.getenv("event_queue_uri")
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
                    robot_name = response['robot_name']
                    observable_name = response['observable_name']
                    event = response['event']
                    time = str(datetime.datetime.strptime(response['time'], "%Y-%m-%dT%H:%M:%S.%f").strftime("%Y-%m-%d %H:%M:%S"))
                    robot = return_robot_config(robot_name)
                    redshift = RedShift()
                    dynamodb = DynamoDb(robot_name, observable_name)
                    
                    last_time = dynamodb.fetch_time()
                    print("this is the between time", last_time, time)
                    map = redshift.fetch_map_data(robot_name, observable_name, last_time, time)
                    graph = redshift.fetch_graph_data(robot_name, observable_name, last_time, time)
                    print("This is the map")
                    print(map)
                    if len(map) == 0:
                        print(f"Deleting message with id: {receipt}")
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                        continue
                    
                    h = Heatmap(robot.x, robot.y)
                    g = Graph()
                    
                    z = h.parse(map)
                    value = g.parse(graph)
                    
                    if dynamodb.check_if_exists():
                        dynamodb.update_latest_time(time)
                        dynamodb.update_graph(time, value)
                        dynamodb.update_map(time, z)
                    else:
                        data = default_data(robot_name, observable_name, time, value, z)
                        dynamodb.upload(data)
                    print(f"Deleting message with id: {receipt}")
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
        except:
            error = traceback.format_exc()
            print(f"Failed because of {error}")
            continue


if __name__ == "__main__":
    main()










'''
example_data = {
    "robot_1": {
        "round": {
            "last_time": "1970-01-01",
            "temperature":{
                "graph": {
                    "x": [1,2,3],
                    "y": [4,5,6]
                },
                "map": {
                    "z": {
                        "4": [[1,2,3], [4,5,6]]
                    }
                }
            },
            "humidity":{
                "graph": {
                    "x": [1,2,3],
                    "y": [4,5,6]
                },
                "map": {
                    "z": {
                        "4": [[1,2,3], [4,5,6]]
                    }
                }
            },

        },   
    }
}'''