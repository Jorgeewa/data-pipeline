import boto3
import json
from typing import Dict, List

class RobotConfiguration:
    
    def __init__(self, x, y):
        self.x = x
        self.y = y
        
def return_robot_config(robot_name: str):
    
    get_robot = {
        "robot_1" : RobotConfiguration(100, 5),
        "robot_2" : RobotConfiguration(100, 5),
    }
    
    return get_robot[robot_name]

class DynamoDb:
    
    def __init__(self):
        boto3.setup_default_session(profile_name='private')
        self.dynamodb = boto3.resource("dynamodb", region_name="eu-central-1")
        TABLE_NAME = "data-pipeline-front"
        self.table = self.dynamodb.Table(TABLE_NAME)
        
    def dynamo_db_upload(self, data):
        self.table.put_item(
            Item=data
        )
        
    def dynamo_db_fetch_item(self, item):
        response = self.table.get_item(
            Key={
                'robot_name': item,
            }
        )
        return response
    
    def check_if_exists(self, item):
        response = self.dynamo_db_fetch_item(item)
        return "Item" in response.keys()
    
    def fetch_time(self, item):
        item =  self.dynamo_db_fetch_item(item)
        return item["Item"]["round"]["last_time"]
    
    def update_latest_time(self, robot_name, last_time_val):

        return self.table.update_item(  
            Key={  
                "robot_name": robot_name  
            },  
            UpdateExpression="SET last_time = :last_time_val",   
            ExpressionAttributeValues={  
                ":last_time_val": last_time_val  
            }  
        )
    
    def update_graph(self, robot_name: str, x: int, y: int):
        return self.table.update_item(  
            Key={  
                "robot_name": robot_name  
            },  
            UpdateExpression="SET round.graph.x = list_append(round.graph.x, :x), round.graph.y = list_append(round.graph.y, :y)",  
            ExpressionAttributeValues={  
                ":x": [x],
                ":y": [y]  
            }  
        )
    
    def update_map(self, robot_name: str, key: str, data: Dict[str, List[List]]):
        return self.table.update_item(  
            Key={  
                "robot_name": robot_name  
            },  
            UpdateExpression="SET round.#map.#z = :data",  
            ExpressionAttributeNames={
                "#map" : "map",
                "#z" : key
            },
            ExpressionAttributeValues={  
                ":data": data  
            }  
        )

    
d = DynamoDb()

res = d.dynamo_db_fetch_item("test")
#res = d.update_map("test", "2022:03:03", [[1,3,4],[5,6,7]])
print(res)
    

    
    
    #insert
    
    
    #select
    
    
    #update


def main():
    # Create SQS client
    sqs = boto3.client('sqs')


    queue_url = "https://sqs.eu-central-1.amazonaws.com/759163837233/events",

    # Long poll for message on provided SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        WaitTimeSeconds=20
    )


'''
example_data = {
    "robot_1": {
        "last_time": "1970-01-01",
        "round": {
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
            
    }
}'''