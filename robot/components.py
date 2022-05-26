import numpy as np
import boto3
import json
from typing import Callable, Dict
import time, threading
import datetime
from interval import SetInterval



class Cordinates:
    
    def __init__(self, x, y):
        self.x = x
        self.y = y
        
    def generate_cordinate_x(self):
        return np.random.uniform(0, self.x)
    
    def generate_cordinate_y(self):
        return np.random.uniform(0, self.y)

class Battery:
    
    def __init__(self, run_time: int, charge_time: int):
        
        # in seconds
        self.run_time = run_time
        
        # in seconds
        self.charge_time = charge_time
        
        
    def charge(self, callback: Callable):
        '''
            Battery has to charge
        '''
        time.sleep(self.charge_time)
        return callback()
    

class UploadData:
    
    def __init__(self, speed: int):
        '''
            Should connect with aws sdk to upload data to cloud
        '''
        self.speed = speed
        
    def upload(self, data):
        boto3.setup_default_session(profile_name='private')
        client = boto3.client('firehose')
        client.put_record(
            DeliveryStreamName="robot-sensor-data",
            Record={
                'Data': json.dumps(data)
            },
        )
        print(f'{data} has been uploaded')


class Sensor:
    
    def __init__(self, name: str, lower_limit:int, upper_limit: int):
        self.name = name
        self.lower_limit = lower_limit
        self.upper_limit = upper_limit
        
        
    def generate_data(self):
        return np.random.uniform(self.lower_limit, self.upper_limit)
        

class Robot:
    
    def __init__(self, name: str, cordinates: Cordinates, battery: Battery, sensors: Dict[str, Sensor], upload: UploadData):
        self.cordinates = cordinates
        self.battery = battery
        self.sensors = sensors
        self.upload = upload
        self.name = name
    
    def move(self):
            
        interval = SetInterval(self.upload.speed, self.upload.upload, self.generate_params)
        t=threading.Timer(self.battery.run_time, interval.cancel)
        t.start()
        t.join()
        self.upload(self.generate_end_of_round_params())
        return self.charge()
        
    def generate_params(self):
        data_to_upload = {
            'x': self.cordinates.generate_cordinate_x(),
            'y': self.cordinates.generate_cordinate_y(),
            'timestamp': datetime.datetime.now().isoformat(),
            'robot_name': self.name
        }
        for name, sensor in self.sensors.items():
            data_to_upload[name] = sensor.generate_data()
        return data_to_upload
    
    def generate_end_of_round_params(self):
        data_to_upload = {
            'x': -1,
            'y': -1,
            'timestamp': datetime.datetime.now().isoformat(),
            'robot_name': self.name
        }
        for name, _ in self.sensors.items():
            data_to_upload[name] = -1
        return data_to_upload
    
    
    def charge(self):
        return self.battery.charge(self.move)
    
    