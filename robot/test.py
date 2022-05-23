from components import Cordinates, Battery, UploadData, Sensor, Robot
import datetime



def test_cordinates():
    
    cordinates = Cordinates(10, 150)
    
    print(cordinates.generate_cordinate_x(), cordinates.generate_cordinate_y())
    assert 0 < cordinates.generate_cordinate_x() <= 10
    assert 0 < cordinates.generate_cordinate_x() <= 150
    
    
def test_battery():
    battery = Battery(10, 5)
    def small_fun():
        return 200
    assert battery.charge(small_fun) == 200
    
    
def test_upload_data():
    upload = UploadData(1)
    data = {
        'x': 0.5,
        'y': 4.5,
        'timestamp': datetime.datetime.now(),
        'robot_name': "George"
    } 
    upload.upload(data)  
    
    
def test_sensor():
    sensor = Sensor("temperature", 10, 30)
    print(sensor.generate_data())
    assert 10 < sensor.generate_data() <= 30
    
    
def test_robot():
    cordinates = Cordinates(10, 150)
    battery = Battery(10, 5)
    upload = UploadData(1)
    sensor = {
        "temperature": Sensor("temperature", 10, 30),
        "humidity": Sensor("humidity", 0, 200)
    }
    
    robot = Robot("my_robot", cordinates, battery, sensor, upload)
    
    r = robot.move()

test_robot()
