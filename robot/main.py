from components import Cordinates, Battery, UploadData, Sensor, Robot
import threading


# create two robots in two threads and run them till the cows come home


def robot_one():
    cordinates = Cordinates(10, 10)
    battery = Battery(15 * 60, 5 * 60)
    upload = UploadData(1)
    sensor = {
        "temperature": Sensor("temperature", 10, 30),
        "humidity": Sensor("humidity", 0, 200)
    }
    
    robot = Robot("robot_1", cordinates, battery, sensor, upload)
    
    r = robot.move()
    
    
def robot_two():
    cordinates = Cordinates(20, 20)
    battery = Battery(20 * 60, 8 * 60)
    upload = UploadData(1)
    sensor = {
        "temperature": Sensor("temperature", 10, 30),
        "humidity": Sensor("humidity", 0, 200)
    }
    
    robot = Robot("robot_2", cordinates, battery, sensor, upload)
    
    r = robot.move()
    
    
def main():
    r1 = threading.Thread(target=robot_one)
    r1.start()
    
    
    r2 = threading.Thread(target=robot_two)
    r2.start()
    
    
if __name__ == "__main__":
    main()