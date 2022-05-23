import time, threading

'''
    simulate javascript set interval. gotten from: https://stackoverflow.com/questions/2697039/python-equivalent-of-setinterval
'''


class SetInterval :
    def __init__(self, interval: int, action, generate_params) :
        self.interval = interval
        self.action = action
        self.generate_params = generate_params
        self.stopEvent = threading.Event()
        thread=threading.Thread(target = self.__setInterval)
        thread.start()

    def __setInterval(self) :
        nextTime=time.time()+self.interval
        while not self.stopEvent.wait(nextTime-time.time()) :
            nextTime+=self.interval
            self.action(self.generate_params())

    def cancel(self) :
        self.stopEvent.set()