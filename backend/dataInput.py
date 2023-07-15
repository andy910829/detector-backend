import handleBreathHeartData
import handleExistData

class dataInput:
    def __init__(self,data,id):
        self.breath_serial_data=None
        self.exist_serial_data=None
        self.data = data
        self.id = id


    def execute(self):
        datalen = len(self.data)
        if self.data[0][0] == "8":
            self.exist_serial_data = handleExistData.handleExistData()
            for data in self.data:
                self.exist_serial_data.execute(data,self.id,datalen)
        elif self.data[0][0] == "80" or self.data[0][0] == "81" or self.data[0][0] == "85":
            self.breath_serial_data = handleBreathHeartData.handleBreathHeartData()
            for data in self.data:
                self.breath_serial_data.execute(data,self.id,datalen)
        else:
            pass

# serial_data.write_csv()
# ser.close()
if __name__ == "__main__":
    dataInput([["80","3","0","1","10","40","54","43","0","1","0","0","0","0","0","0","0","0","0","0"],["80","5","0","4","0","8d","0","5a","1c","54","0","0","0","0","0","0","0","0","0","0"],["80","5","0","4","0","8d","0","5a","1c","54","0","0","0","0","0","0","0","0","0","0"]],"labby").execute()
