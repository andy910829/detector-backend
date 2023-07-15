import csv
from pymongo import MongoClient
import uuid
import os
from datetime import datetime, timedelta

class handleExistData:
    def __init__(self):
        self.cluster = MongoClient("mongodb://localhost:27017")
        self.db = self.cluster["detector_data"]
        self.collection = self.db["board"]
        self.data_dict = {
            "static_val": list(),
            "dis_static": list(),
            "dynamic_val": list(),
            "dis_move": list(),
            "speed": list()
        }
        self.datatype_dict = {
            4: "static_val",
            5: "dis_static",
            6: "dynamic_val",
            7: "dis_move",
            8: "speed"
        }
        self.limit_dict = {
            4: int("fa", 16),
            5: int("6", 16),
            6: int("fa", 16),
            7: int("8", 16),
            8: int("14", 16)
        }
        self.id=str()
        self.csv_file = str()
        self.folder_path = "/var/www/detectorData/"
        self.cnt = 0
        self.time_dict = dict()

    def execute(self, data, id, dataLen):
        self.cnt+=1
        self.id = id 
        dir_path = f"{self.folder_path+str(self.id)}"
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        board_info = self.collection.find_one({"board_id": self.id})
        if board_info:
            self.csv_file = board_info["current_csv_file"]
        else:
            file_name = uuid.uuid4()
            self.collection.insert_one({"board_id": self.id, "current_csv_file": self.folder_path+str(self.id)+"/"+str(file_name)+".csv", "CSV_list":[self.folder_path+str(self.id)+"/"+str(file_name)+".csv"]})    
            self.csv_file = self.folder_path+str(self.id)+"/"+str(file_name)+".csv"
        try:
            if data[1] == "7":
                pass
            else:
                for index in range(4, 9):
                    if int(data[index], 16) > self.limit_dict[index]:
                        pass
                    else:
                        self.data_dict[self.datatype_dict[index]].append(
                            int(data[index], 16))
        except Exception as e:
            pass
        finally:
            if self.cnt == dataLen:
                self.write_csv()
                self.cleanList()

    def write_csv(self):
        self.swap()
        with open(self.csv_file, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            cnt = 0
            for data in self.data_dict.values():
                csv_writer.writerow(data)
                csv_writer.writerow(self.time_dict[cnt])
                cnt += 1

    def swap(self):
        try:
            # print("try")
            self.read_csv()
        except FileNotFoundError:
            # print("except FileNotFoundError")
            f = open(self.csv_file, 'w', newline='')
            f.close()
            self.read_csv()
        except IndexError:
            # print("except IndexError")
            with open(self.csv_file, 'r', newline='') as csv_file:
                rows = csv.reader(csv_file)
                cnt = 0
                current_time_cnt = 0
                for row in rows:
                    if row[0] in self.data_dict.keys():
                        newdata = self.data_dict[str(row[0])]
                        current_time_cnt = len(newdata)
                        self.data_dict[str(row[0])] = row+newdata
                    elif row[0] == "time":
                        for _ in range(current_time_cnt):
                            time = datetime.now()+timedelta(hours=8)
                            row.append(time.strftime('%m/%d %H:%M'))
                        self.time_dict[cnt] = row
                        cnt += 1

    def read_csv(self):
        change_file = False
        with open(self.csv_file, 'r', newline='') as csv_file:
            rows = csv.reader(csv_file)
            rows_len = len(list(rows))
            if rows_len == 0:
                self.csv_init()
            csv_file.seek(0)
            cnt = 0
            current_time_cnt = 0
            for row in rows:
                if len(row)>500:
                    change_file = True
                if row[0] in self.data_dict.keys():
                    newdata = self.data_dict[row[0]]
                    current_time_cnt = len(newdata)
                    self.data_dict[str(row[0])] = row+newdata
                elif row[0] == "time":
                    for _ in range(current_time_cnt):
                        time = datetime.now()+timedelta(hours=8)
                        row.append(time.strftime('%m/%d %H:%M'))
                    self.time_dict[cnt] = row
                    cnt += 1
        if change_file:
            self.create_new_CSVfile()

    def cleanList(self):
        for data_list in self.data_dict.values():
            data_list.clear()

    def create_new_CSVfile(self):
        file_name = self.folder_path+str(self.id)+"/"+str(uuid.uuid4())+".csv"
        self.collection.update_many({"board_id": self.id}, {"$set": {"current_csv_file": file_name},"$push": {"CSV_list": file_name}})

    def csv_init(self):
        with open(self.csv_file, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["speed"])
            csv_writer.writerow(["time"])
            csv_writer.writerow(["dis_static"])
            csv_writer.writerow(["time"])
            csv_writer.writerow(["static_val"])
            csv_writer.writerow(["time"])
            csv_writer.writerow(["dynamic_val"])
            csv_writer.writerow(["time"])
            csv_writer.writerow(["dis_move"])
            csv_writer.writerow(["time"])


# if __name__ == "__main__":
#     handleData = handleData()
#     data = [
#         ["0", "0", "85", "0", "5", "10", "15", "20"],
#         ["0", "0", "85", "03", "6", "11", "16", "21"],
#         ["0", "0", "85", "2", "7", "12", "17", "22"],
#         ["0", "0", "85", "2", "7", "12", "17", "22"],
#         ["0", "0", "85", "3", "8", "13", "18", "23"],
#         ["0", "0", "85", "3", "8", "13", "18", "23"],
#     ]
#     for test_data in data:
#         handleData.execute(test_data)
