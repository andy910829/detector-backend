import csv
from pymongo import MongoClient
import uuid
import os
from datetime import datetime, timedelta
import time


class handleBreathHeartData:
    def __init__(self):
        self.cluster = MongoClient("mongodb://localhost:27017")
        self.db = self.cluster["detector_data"]
        self.collection = self.db["board"]
        self.data_dict = {
            "heart_rate": list(),
            "breath_rate": list(),
            "bodysign_val": list(),
            "distance": list()
        }
        self.time_dict = dict()
        self.datatype_dict = {
            "85": "heart_rate",
            "81": "breath_rate",
            "80": {
                "3": "bodysign_val",
                "4": "distance"
            }
        }
        self.id = str()
        self.csv_file = str()
        self.folder_path = "/var/www/detectorData/"
        self.cnt = 0

    def execute(self, data, id, dataLen):
        self.cnt += 1
        self.id = id
        dir_path = f"{self.folder_path+str(self.id)}"
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        board_info = self.collection.find_one({"board_id": self.id})
        if board_info:
            self.csv_file = board_info["current_csv_file"]
        else:
            file_name = uuid.uuid4()
            self.collection.insert_one({"board_id": self.id, "current_csv_file": self.folder_path+str(self.id)+"/"+str(
                file_name)+".csv", "CSV_list": [self.folder_path+str(self.id)+"/"+str(file_name)+".csv"]})
            self.csv_file = self.folder_path + \
                str(self.id)+"/"+str(file_name)+".csv"
        data_type = data[0]
        try:
            if data_type == "80":
                value = int(data[4], 16)
                if data[1] == "4":
                    value = value * 256 + int(data[5], 16)
                self.data_dict[self.datatype_dict["80"][data[1]]].append(value)
            else:
                self.data_dict[self.datatype_dict[data_type]].append(
                    int(data[4], 16))
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
            self.read_csv()
        except FileNotFoundError:
            with open(self.csv_file, 'w', newline='') as csv_file:
                pass
            self.read_csv()
        except IndexError:
            with open(self.csv_file, 'r', newline='') as csv_file:
                rows = csv.reader(csv_file)
                cnt = 0
                current_time_cnt = 0
                for row in rows:
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
                if len(row) > 500:
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
        self.collection.update_many({"board_id": self.id}, {
                                    "$set": {"current_csv_file": file_name}, "$push": {"CSV_list": file_name}})

    def csv_init(self):
        with open(self.csv_file, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["heart_rate"])
            csv_writer.writerow(["time"])
            csv_writer.writerow(["breath_rate"])
            csv_writer.writerow(["time"])
            csv_writer.writerow(["bodysign_val"])
            csv_writer.writerow(["time"])
            csv_writer.writerow(["distance"])
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
