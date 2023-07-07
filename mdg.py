import threading
import random
import json
import time
import datetime
from flask import Response, Flask, request, session
import prometheus_client
from prometheus_client import Summary, Counter, Histogram, Gauge
import requests
from network_comm.sender import Mqtt_Sender
from network_comm.receiver import Mqtt_Receiver
import settings
import socket
from logging.handlers import SysLogHandler
import logging
import sys

FLASK_PORT = 10001


class MDG_MQTT():

    def __init__(self):
        self.flaskPort = FLASK_PORT
        self.enable_debug = False
        self.recv_jsonData = None
        self.payload_counter = 0
        self.FLAG_ENABLE_DATA_GEN = False  # Default State
        self.FLAG_DATA_GENERATION_CMPLT = True  # Default State
        self.start_time = 0
        self.final_data = []
        self.num_of_devices = 2
        self.settimeinterval = 1
        self.prev_num_of_data_vol = 0
        self.mdg_settings = {
            "polling_freq": 1000,
            "data_vol": 2
        }
        self.temp_data = []
        self.humid_data = []
        self.ir_data = []
        self.moisture_data = []
        self.pressure_data = []
        self.FLAG_ENABLE_PRESSURE = True
        self.deviceID = []
        self.maxLionxInMDG = 8
        self.tempRange = [92.0, 99.9]
        self.humidityRange = [50, 95]
        self.moistureRange = [20, 99.9]
        self.infraredRange = [0, 2]
        self.pressureRange = [20, 99.9]
        self.begin_time = 10
        self.mdgOffset = 100

        args = sys.argv
        if(len(args) > 1):
            self.flaskPort = args[1]
            self.MDG_Num = args[2]

        self.createDeviceID(int(self.MDG_Num))
        print("Device ID: ", self.deviceID)

        self.graphs = {}
        self.graphs['data_vol'] = Counter('Num_Of_Total_Generated_Data', 'The total number of processed requests')
        self.graphs['requested_data_vol'] = Gauge('Requested_Data_Volume', 'The total number of unprocessed requests')
        self.graphs['requested_timeinterval'] = Gauge('Requested_Time_Interval', 'The total number of unprocessed requests')

        try:
            self.logger = self.get_logger(settings.SYSLOG_HOST, settings.SYSLOG_PORT)
        except Exception as e:
            print("Exception in logger init".e)
        self.init_mqtt()
        self.flaskApp = Flask(__name__)

        try:
            self.logger.info("MQTT_MDG HAS STARTED")
        except Exception as e:
            print("Exception in logger", e)

        thread1 = threading.Thread(target=self.dataGenPolling)
        thread1.start()

        thread2 = threading.Thread(target=lambda : self.flaskApp.run(host="0.0.0.0", port=self.flaskPort, debug=False))
        thread2.start()

        self.graphs['requested_timeinterval'].set(1000)

        @self.flaskApp.route("/stopmdg")
        def stopmdg():
            self.FLAG_ENABLE_DATA_GEN = False
            return "{\n\t\"status\":\"success\",\n\t\"message\":\"stopped\"\n}"

        @self.flaskApp.route("/resumemdg")
        def resumemdg():
            self.FLAG_ENABLE_DATA_GEN = True
            return "{\n\t\"status\":\"success\",\n\t\"message\":\"mdg is resuming\"\n}"

        @self.flaskApp.route("/settimeinterval", methods=['POST'])
        def settimeinterval():
            try:
                json = request.get_json()
                self.settimeinterval = int(json["settimeinterval"])
                self.graphs['requested_timeinterval'].set(int(self.settimeinterval))
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"success\"\n}"
            except Exception as e:
                print("Exceptionn in settimeinterval: ", e)

        @self.flaskApp.route("/setlimit", methods=['POST'])
        def setlimit():
            try:
                json = request.get_json()
                print(json)
                self.tempRange[0] = float(json["temperature"]["min"])
                self.tempRange[1] = float(json["temperature"]["max"])

                self.humidityRange[0] = float(json["humidity"]["min"])
                self.humidityRange[1] = float(json["humidity"]["max"])

                self.moistureRange[0] = float(json["moisture"]["min"])
                self.moistureRange[1] = float(json["moisture"]["max"])

                self.infraredRange[0] = int(json["infrared"]["min"])
                self.infraredRange[1] = int(json["infrared"]["max"])

                self.pressureRange[0] = float(json["pressure"]["min"])
                self.pressureRange[1] = float(json["pressure"]["max"])
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"success\"\n}"
            except Exception as err:
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"invalid json format\"\n}"

        @self.flaskApp.route("/metrics")
        def requests_count():
            res = []
            for k, v in self.graphs.items():
                res.append(prometheus_client.generate_latest(v))
            return Response(res, mimetype="text/plain")

        @self.flaskApp.route("/generatedata", methods=['POST'])
        def datagenerator():
            try:
                self.FLAG_ENABLE_DATA_GEN = True
                json = request.get_json()
                self.settimeinterval = int(json["polling_freq"])
                self.mdg_settings["data_vol"] = int(json["data_vol"])
                self.num_of_devices = int(json["num_of_devices"])
                self.payload_counter = 0
                self.graphs['requested_data_vol'].set(0)
                self.begin_time = time.time()
                for i in range(0, self.mdg_settings["data_vol"]):
                    self.graphs['requested_data_vol'].inc()
                if self.num_of_devices <= 8:
                    try:
                        self.logger.info("DATAGENERATOR- API for datageneration with pollig_freq: " + str(
                            self.settimeinterval) + ", Data Volume: " + str(
                            self.mdg_settings["data_vol"]) + ", Num of Lionx devices: " + str(self.num_of_devices))
                    except Exception as e:
                        print("Exception in logger", e)
                    return "{\n\t\"status\":\"success\",\n\t\"message\":\"success\"\n}"
                else:
                    self.logger.error("DATAGENERATOR- API failed due to number of devices exceeded from 8")
                    return "{\n\t\"status\":\"success\",\n\t\"message\":\"Limit exceeded, max devices are 8\"\n}"
            except Exception as e:
                self.logger.error("DATAGENERATOR- Invalid api payload")
                print("Exception in generatedata: ", e)

    def createDeviceID(self, mdg_num):
        for i in range(self.maxLionxInMDG):
            self.deviceID.append("MDG_" + str(self.mdgOffset + ((mdg_num-1)*8+i+1)))

    def get_logger(self, host: str, port: int) -> logging.Logger:
        # Value passed to getLogger is app identifier
        logger = logging.getLogger('sample_application')
        logger.setLevel(logging.DEBUG)
        handler = SysLogHandler(address=(host, port), socktype=socket.SOCK_STREAM)

        # Here we can specify any message format which we want
        handler.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s\n'))

        logger.addHandler(handler)

        return logger

    def init_mqtt(self):
        self.mdg_mqtt_receiver = Mqtt_Receiver(settings.BROKER_HOSTNAME, settings.BROKER_PORT,
                                               settings.MDG_CLIENT_ID+self.MDG_Num,
                                               settings.TOPIC_TO_RECEIVE_DATA)
        self.mdg_mqtt_receiver.mqtt_receiver_callback(self.receiver_callback_function)
        self.mqtt_sender = Mqtt_Sender(settings.BROKER_HOSTNAME, settings.BROKER_PORT, settings.SENDER_CLIENT_ID)

    def receiver_callback_function(self, callback):
        pass
        # self.recv_jsonData = json.loads(callback)
        # if "polling_freq" and "data_vol" in self.recv_jsonData:
        #     """
        #     >--- When {settings} are applied ---<
        #     """
        #     self.mqtt_setting_update()
        # elif "polling_freq" in self.recv_jsonData:
        #     self.mdg_settings["polling_freq"] = int(self.recv_jsonData["polling_freq"])  # Freq in ms

    def mqtt_setting_update(self):
        try:
            self.mdg_settings["polling_freq"] = int(self.recv_jsonData["polling_freq"])  # Freq in ms
            self.mdg_settings["data_vol"] = int(self.recv_jsonData["data_vol"])
            self.FLAG_ENABLE_DATA_GEN = True
            self.payload_counter = 0
        except Exception as e:
            print("Exception: ", e)

    def createRecords(self):
        try:
            self.temp_data = []
            self.humid_data = []
            self.ir_data = []
            self.moisture_data = []
            self.pressure_data = []
            for i in range(0, self.num_of_devices):
                datetime_object = datetime.datetime.now()
                # data = "{\"device_id\":\"" + str(settings.DEVICE_ID[i]) + "\",\"timestamp\":\"" + str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")) + "\",\"temperature\":\"" + str(round(random.uniform(92.0, 99.9), 1)) + "\",\"humidity\":\"" + str(random.randint(50, 95)) + "%" + "\",\"ir_sensor\":\"" + str(random.randint(0, 2)) + "\"," + "\"soil_moisture\":\"" + str(round(random.uniform(20.0, 99.9), 1)) + "%" + "\"}"
                # data1 = "{\"identity\":{\"device_id\":\"" + str(settings.DEVICE_ID[i]) + "\"},\"process_data\":{\"timestamp\":\"" + str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")) + "\",\"temperature\":\"" + str(round(random.uniform(92.0, 99.9), 1)) + "\"}}"
                # data2 = "{\"identity\":{\"device_id\":\"" + str(settings.DEVICE_ID[i]) + "\"},\"process_data\":{\"timestamp\":\"" + str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")) + "\",\"humidity\":\"" + str(random.randint(50, 95)) + "%\"}}"
                # data3 = "{\"identity\":{\"device_id\":\"" + str(settings.DEVICE_ID[i]) + "\"},\"process_data\":{\"timestamp\":\"" + str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")) + "\",\"ir_sensor\":\"" + str(random.randint(0, 2)) + "\"}}"
                # data4 = "{\"identity\":{\"device_id\":\"" + str(settings.DEVICE_ID[i]) + "\"},\"process_data\":{\"timestamp\":\"" + str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")) + "\",\"soil_moisture\":\"" + str(round(random.uniform(20.0, 99.9), 1)) + "%\"}}"
                data1 = {"identity": {"device_id": str(self.deviceID[i])},
                         "process_data": {"timestamp": str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")),
                                          "temperature": str(
                                              round(random.uniform(self.tempRange[0], self.tempRange[1]), 1))}}
                data2 = {"identity": {"device_id": str(self.deviceID[i])},
                         "process_data": {"timestamp": str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")),
                                          "humidity": str(
                                              round(random.uniform(self.humidityRange[0], self.humidityRange[1]), 1))}}
                data3 = {"identity": {"device_id": str(self.deviceID[i])},
                         "process_data": {"timestamp": str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")),
                                          "infrared": str(
                                              random.randint(self.infraredRange[0], self.infraredRange[1]))}}
                data4 = {"identity": {"device_id": str(self.deviceID[i])},
                         "process_data": {"timestamp": str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")),
                                          "moisture": str(
                                              round(random.uniform(self.moistureRange[0], self.moistureRange[1]),
                                                    1))}}
                if self.FLAG_ENABLE_PRESSURE:
                    data5 = {"identity": {"device_id": str(self.deviceID[i])},
                             "process_data": {"timestamp": str(datetime_object.strftime("%Y-%m-%d %H:%M:%S")),
                                              "pressure": str(
                                                  round(random.uniform(self.pressureRange[0], self.pressureRange[1]),
                                                        1))}}

                self.temp_data.append(data1)
                self.humid_data.append(data2)
                self.ir_data.append(data3)
                self.moisture_data.append(data4)
                if self.FLAG_ENABLE_PRESSURE:
                    self.pressure_data.append(data5)
            # print(self.temp_data)
        except Exception as e:
            print("Exception in create records: ", e)

    def dataGenPolling(self):
        while True:
            if (self.FLAG_ENABLE_DATA_GEN == True):
                # if (self.payload_counter < self.mdg_settings["data_vol"]):
                if( (time.time() - self.begin_time) < (int(self.mdg_settings["data_vol"])) ):    # data_vol is time in seconds
                    if (time.time() - self.start_time) > (int(self.settimeinterval) / 1000):
                        print("****** Sent ******")
                        self.start_time = time.time()
                        self.payload_counter = self.payload_counter + 1
                        self.createRecords()
                        self.mqtt_sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_TEMP_DATA, self.temp_data)
                        # time.sleep(0.2)
                        self.mqtt_sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_HUMID_DATA, self.humid_data)
                        # time.sleep(0.2)
                        self.mqtt_sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_IR_DATA, self.ir_data)
                        # time.sleep(0.2)
                        self.mqtt_sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_MOISTURE_DATA, self.moisture_data)
                        if self.FLAG_ENABLE_PRESSURE:
                            self.mqtt_sender.mqtt_send(settings.TOPIC_TO_TRANSMIT_PRESSURE_DATA, self.pressure_data)
                        # time.sleep(0.2)
                        self.graphs['data_vol'].inc()
                        try:
                            if self.FLAG_ENABLE_PRESSURE:
                                self.logger.info("PROCESS_DATA- " + str(self.temp_data) + str(self.humid_data) + str(
                                    self.ir_data) + str(self.moisture_data) + str(self.pressure_data))
                            else:
                                self.logger.info("PROCESS_DATA- " + str(self.temp_data) + str(self.humid_data) + str(
                                    self.ir_data) + str(self.moisture_data))
                        except Exception as e:
                            print("Exception in logger", e)


if __name__ == "__main__":
    MDG_MQTT()
