import threading
from flask import Response, Flask, request
import shutil
import os
from subprocess import Popen

class MDG_APP():

    def __init__(self):
        self.enable_debug = True
        self.num_of_mdg = 0

        self.active_ports = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        self.mdgobj = []


        self.flaskApp = Flask(__name__)

        thread1 = threading.Thread(target=lambda: self.flaskApp.run(host="0.0.0.0", port=20231, debug=False))
        thread1.start()


        @self.flaskApp.route("/createmdg", methods=['POST'])
        def createMDG():
            json = request.get_json()
            port = int(json['port'])
            if port not in self.active_ports:
                self.createMDG(port)
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"success\"\n}"
            else:
                return "{\n\t\"status\":\"failure\",\n\t\"message\":\"port already running\"\n}"

        @self.flaskApp.route("/runningmdg", methods=['GET'])
        def runningMDGCount():
            temp = []
            for i in range(0, len(self.active_ports)):
                if self.active_ports[i] > 0:
                    temp.append(str(self.active_ports[i]))
            return "{\n\t\"status\":\"success\",\n\t\"message\":\""+ str(temp) + "\"\n}"
            # return "{\n\t\"status\":\"success\",\n\t\"message\":\""+ str(self.active_ports) + "\"\n}"

        @self.flaskApp.route("/deletemdg", methods=['POST'])
        def deleteMDG():
            json = request.get_json()
            port = int(json['port'])
            print("Delete MDG")
            if port in self.active_ports:
                portindex= self.active_ports.index(port)
                print(portindex)
                cmd = "kill $(lsof -t -i:" + str(port) + ")"
                print(cmd)
                os.popen(cmd)
                os.remove("mdg_"+str(portindex) + ".py")
                self.active_ports[portindex] = 0
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"" + str(self.active_ports) + "\"\n}"
            else:
                return "{\n\t\"status\":\"success\",\n\t\"message\":\"No active service on this port\"\n}"

    def createMDG(self, port):
        print("Create MDG")
        self.num_of_mdg = self.num_of_mdg + 1
        self.active_ports[self.num_of_mdg] = port
        shutil.copyfile("mdg.py", "mdg_" + str(self.num_of_mdg) + ".py")
        cmd = "python3 mdg_" + str(self.num_of_mdg) + ".py " + str(port) + " " + str(self.num_of_mdg)
        # cmd = "mdg_" + str(self.num_of_mdg) + ".py " + str(port) + " " + str(self.num_of_mdg)
        # os.system(cmd)
        # proc = Popen(cmd)
        proc = Popen([cmd], shell=True)
        # proc.wait(1)
        """
        INFO: MDG Requirement
            1. PORT
            2. TOPICS
            3. LIONX
        """

if __name__ == "__main__":
    MDG_APP()