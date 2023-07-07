import paho.mqtt.client as sender
import settings
import json


class Mqtt_Sender():

    def __init__(self, hostname, port, client_id):

        self.hostname = hostname
        self.port = port
        self.client_id = client_id

        self.enable_debug = False
        try:
            self.client = sender.Client(client_id=self.client_id, clean_session=False)
            # self.client.username_pw_set(settings.BROKER_USERNAME, settings.BROKER_PASSWORD)
            self.client.connect(self.hostname, self.port, 60)
            self.client.loop_start()
        except Exception as ex:
            print("Sender Exception: "+str(ex))

    def mqtt_send(self, mqtt_topic, payload):
        # self.client.connect(self.hostname, self.port, 60)
        if self.enable_debug :
            print("Sending MSG from MQTT: " + str(payload));
        # self.client.publish(mqtt_topic, str(payload))
        self.client.publish(mqtt_topic, json.dumps(payload))
