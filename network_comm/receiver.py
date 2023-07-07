import paho.mqtt.client as receiver
import settings
import sys

class Mqtt_Receiver():

    def __init__(self, hostname, port, client_id, subscriber_topic):

        self.hostname = hostname
        self.port = port
        self.client_id = client_id
        self.subscriber_topic = subscriber_topic

        self.update_received_message = None
        self.enable_debug = False
        self.receiver_client_connected = False

        try:
            self.client = receiver.Client(client_id=self.client_id, clean_session=False)
            # self.client.username_pw_set(settings.BROKER_USERNAME, settings.BROKER_PASSWORD)
            # self.client = receiver.Client()
            self.client.on_connect = self.receiver_on_connect
            self.client.on_message = self.receiver_on_message
            # self.client.username_pw_set(settings.BROKER_USERNAME, settings.BROKER_PASSWORD)
            self.client.connect(host=self.hostname, port=self.port, keepalive=10)
            self.client.loop_start()


            # mqtt_connection = threading.Thread(target=self.mqtt_loop_task)
            # mqtt_connection.setDaemon(True)
            # mqtt_connection.start()
        except Exception as ex:
            print("Exception in Receiver: "+ str(ex))
            sys.exit()

    def mqtt_receiver_callback(self, callback):
        self.update_received_message = callback

    def receiver_on_connect(self, client, userdata, flags, rc):
        if self.enable_debug:
            print("Connected with result code " + str(rc))
        if rc == 0 and self.enable_debug:
            print("***** Receiver is connected successfully with MQTT broker *****")
        elif rc != 0:
            print("***** Receiver is not connected with MQTT broker *****")

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        self.client.subscribe(self.subscriber_topic)
        # for i in range(0,len(self.subscriber_topic)):
        #     self.client.subscribe(self.subscriber_topic[i], qos = 0)


    def receiver_on_message(self, client, userdata, msg):
        if self.enable_debug:
            print("***** Received MSG from MQTT: "+msg.topic + " " + str(msg.payload) + " *****")
        self.update_received_message(str(msg.payload.decode('utf-8')))

    def receiver_reconnect(self):
        if not self.receiver_client_connected:
            self.client.connect(host=self.hostname, port=self.port, keepalive=60)