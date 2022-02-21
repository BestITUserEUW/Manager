import paho.mqtt.client as mqtt
from collections import deque
from playsound import playsound
import multiprocessing
import json
import time
import sys
import os

class MQTT():
    __ip = "192.168.0.36"
    __client = None
    __base_topic = "manager"
    __topics = {
        "sound":__base_topic+"/sound",
        "speech":__base_topic+"/speech",
    }
    queues = {
        "sound": deque(),
        "speech": deque(),
    }
   
    @classmethod 
    def __connect_to_broker(cls,client,userdata,flags,rc):
        topic = cls.__base_topic+"/#"
        client.subscribe(topic)
        print(f"[MQTT]: Connected to {topic} with Code: {rc}")
       
    @classmethod
    def __on_message(cls,client,userdata,msg):
        msg_str = str(msg.payload.decode("utf-8"))
        topic = msg.topic
        print(f"[MQTT]: received {msg_str} on topic {topic}")
        dmsg = json.loads(msg_str)
        if topic == cls.__topics["sound"]:
            cls.queues["sound"].appendleft(dmsg)
            print(f"[MQTT]: {dmsg} was put into sound queue...")
        elif topic == cls.__topics["speech"]:
            cls.queues["speech"].appendleft(dmsg)
            print(f"[MQTT]: {dmsg} was put into speech queue...")


    @classmethod
    def connect(cls):
        while True:
            try:
                print("[MQTT]: Connecting to broker: {}\n".format(cls.__ip))
                client = mqtt.Client()
                client.connect(cls.__ip,1883,60)
                client.on_connect = cls.__connect_to_broker
                client.on_message = cls.__on_message
                cls.__client = client
                client.loop_start()
                break
            except Exception:
                print(f"[MQTT]: broker {cls.__ip} could not be reached retrying...")
                time.sleep(1)
    
    @classmethod
    def publish(cls,topic,msg):
        try:
            if cls.__client!=None:
                cls.__client.publish(topic,msg)
                print(f"[MQTT]: published {msg} on {topic}...")
        except Exception:
            print("[MQTT]: timeout occured trying to reconnect...")
            cls.__disconnect()
            cls.__connect()

    @classmethod
    def disconnect(cls):
        cls.__client.disconnect()
        print("[MQTT]: disconnected...")
        cls.__client = None
    
    @classmethod
    def println(cls,msg):
        sys.stdout.write(msg)
        sys.stdout.flush()

class Manager():
    __mqtt = MQTT()
    __sound_dir = os.getcwd()+"/sounds"

    @classmethod
    def start(cls):
        cls.__mqtt.connect()
        cls.sound_worker()
        
    @classmethod
    def play(cls,file):
        path = f"{cls.__sound_dir}/{file}.wav"
        while True:
            print(f"[Player]: playing: {path}...")
            playsound(path)
            print("[Player]: done...")
    
    @classmethod     
    def sound_worker(cls):
        player = None
        print("[SOUND WORKER]: starting...")
        while True:
            if len(cls.__mqtt.queues["sound"])!=0:
                msg = cls.__mqtt.queues["sound"].pop()
                print(f"[SOUND WORKER]: took {msg} from sound queue...")
                print(msg)
                if msg["cmd"] == "start":
                    player = multiprocessing.Process(target=cls.play,args=(msg["audio"],),daemon=True)
                    player.start()
                    print(f"[SOUND WORKER]: started Player with pid@{player.pid}")
                elif msg["cmd"] == "stop":
                    print(f"[SOUND WORKER]: killed Player with pid@{player.pid}")
                    player.terminate()
            else:
                time.sleep(0.5)



if __name__ == "__main__":
    Manager.start()