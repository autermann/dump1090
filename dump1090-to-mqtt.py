#!/usr/bin/env python2
import json
from pprint import pprint
from decimal import Decimal
import sched, time
import paho.mqtt.client as mqtt
import time
import sys
import argparse

class Config(object):
    pass

c = Config()
parser = argparse.ArgumentParser()
parser.add_argument('-s', '--mqtt-server', help='the target host of the MQTT broker', required=True)
parser.add_argument('-p', '--mqtt-port', help='the target port of the MQTT broker', type=int, default=1883)
parser.add_argument('-m', '--mqtt-topic', help='the MQTT topic', default='adsb52n')
parser.add_argument('-t', '--timeout', help='generic timeout for network communication', type=int, default=60)
parser.add_argument('-d', '--dump1090-file', help='the dump1090 aircraft.json file', required=True)
parser.add_argument('-l', '--logging', help='enable logging to stdout', type=bool, default=False)
args = parser.parse_args(namespace=c)

#global fields
s = sched.scheduler(time.time, time.sleep)
knownAircrafts = {}

def pushAircraftUpdate(ac):
    if "lat" in ac:
        ac["timestamp"] = int(time.time())
        acJson = json.dumps(ac)
        if c.logging:
            print("### Publishing on topic '"+ c.mqtt_topic +"': "+ acJson)
        client.publish(c.mqtt_topic, acJson, 1, 0)
    elif c.logging:
        print("### no location information for: "+acJson)

def readAircraftFile():
    f = open(c.dump1090_file, 'r')
    data = json.loads(f.read())

    for ac in data["aircraft"]:
        if not ac["hex"] in knownAircrafts:
            knownAircrafts[ac["hex"]] = ac
            #print("Added "+ json.dumps(ac))
            pushAircraftUpdate(ac)
        else:
            lastSeen = Decimal(knownAircrafts[ac["hex"]]["seen"])
            nowSeen = Decimal(ac["seen"])
            if lastSeen > nowSeen:
                pushAircraftUpdate(ac)
    #read again in 1 second
    s.enter(1, 1, readAircraftFile, ())


client = mqtt.Client()
client.connect(c.mqtt_server, c.mqtt_port, c.timeout)

s.enter(1, 1, readAircraftFile, ())
s.run()
