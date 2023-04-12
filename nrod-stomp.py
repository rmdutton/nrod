#!/usr/bin/env python3





#import argparse

import json
import stomp
import datetime
import time
import random
import string
import math
import io
import sys
import signal

from time import sleep
from itertools import count
from dotenv import dotenv_values
from random import randrange

import redis




    #todo: rename the Persistence class(es) as MessageQueue, and place it
    #in it's own module.

    #MessageQueue.print
    #MessageQueue.error
    #MessageQueue.push_to_stream
    #MessageQueue




class Persistence:


    """

    """

    @staticmethod
    def out(db,*args,**kwargs) -> None:

        output = io.StringIO()
        print(*args,file=sys.stdout,**kwargs)
        print(*args,file=output,end='',**kwargs)

        if isinstance(db,redis.Redis):
            db.xadd("NROD:Log",{'message':output.getvalue(),'status':"Event"})


    """

    """

    @staticmethod
    def err(db,*args,**kwargs) -> None:

        output = io.StringIO()
        print(*args,file=sys.stderr,**kwargs)
        print(*args,file=output,end='',**kwargs)

        if isinstance(db,redis.Redis):
            db.xadd("NROD:Log",{'message':output.getvalue(),'status':"Error"})



    """

    """

    @staticmethod
    def push(db,streamid:str,**kwargs) -> None:

        #
        for key, value in kwargs.items():
            if not isinstance(value,str):
                Persistence.err(db,"Cannot push datatype: ",key,":",type(value),\
                        " to the stream ",streamid,sep='')
                return

        if isinstance(db,redis.Redis):
            db.xadd(streamid,{**kwargs})













class TDSCSubcription():


    @property
    def name(this) -> str:
        return "TDSC"

    @property
    def topic(this) -> str:
        return "/topic/TD_ALL_SIG_AREA"

    @property
    def enabled(this) -> bool:
        return this._enabled



    def __init__(this,persistence,enabled = False):


        this._persistence = persistence


        this._enabled = enabled



    def on_message(this,timestamp:int,messages:[]) -> None:

        for message in messages:
            Persistence.push(this._persistence,"NROD:"+this.name,content=str(message))





class TRSTSubcription():




    """

    Avanti West Coast       --> /topic/TRAIN_MVT_HF_TOC
    Everything              --> /topic/TRAIN_MVT_ALL_TOC

    """

    @property
    def name(this) -> str:
        return "TRST"

    @property
    def enabled(this) -> bool:
        return this._enabled

    @property
    def topic(this) -> str:
        return "/topic/TRAIN_MVT_HF_TOC"




    def __init__(this,persistence, enabled = False):


        this._persistence = persistence


        this._enabled = enabled


    """
    e.g. message:

    [
    {'header': {'msg_type': '0003', ... 'body': { ... } }
    {'header': {'msg_type': '0003', ... 'body': { ... } }
    ]

    """

    def on_message(
            this,timestamp:int,messages:[]
        ) -> None:
        

        for message in messages:
            Persistence.push(this._persistence,"NROD:"+this.name,content=str(message))





class VSTPSubcription():


    """

    """

    @property
    def name(this) -> str:
        return "VSTP"

    @property
    def enabled(this) -> bool:
        return this._enabled

    @property
    def topic(this) -> str:
        return "/topic/VSTP_ALL"





    def __init__(this,persistence,enabled = True):

        this._persistence = persistence

        this._enabled = enabled



    def on_message(this,timestamp:int,message) -> None:
        Persistence.push(this._persistence,"NROD:"+this.name,content=str(message))









class Connection(stomp.ConnectionListener):



    _connection: stomp.Connection
    
    _subscriptions: []



    """

    """

    @property
    def is_connected(this):
        return this._connection.is_connected()


    """
    > endpoint:             this is the hostname:str and port:int of the remote server (as a tuple).
    > subscriptions:        this is an array of subscription objects. the subscription object needs to
                            be callable, where the parameters are: timestamp:int, messages:[] which
                            is an array of dict structures.
    > username (optional)   username; can be None for no username.
    > password (optional)   password; can be None for no password.
    > heartbeat:            the number of ms between heartbeat messages: (client,server), zeros mean
                            don't send heartbeats. e.g: (2000,0) means the client will send heartbeats
                            every 2 seconds, and the server won't send any at all.
    """

    def __init__(this,hostport:(str,int),hostauth:(str,str),subscriptions:[],
                 heartbeat:(int,int) = (0,0),persistence = None):

        # auth creds are required for connecting later on.
        this._username = hostauth[0]
        this._password = hostauth[1]

        # create the stomp connection object, but don't proceed with establishing a connection yet.
        this._connection = stomp.Connection([hostport], keepalive=True, heartbeats=heartbeat, heart_beat_receive_scale=2.0)

        # this class handles stomp events, and separates messages for each subscription.
        this._connection.set_listener("main",this)

        # each subscription is placed into a dictionary container, where the key is the subscription id field.
        this._subscriptions = {name: subscription for name, subscription in zip([x.name for x in subscriptions], subscriptions)}

        # this class provides an abstraction for the storage method.
        this._persistence = persistence

    
    """
    
    """

    def on_message(this, message):

        # get the stomp headers, and the body of the content.
        headers, body = message.headers, message.body

        # get the message(s); the type is subscription dependant.
        body_json = json.loads(body)

        # ack receipt of the message; this is important for client-individual subscriptions.
        this._connection.ack(id=headers["message-id"],subscription=headers["subscription"])

        # get the timestamp of when the message(s) were generated.
        timestamp = int(headers.get('timestamp',time.time_ns()/1000.0))

        # send the message to the appropriate handler; todo: report cases where the subscription doesn't match any key.
        this._subscriptions.get(headers.get('subscription'),lambda*_:None).on_message(timestamp,body_json)

        #print(message)


    """

    """

    def on_connecting(this, hostandport):
        Persistence.out(this._persistence,"Connecting to ",\
                hostandport[0],":",str(hostandport[1]),sep="")

    """
    """

    def on_connected(this, message):

        # get the headers, which contain information about the remote server.
        headers = message.headers

        Persistence.out(this._persistence,"Connected")
        Persistence.out(this._persistence,"Server:",headers.get("server",""))

    """

    """

    def on_heartbeat(this):
        Persistence.out(this._persistence,"Received heartbeat")

    def on_heartbeat_timeout(this):
        Persistence.out(this._persistence,"Heartbeat timeout")

    """


    """

    def on_disconnected(this):
        Persistence.out(this._persistence,"Disconnected")
 

    """

    """

    def on_error(this, message): # todo: expand..
        Persistence.err(this._persistence,"Error:",str(message))




    """

    """

    def connect(this) -> None:

        # do not attempt to make a connection if one already exists..
        if this.is_connected:
            return

        # client id should be set to the username. if no username is
        # provided, use a random 7 character string.
        client_id = this._username if this._username else\
            str().join(random.choice(string.ascii_letters)\
            for n in range(7))

        # the connection header.
        connect_headers = {
        "username":     this._username,
        "passcode":     this._password,
        "client-id":    client_id,
        "heart_beat_receive_scale": 2.0,
        "wait":         True
        }

        try:
            this._connection.connect(**connect_headers)
            # this is to prevent subscribing before the on_connected method is called.
            sleep(0.1)
        except stomp.exception.ConnectFailedException as error:
            Persistence.err(this._persistence,"Connection could not be established:",error)
            return

        # iterate over each subscription.
        for subscription in this._subscriptions.values():

            # do not subscribe unless the sub is enabled.
            if subscription.enabled:
                
                def get_subscription_headers(
                        subscription,subscription_id
                    ) -> dict:
                    return {"destination":subscription.topic,"id":\
                        subscription.name,"activemq.subscriptionName":\
                        subscription_id+"_"+subscription.topic,\
                        "ack": "client-individual"}

                sub_headers = get_subscription_headers(subscription,this._username)
                this._connection.subscribe(**sub_headers)
                Persistence.out(this._persistence,"Subscribed @",subscription.topic)

    """

    """

    def disconnect(this):

        # do we need to disconnect?
        if not this.is_connected:
            return
            
        # unsubscribe from all active subscriptions.
        for subscription in this._subscriptions.values():
            if subscription.enabled:
                this._connection.unsubscribe(id=subscription.name)
                Persistence.out(this._persistence,"Unsubscribed @",subscription.topic)

        # begin the disconnection process.
        this._connection.disconnect()

        # block until the connection is terminated.
        while this.is_connected:
            sleep(0.1)





if __name__ == "__main__":


    # not very secure. perhaps use system keychain?
    env = dotenv_values()



    # get redis credentials?
    persistence = redis.Redis(charset="utf-8",decode_responses=True)



    subscriptions = [
    TRSTSubcription(persistence=persistence,enabled=True),  # handles messages sent by Trust.
    VSTPSubcription(persistence=persistence,enabled=True),  # handles schedule alterations by the VSTP system.
    TDSCSubcription(persistence=persistence,enabled=False)   # handles messages sent at the berth level.
    ]

    hostport = (
    env.get("NROD_HOST"),
    env.get("NROD_PORT")
        )

    hostauth = (
    env.get("NROD_USERNAME"),
    env.get("NROD_PASSWORD")
        )

    # 
    connection = Connection(
    hostport,
    hostauth,
    subscriptions,
    (5000,10000),
    persistence
        )



    class SignalHandler:

        @property
        def nosig(this) -> bool:
            return this._nosig

        def __init__(this) -> None:
            this._nosig = True
            signal.signal(signal.SIGINT, this._sig)
            signal.signal(signal.SIGTERM, this._sig)

        def _sig(this,*args):
            this._nosig = False

    handler = SignalHandler()
    while handler.nosig:
        sleep(0.5)
        if not connection.is_connected:
            connection.connect()
    connection.disconnect()

