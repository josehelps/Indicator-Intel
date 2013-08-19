#!/usr/bin/python

# Ilastic receiver, gets data from clients
#TODO:
#help functions and arguments
#parser for ilastic input configs and feeds, logging, error handling, better
#Error Handling on: reading config, lisenting to socket, fail to connect to elasticsearch server
#doc

import socket
import esclient
import ConfigParser
import re
import os
import logging
import datetime
from crontab import CronTab
import simplejson as json
import collections
from collections import defaultdict
import logging




#CRON 
cron = CronTab(os.getlogin())
cron.remove_all('curl')

logger = logging.getLogger('gatherer')
logger.setLevel(logging.INFO)

# create a file and console handlehandler = logging.FileHandler('gatherer.log')
fh = logging.FileHandler('gatherer.log')
fh.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.WARN)
# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)
# add the handlers to the logger
logger.addHandler(fh)
logger.addHandler(ch)

#GLOBAL VARIABLES
configPath = 'gatherer.conf'
splitter = '---'

#Parsing config
config = ConfigParser.RawConfigParser()
config.read(configPath)
sections = config.sections()

#Pre Compile regex could add as a function for all regex later
net_prefix_re = re.compile("^<\d\d\>")

def help():
    """
    TODO:
        help functions and arguments
        parser for ilastic input configs and feeds, logging, error handling, better
        Error Handling on: reading config, lisenting to socket, fail to connect to elasticsearch server
        documentation

    Usage:


    Example:

    """
def configSectionValue(section):
    """ helper function to recall config values """
    values = {}
    options = config.options(section)
    for option in options:
        try:
            values[option] = config.get(section, option)
            if values[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
                logger.error("exception on %s!" + option)
                values[option] = None
    return values

def parseFeeds(feed_file = configSectionValue('feed')['file']):
    """ helps parse the feed file defined under configuration """
    feeds = defaultdict(lambda: defaultdict(dict))
    values = dict()
    feedobjects = []
#    cron.remove_all('gatherer')
    #Create config parser and get the sections
    feed_config = ConfigParser.RawConfigParser()
    feed_config.read(feed_file)
    feed_sections = feed_config.sections()
    
    #Store each configuration with the options in a hash
    for feed in feed_sections:
        options = feed_config.options(feed)
        for option in options:
            try:
                feeds[feed][option] = feed_config.get(feed, option)
                #feeds[feed][option] = feed_config.get(feed, option)
                if feeds[feed][option] == -1:
                    DebugPrint("skip: %s" % option)
            except:
                logger.error("exception on %s!" + option)
                feeds[feed][option] = None
            #print "Feed: ", feed, "Option: ", option, "Values: ", values
        #print feeds.setdefault(feed, []).append(values)
    return feeds


def scheduler(feeds):
    """ Takes in a feed dictionary, schedules cron jobs under the user which ran the app """

    for feed,option in feeds.iteritems():
        logger.warn("scheduling feed: %s for %s consumption", feed, option.get('schedule'))
        command = 'curl '+ option.get('source')+ ' -o /dev/shm/' + feed
        job = cron.new(command=command,comment='gatherer')
        if option.get('schedule') == 'hourly':
            job.minute.every(60)
        elif option.get('schedule') == 'daily':
            job.hour.every(24)
        elif option.get('schedule') == 'weekly':
            job.dow.on('SUN')
        elif option.get('schedule') == 'now':
            job.minute.every(1)
        else:
            job.hour.every(24)
        job.enable
        cron.write()

#def check_modified(feed_file):
    
#    time = feed_file + "-" + str(os.path.getmtime(feed_file))
#    old_time = feed_file + "-" + str(os.path.getmtime(feed_file)) + "\n"
#    f.write(old_time)
#    print "Modified: ", old_time
#    for line in f:
#        if time == old_time:
#            print " no Modified"
#            return True
#        else:
#            old_time = feed_file + "-" + str(os.path.getmtime(feed_file)) + "\n"
#            f.write(old_time)
#            print "Modified: ", old_time
#            return False
    

def writeFeed2ES():
    """ Writes Feeds to Elasticsearch """
    groupings = dict()
    body = dict()
    matches = []
    feeds = parseFeeds() 
    for feed,option in feeds.iteritems():
        if os.path.exists('/dev/shm/' + feed): 
            logger.warn('feed %s exists processing it', feed)
            mapping = option.get('mapping')                
            mapping = mapping.split(splitter)
            groupings.setdefault(feed,[]).append(mapping)
            feed_file = open('/dev/shm/'+feed, 'r') 
            for groups in groupings[feed]:
                for group in groups:
                    parser = re.compile(group)
                    for line in feed_file:
                        match = re.match(parser,line)
                        if (match):
                            matches.append(match.groupdict())
        
            logger.debug("Sending feed data to index %s", feed)
            for d in matches:     #match = prog.match(feed)
                clientes.index(feed,'feed',d,parent=None, routing=None)
                #results.append(match)
            #print results
        else:
            logger.warn('feed data for %s has not been collected, look at crontab for scheduled jobs', feeds)


def writeData2ES(data):
    """ Writes data received on UDP socket to elasticsearch server """
    groupings = dict()
    body = dict()
    matches = []
    
    writeFeed2ES()

    

    #Parse each mapping variable
    for index in configSectionValue('mapping'):
        map_re = mapping(index)
        map_re = map_re.split(splitter)
        groupings.setdefault(index,[]).append(map_re)

        identification = identify(index)

        if re.search(identification, data):
            logger.debug("Data Matching on index " + index)
            for groups in groupings[index]:
                for group in groups:
                    match = re.match(group,data)
                    if (match):
                        matches.append(match.groupdict()) 
            #Merge all matched Groupings
            for d in matches: body.update(d)
            #logger.debug("Sending to indexer " + body)
            #Write it to elasticsearch
            clientes.index(index,'event',body,parent=None, routing=None)

def identify(index):
    inputs = configSectionValue('input')
    return inputs[index]

def mapping(index):
    mappings = configSectionValue('mapping')
    return mappings[index]

def elasticsearch(es_port,es_host):
    #Create an ESClient
    global clientes
    clientes = esclient.ESClient("http://" + es_host + ":" + es_port + "/")
    #Parse all indices
    for inputs in configSectionValue('input'):
        # create index
        logger.debug("creating index: %s", inputs)
        clientes.create_index(inputs,body=None)
    #logger.debug("elasticsearch client status: %s", clientes.status())
    
def socketLisent(host = configSectionValue('server')['host'], port = configSectionValue('server')['port']):
    """ Configures UDP socket to lisent for data to be send to Elasticsearch
    Also INITs index in elasticsearch """
#Setting up socket lisenter
    host.strip()
    port.strip()
    port = int(port)
    logger.warn("lisenting on %s:%d", host, port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host,port))

    while True:
        data,addr = sock.recvfrom(1024)
        """Drop <\d\d> added by logs sent through network, using syslog-ng"""
        if net_prefix_re.match(data):
            data = re.sub(net_prefix_re, "", data) 
        logger.warning('received data: '+ data)
        writeData2ES(data)

def main():
    """Main function, calls other functions"""
    logger.info('INIT') 

    #Parse the Feed configuration File
    feeds = parseFeeds()

    #Get all the feeds
    feed = []
    for key in feeds.keys():
        feed.append(key)
    logger.info('CONFIGURED FEEDS %s', feed) 

    #Schedule all the feeds to be gatherered
    scheduler(feeds)

    #Configure the elasticsearch server
    elasticsearch(configSectionValue('elasticsearch')['port'],configSectionValue('elasticsearch')['host'])
    socketLisent()


if __name__ == "__main__":
        main()
