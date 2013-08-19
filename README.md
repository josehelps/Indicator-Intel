Indicator-Intel
===============

Indicator Intel Framework

NAME
    gatherer

DESCRIPTION
    # Ilastic receiver, gets data from clients
    #TODO:
    #help functions and arguments
    #parser for ilastic input configs and feeds, logging, error handling, better
    #Error Handling on: reading config, lisenting to socket, fail to connect to elasticsearch server
    #doc

FUNCTIONS
    configSectionValue(section)
        helper function to recall config values
    
    elasticsearch(es_port, es_host)
    
    help()
        TODO:
            help functions and arguments
            parser for ilastic input configs and feeds, logging, error handling, better
            Error Handling on: reading config, lisenting to socket, fail to connect to elasticsearch server
            documentation
        
        Usage:
        
        
        Example:
    
    identify(index)
    
    main()
        Main function, calls other functions
    
    mapping(index)
    
    parseFeeds(feed_file='feed.conf')
        helps parse the feed file defined under configuration
    
    scheduler(feeds)
        Takes in a feed dictionary, schedules cron jobs under the user which ran the app
    
    socketLisent(host='10.0.0.5', port='9999')
        Configures UDP socket to lisent for data to be send to Elasticsearch
        Also INITs index in elasticsearch
    
    writeData2ES(data)
        Writes data received on UDP socket to elasticsearch server
    
    writeFeed2ES()
        Writes Feeds to Elasticsearch

DATA
    ch = <logging.StreamHandler object>
    config = <ConfigParser.RawConfigParser instance>
    configPath = 'gatherer.conf'
    cron = <crontab.CronTab object>
    fh = <logging.FileHandler object>
    formatter = <logging.Formatter object>
    logger = <logging.Logger object>
    net_prefix_re = <_sre.SRE_Pattern object>
    sections = ['server', 'input', 'input_confidence', 'input_score_modifi...
    splitter = '---'


