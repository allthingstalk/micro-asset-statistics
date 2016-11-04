__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

import datetime

from att_event_engine.when import When
from att_trusted_event_server.when_server import appendToMonitorList
from att_event_engine.resources import Sensor, Asset
from att_event_engine.timer import Timer
from att_event_engine.att import HttpClient
from dateutil.relativedelta import relativedelta
import os
import json

from statistician import Statistician


def getSec(str):
    """
    converts a string in the form 'year:month:week:day:hour:minute' into seconds
    :param str: the string to convert
    :return: nr of seconds
    """
    curTime = datetime.datetime.now()
    values = str.split(':')
    timeoffset = datetime.timedelta(days=int(values[3]),hours=int(values[4]),minutes=int(values[5]))

    nextTime = curTime + timeoffset + relativedelta(months=int(values[1]) * int(values[0]), weeks=values[2])
    result = nextTime - curTime
    return result.total_seconds()


def loadDefinition(name):
    with open(os.path.join('definitions', name)) as f:  # load the definitions for this asset
        return json.load(f)


@When([])
def calculateStatistics():
    """
    called when the asset fvalue has changed and the statistics need to be recalculated.
    :return:
    """
    current = Asset.current()
    definition = loadDefinition(current.id+".json")
    current.connection = HttpClient()
    current.connection.connect_api(definition['username'], definition['pwd'])    # create the connection -> this is a dynamic object, so we don't yet have the connection, can be for a different user.
    stats = AssetStats(definition, current.connection)
    for group in stats.groups:
        group.calculate(current)


@When([])
def resetGroup():
    """
    called when a timer has finished and it's time to reset the values related to that group.
    :return: None
    """
    timer = Timer.current()
    timer.group.resetValues()
    timer.set(getSec(timer.group.resetEvery))  # restart the timer


class AssetStats(object):
    """
    wraps a single asset statistics definition. This object contains all the groupings that are defined and which
     should be calculated whenever the value of the asset to be monitored, changes.
    """
    def __init__(self, definition, connection):
        """
        create the object
        :param definition: a json dict that contains the definition for the stats. (see examples in definitions dir)
        """
        self.asset = Sensor(definition['asset'], connection=connection)
        self.groups = []
        self.timers = []
        for group in definition['groups']:
            if 'reset' in group:
                reset = group['reset']
            else:
                reset = None
            stat = Statistician(group['name'], group['calculate'], reset, self.asset)
            self.groups.append(stat)
            if "reset" in group:
                timer = Timer(self.asset, group['name'])
                self.timers.append(timer)
                # wait a little with this, timer not yet active.
                # timer.set(getSec(group['reset']))
                timer.group = stat

    def register(self):
        appendToMonitorList(calculateStatistics, self.asset)
        map(lambda x: appendToMonitorList(resetGroup, x), self.timers)
