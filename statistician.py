__author__ = 'Jan Bogaerts'
__copyright__ = "Copyright 2016, AllThingsTalk"
__credits__ = []
__maintainer__ = "Jan Bogaerts"
__email__ = "jb@allthingstalk.com"
__status__ = "Prototype"  # "Development", or "Production"

import math
import datetime
import dateutil.parser
from att_event_engine.resources import Sensor, Actuator, Virtual, Gateway, Parameter
import urllib                                                           # to make certain that the name of the asset doesn't contain any wrong chars (from the name of the timer)

class Statistician:
    """
    performs all the statistical calculations for a single asset.
    """

    def __init__(self, name, functions, resetEvery, startDate, asset):
        """
        create object
        :param functions: a list of 'function' objects that this statistician has to calculate when a value is changed
        :param asset: an Asset object or id string that this statistician should calculate values for.
        """
        if isinstance(asset, basestring):
            self._asset = Sensor(asset)                                 # we treat it as a sensor, could also be an actuator.
        else:
            self._asset = asset
        self._name = name                                               # the name of the statistical group.
        self._functions = {}
        self.resetEvery = resetEvery                                    # so we can restart the timer.
        self.startDate = dateutil.parser.parse(startDate) if startDate else None
        for function in functions:
            name = function['function']
            self._functions[function['function']] = function            # so we can store some parameters, if there are any
            if name == "std":                                           # for std, we need avg
                if "avg" not in self._functions:
                    name = 'avg'                                        # change the name to avg so the next if works: for avg, we also need count.
                    self._functions[name] = None
            # this is correct: for std, we need avg and count
            if name == "avg" and "count" not in self._functions:        # if we need avg, then we need count to get the avg right.
                self._functions['count'] = None
            if name == "distprocent":  # for std%, we also need std
                if "dist" not in self._functions:
                    name = "dist"                                       # change the name to std, so we process the next 'if' statemtn (include std and it's subparts)
                    self._functions[name] = function                    # the parameters for std are the same as for distprocent
            if name == "distsumtimeprocent":                                  # for distsumtime%, we also need distsumtime
                if "distsumtime" not in self._functions:
                    name = "distsumtime"                                 # change the name to std, so we process the next 'if' statemtn (include std and it's subparts)
                    self._functions[name] = function                     # the parameters for std are the same as for distprocent
            if (name == "dist" or name == "distsumtime") and self._asset.profile['type'] != 'boolean':    # boolean dist does not need min or max
                if "min" not in self._functions and "min" not in function:      # if user specified min, don't need to calculate it.
                    self._functions['min'] = None
                if "max" not in self._functions and "max" not in function:      # if user specified max, don't need to calculate it.
                    self._functions['max'] = None


    def createAssets(self, context):
        """
        creates the assets that represent the values of the statistical functions.
        :param device: the device object or id to attach the assets too.
        :return: None
        """
        if 'count' in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('count'), self.getAssetLabel('count'), "generated by the statistician", "integer")
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('countHistory'), self.getAssetLabel('count history'), "generated by the statistician. count of previous time windows", "integer")
        if 'min' in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('min'), self.getAssetLabel('min'), "generated by the statistician", self._asset.profile)
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('minHistory'), self.getAssetLabel('min history'), "generated by the statistician. min of previous time windows", self._asset.profile)
        if 'max' in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('max'), self.getAssetLabel('max'), "generated by the statistician", self._asset.profile)
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('maxHistory'), self.getAssetLabel('max history'), "generated by the statistician. max of previous time windows", self._asset.profile)
        if 'avg' in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('avg'), self.getAssetLabel('avg'), "generated by the statistician", "number")
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('avgHistory'), self.getAssetLabel('avg history'), "generated by the statistician. avg of previous time windows", "number")
        if "std" in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('devSum'), self.getAssetLabel('devSum'), "generated by the statistician","number")
            Virtual.create(context, self._asset.device, self.getAssetName('std'), self.getAssetLabel('std'), "generated by the statistician", "number")
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('stdHistory'), self.getAssetLabel('std history'), "generated by the statistician. std of previous time windows", "number")
        if "dist" in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('dist'), self.getAssetLabel('dist'), "generated by the statistician",{"type": "array", "items":{"type": "integer" }})
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('distHistory'), self.getAssetLabel('dist history'), "generated by the statistician. dist of previous time windows", {"type": "array", "items":{"type": "integer" }})
        if "distprocent" in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('distprocent'), self.getAssetLabel('dist %'), "generated by the statistician. Distribution expressed in percentages", {"type": "array", "items": {"type": "number"}})
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('distprocentHistory'), self.getAssetLabel('dist % history'), "generated by the statistician. dist % of previous time windows", {"type": "array", "items": {"type": "number"}})
        if "distsumtime" in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('distsumtime'), self.getAssetLabel('dist sum time'), "generated by the statistician",{"type": "array", "items": {"type": "integer"}})
            Virtual.create(context, self._asset.device, self.getAssetName('distsumtimeprev'), self.getAssetLabel('dist sum time prev'), "generated by the statistician", {"type": "object", "properties": { "value": self._asset.profile, "timestamp": {"type": "string"}}})
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('distsumtimeHistory'), self.getAssetLabel('dist-sum-time history'), "generated by the statistician. dist sum time of previous time windows", {"type": "array", "items": {"type": "integer"}})
        if "distsumtimeprocent" in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('distsumtimeprocent'), self.getAssetLabel('distsumtime %'), "generated by the statistician. Distribution expressed in percentages", {"type": "array", "items": {"type": "number"}})
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('distsumtimeprocentHistory'), self.getAssetLabel('dist sum time % history'), "generated by the statistician. dist sum time % of previous time windows", {"type": "array", "items": {"type": "number"}})
        if "delta" in self._functions:
            Virtual.create(context, self._asset.device, self.getAssetName('deltaCurrentPeriod'), self.getAssetLabel('delta current period'), "generated by the statistician", self._asset.profile)
            Virtual.create(context, self._asset.device, self.getAssetName('deltaPrevTotal'), self.getAssetLabel('delta prev total'), "generated by the statistician. The value of the asset at the end of the previous period", self._asset.profile)
            if self.resetEvery:
                Virtual.create(context, self._asset.device, self.getAssetName('deltaHistory'), self.getAssetLabel('delta history'), "generated by the statistician. The deltas of the previous periods", self._asset.profile)
                Virtual.create(context, self._asset.device, self.getAssetName('deltaHistoryPrevTotal'),self.getAssetLabel('delta history previous total'), "generated by the statistician. The total value, at the end of the previous time group. Used to calcualte the history delta, when the period has ended", self._asset.profile)


    def getAssetLabel(self, functionName):
        return "{}-{}-{}".format(self._asset.name, self._name, functionName)

    def getAssetName(self, functionName):
        """
        builds the asset name for the asset that should be used for the specified function.
        :param functionName: a string, the name of the function
        :return: unique name for asset, function and group
        """
        return "{}-{}-{}".format(self._asset.name, self._name.replace(" ", "-"), functionName)


    def try_calculate_count(self, context):
        """
        calculate the count.
        """
        if 'count' in self._functions:
            cnt = Actuator(device=self._asset.device, name=self.getAssetName('count'), connection=self._asset.connection)
            prevVal = cnt.value
            if not prevVal:
                cnt.value = 1
                context['count'] = 1
            else:
                cnt.value = prevVal + 1
                context['count'] = prevVal + 1


    def try_calculate_min(self, value, context):
        if 'min' in self._functions:
            minAct = Actuator(device=self._asset.device, name=self.getAssetName('min'), connection=self._asset.connection)
            prevVal = minAct.value
            if prevVal == None or value < prevVal:
                minAct.value = value
                context['prev_min'] = prevVal
                context['min'] = value
            else:
                context['prev_min'] = prevVal
                context['min'] = prevVal


    def try_calculate_max(self, value, context):
        if 'max' in self._functions:
            maxAct = Actuator(device=self._asset.device, name=self.getAssetName('max'), connection=self._asset.connection)
            prevVal = maxAct.value
            if prevVal == None or value > prevVal:
                maxAct.value = value
                context['prev_max'] = prevVal
                context['max'] = value
            else:
                context['prev_max'] = prevVal
                context['max'] = prevVal

    def try_calculate_avg(self, value, context):
        if 'avg' in self._functions:
            avg = Actuator(device=self._asset.device, name=self.getAssetName('avg'), connection=self._asset.connection)
            avgVal = avg.value
            if avgVal == None:
                avg.value = value
                context['avg'] = value
            else:
                cntVal = context['count']
                val = avgVal - (avgVal / cntVal) + (float(value) / cntVal)
                avg.value = val
                context['prev_avg'] = avgVal
                context['avg'] = val


    def try_calculate_std(self, value, context):
        if "std" in self._functions:
            devSum = Actuator(device=self._asset.device, name=self.getAssetName('devSum'), connection=self._asset.connection)  # we use a helper actuator for this to store a midstage value
            if devSum.value == None:
                devSum.value = 0
            else:
                devSum.value += (value - context['avg'])
                std = Actuator(device=self._asset.device, name=self.getAssetName('std'), connection=self._asset.connection)
                context['prev_avg'] = std.value
                std.value = math.sqrt((devSum.value * devSum.value) / context['count'])
                context['std'] = std.value


    def try_calculate_dist(self, value, context):
        if "dist" in self._functions:
            distDef = self._functions['dist']
            dist = Actuator(device=self._asset.device, name=self.getAssetName('dist'),
                            connection=self._asset.connection)
            if isinstance(value, bool):
                index = 0 if value == False else 1
                result = dist.value
                if not result:
                    result = [0, 0]
            else:
                result, min = self.prepareDistList(dist.value, context, distDef)
                index = (value - min) / distDef['bucketsize']
            if index < len(result) and index >= 0:
                result[index] += 1
                dist.value = result                         # send the value back to the server.
                context['dist'] = result

    def prepareDistList(self, value, context, distDef):
        """
        prepare a list for the distribution functions: create all the slots
        :param value: the current list, if any
        :param context: the context (previously calcualted values)
        :param distDef: the definition object of the function parameters
        :return: the list, minimum value
        """
        if value == None:
            result = []
            min = context['min'] if 'min' not in distDef else distDef['min']
            max = context['max'] if 'max' not in distDef else distDef['max']
            for x in range(min, max + 1, distDef['bucketsize']):
                result.append(0)
        else:
            result = value
            if 'min' not in distDef:
                min = context['min']
                for x in range(min, context['prev_min'], distDef['bucketsize']):
                    result.insert(0, 0)
            else:
                min = distDef['min']
            if 'max' not in distDef:
                for x in range(context['prev_max'] + 1, context['max'] + 1, distDef['bucketsize']):
                    result.append(0)
        return result, min

    def try_calculate_dist_sum_time(self, asset, context):
        if "distsumtime" in self._functions:
            distDef = self._functions['distsumtime']
            dist = Actuator(device=self._asset.device, name=self.getAssetName('distsumtime'), connection=self._asset.connection)
            value = asset.value

            prevValAct = Actuator(device=self._asset.device, name=self.getAssetName('distsumtimeprev'), connection=self._asset.connection)
            prevVal = prevValAct.value
            newTime = asset.value_at
            if prevVal:
                prevDate = dateutil.parser.parse(prevVal['timestamp'])
                prevVal = prevVal['value']
                if isinstance(prevVal, bool):
                    index = 0 if prevVal == False else 1
                    result = dist.value
                    if not result:
                        result = [0, 0]
                else:
                    result, min = self.prepareDistList(dist.value, context, distDef)
                    index = (prevVal - min) / distDef['bucketsize']
                if index < len(result) and index >= 0:
                    timeDif = dateutil.parser.parse(newTime) - prevDate
                    result[index] += timeDif.total_seconds()
                    context['distsumtime'] = result
                dist.value = result  # send the value back to the server.
            prevValAct.value = {'value': value, 'timestamp': newTime}

    def try_calculate_delta(self, value, context):
        if "delta" in self._functions:
            deltaPrev = Actuator(device=self._asset.device, name=self.getAssetName('deltaPrevTotal'), connection=self._asset.connection)
            deltaCur = Actuator(device=self._asset.device, name=self.getAssetName('deltaCurrentPeriod'), connection=self._asset.connection)
            prevDelta = deltaPrev.value
            if prevDelta:
                deltaCur.value = value - deltaPrev.value
            deltaPrev.value = value
            context['delta'] = deltaCur.value

    def try_calculate_dist_percent(self, context):
        if "distprocent" in self._functions:
            res = Actuator(device=self._asset.device, name=self.getAssetName('distprocent'), connection=self._asset.connection)
            dist = context['dist']
            total = sum(dist)
            res.value = [(i * 100) / total for i in dist]
            context['distprocent'] = res.value

    def try_calculate_dist_sum_time_percent(self, context):
        if "distsumtimeprocent" in self._functions:
            res = Actuator(device=self._asset.device, name=self.getAssetName('distsumtimeprocent'), connection=self._asset.connection)
            dist = context['distsumtime']
            total = sum(dist)
            res.value = [(i * 100) / total for i in dist]
            context['distsumtimeprocent'] = res.value

    def calculate(self, asset):
        """
        updates  all the assets that contain the results of the functions that this statistician has to calculate.
        :param asset: the asset with the new value that arrived.
        :return:
        """
        context = {}
        self.try_calculate_count(context)
        self.try_calculate_min(asset.value, context)
        self.try_calculate_max(asset.value, context)
        self.try_calculate_avg(asset.value, context)
        self.try_calculate_std(asset.value, context)
        self.try_calculate_dist(asset.value, context)
        self.try_calculate_dist_percent(context)
        self.try_calculate_dist_sum_time(asset, context)
        self.try_calculate_dist_sum_time_percent(context)
        self.try_calculate_delta(asset.value, context)



    def resetValues(self):
        """
        resets all the values of the assets that this statistician feeds. This is called when
        a time period has passed.
        :return:
        """
        if 'count' in self._functions:
            cnt = Actuator(device=self._asset.device, name=self.getAssetName('count'), connection=self._asset.connection)
            cntHist = Actuator(device=self._asset.device, name=self.getAssetName('countHistory'), connection=self._asset.connection)
            cntHist.value = cnt.value
            cnt.value = 0
        if 'min' in self._functions:
            minAct = Actuator(device=self._asset.device, name=self.getAssetName('min'), connection=self._asset.connection)
            minActHist = Actuator(device=self._asset.device, name=self.getAssetName('minHistory'), connection=self._asset.connection)
            minAct.value = self._asset.value
        if 'max' in self._functions:
            maxAct = Actuator(device=self._asset.device, name=self.getAssetName('max'), connection=self._asset.connection)
            maxActHist = Actuator(device=self._asset.device, name=self.getAssetName('maxHistory'), connection=self._asset.connection)
            maxActHist.value = maxAct.value
            maxAct.value = self._asset.value
        if 'avg' in self._functions:
            avg = Actuator(device=self._asset.device, name=self.getAssetName('avg'), connection=self._asset.connection)
            avgHist = Actuator(device=self._asset.device, name=self.getAssetName('avgHistory'), connection=self._asset.connection)
            avgHist.value = avg.value
            avg.value = 0
        if "std" in self._functions:
            devSum = Actuator(device=self._asset.device, name=self.getAssetName('devSum'), connection=self._asset.connection)
            devSum.value = 0
            std = Actuator(device=self._asset.device, name=self.getAssetName('std'), connection=self._asset.connection)
            stdHist = Actuator(device=self._asset.device, name=self.getAssetName('stdHistory'), connection=self._asset.connection)
            stdHist.value = std.value
            std.value = 0
        if "dist" in self._functions:
            dist = Actuator(device=self._asset.device, name=self.getAssetName('dist'), connection=self._asset.connection)
            distHist = Actuator(device=self._asset.device, name=self.getAssetName('distHistory'), connection=self._asset.connection)
            distHist.value = dist.value
            dist.value = []
        if "distprocent" in self._functions:
            dist = Actuator(device=self._asset.device, name=self.getAssetName('distprocent'), connection=self._asset.connection)
            distHist = Actuator(device=self._asset.device, name=self.getAssetName('distprocentHistory'), connection=self._asset.connection)
            distHist.value = dist.value
            dist.value = []
        if "distsumtime" in self._functions:
            distSumPrev = Actuator(device=self._asset.device, name=self.getAssetName('distsumtimeprev'), connection=self._asset.connection)  # we use a helper actuator for this to store a midstage value
            distSumPrev.value = {"value": None, "timestamp": None}
            distSum = Actuator(device=self._asset.device, name=self.getAssetName('distsumtime'), connection=self._asset.connection)
            distSumHist = Actuator(device=self._asset.device, name=self.getAssetName('distsumtimeHistory'), connection=self._asset.connection)
            distSumHist.value = distSum.value
            distSum.value = []
        if "distsumtimeprocent" in self._functions:
            dist = Actuator(device=self._asset.device, name=self.getAssetName('distsumtimeprocent'), connection=self._asset.connection)
            dist.value = []
        if 'delta' in self._functions:
            deltaHist = Actuator(device=self._asset.device, name=self.getAssetName('deltaHistory'), connection=self._asset.connection)
            deltaHistPrevTotal = Actuator(device=self._asset.device, name=self.getAssetName('deltaHistoryPrevTotal'), connection=self._asset.connection)
            deltaHist.value = self._asset.value - deltaHistPrevTotal.value
            deltaHistPrevTotal.value = self._asset.value
