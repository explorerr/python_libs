'''
Created on Jun 27, 2017

@author: hirschs
'''

import requests
import pandas as pd
from datetime import datetime
from dateutil import tz
import calendar
import sys
# import json

class openTSDBClient(object):
    '''
    classdocs
    '''
    
    utc_tz = tz.gettz('UTC')

    # Pass optional tzname arg to work in a local timezone. Default is to assume
    # that both query bounds and result are UTC
    #
    def __init__(self, host, port, tzname='UTC', tsid='timestamp'):
        self.url = "%s:%d/api/query" % (host, port)
        self.tzname = tzname
        self.tsid = tsid
        if self.tzname != 'UTC':
            self.local_tz = tz.gettz(self.tzname)
        else:
            self.local_tz = None
            
    def getMetricRange(self,metric,start,end,pkey,tag_filter={},aggregator='none',downsample=None):
        query = {
            "start": start,
            "end": end,
            "timezone": self.tzname,
            "queries": [
                    {
                        "aggregator": aggregator,
                        "metric": metric,
                        "tags": tag_filter
                    }
                ]
            }
        if downsample != None:
            query['queries'][0]['downsample'] = downsample
            # If downsampling in local time zone, make it calendar relative
            if self.local_tz != None:
                query['useCalendar'] = True
            
        # print(json.dumps(query,indent=4))
        retries = 0
        while True:
            try:
                rsp = requests.post(self.url, json=query).json()
                break
            except Exception as e:
                if retries < 5:
                    print("WARNING: Web service request exception:" + str(e))
                    retries += 1
                else:
                    print("ERROR:  Retry count exceeded. Exiting")
                    sys.exit(1)
        # print(json.dumps(rsp,indent=4))

        dfs = []
        cols = [self.tsid]
        cols.extend(pkey)
        cols.append(metric)
        
        # Iterate over subquery responses and build a list of data frames
        for s in rsp:
            data = []
            key_cols = []
            for col in pkey:
                if col in s['tags']:
                    key_cols.append(s['tags'][col])
                else:
                    key_cols.append(None)

            dps = s['dps']
            for key, value in dps.iteritems():
                ticks = int(key)
                if self.local_tz != None:
                    # Transform from UTC to local timezone
                    localtime = datetime \
                        .fromtimestamp(ticks, self.utc_tz) \
                        .astimezone(self.local_tz) \
                        .replace(tzinfo=None)
                    # and then to ticks seconds
                    ticks = calendar.timegm(localtime.timetuple()) 
                # print("%s %d %f" % (assetid,ticks,value))
                row = [ticks]
                row.extend(key_cols)
                row.append(value)
                data.append(row)

            df = pd.DataFrame(data, columns=cols)
            # Coerce the value to a numeric (handles case of "NaN" as string)
            df[metric] = df[metric].apply( pd.to_numeric, errors='ignore')
            dfs.append( df )
            
        # Make sure that we return a valid, empty dataframe if query
        # finds nothing
        if len(dfs) == 0:
            return pd.DataFrame([], columns=cols)
            
        sort_key = [self.tsid]
        sort_key.extend(pkey)
        # Concatenate all results into a single frame, sort it and
        # reset the row index
        return pd.concat(dfs, ignore_index=True) \
            .sort_values(by=sort_key) \
            .reset_index(drop=True)
            
    def getMetricRangeMulti(self,metrics,start,end,pkey,downsample,tag_filter={},aggregator='none'):
        first = True
        join_key = [self.tsid]
        join_key.extend(pkey)
        
        for metric in metrics:
            df = self.getMetricRange( metric=metric, 
                                      start=start, 
                                      end=end,
                                      pkey=pkey, 
                                      tag_filter=tag_filter,
                                      aggregator=aggregator,
                                      downsample=downsample)
            if first:
                result = df
                first = False
            else:
                # Left outer join on assetid and timestamp
                result = pd.merge(result,df,on=join_key,how='left')

        return result
        
if __name__ == "__main__":
    # import sys
    
    # Widen the display to avoid column wrapping
    pd.set_option('display.width', 180)

    obj = openTSDBClient(host='http://hamburg-hdp-n1.utopusinsights.com', port=4242)

    df1 = obj.getMetricRange( metric='HypercastPower', 
                              start='2017/06/29 00:00:00', 
                              end='2017/07/20 00:00:00', 
                              tag_filter={'farm_name':'WP16'},
                              pkey=['farm_name','prediction_time'])
    print(df1)

    df1 = obj.getMetricRange( metric='ActivePower', 
                              start='2d-ago', 
                              end='now', 
                              tag_filter={'farm_name':'WP4'},
                              pkey=['farm_name','prediction_time'],
                              downsample='15m-avg-nan',
                              aggregator='avg')
    print(df1)

    df1 = obj.getMetricRangeMulti( metrics=['ActivePower','HypercastPower'], 
                                   start='2d-ago', 
                                   end='now', 
                                   tag_filter={'farm_name':'WP4'},
                                   pkey=['farm_name','prediction_time'],
                                   downsample='15m-avg-nan',
                                   aggregator='avg')
    print(df1)

    # Operate in IST localtime
    obj = openTSDBClient(host='http://cdm-hdp-n1.utopusinsights.com', port=4245, tzname='Asia/Kolkata')

    # Focus on a subset of turbines, one metric
    df1 = obj.getMetricRange( metric='kudle.active_pwr', 
                              start='2016/03/01 00:00:00', 
                              end='2016/03/02 00:00:00', 
                              pkey=['assetId'],
                              tag_filter={'assetId':'114|115'},
                              downsample='1d-avg')
    print(df1)

    # Now do the same thing in UTC
    obj = openTSDBClient(host='http://cdm-hdp-n1.utopusinsights.com', port=4245)
    df1 = obj.getMetricRange( metric='kudle.active_pwr', 
                              start='2016/03/01 00:00:00', 
                              end='2016/03/02 00:00:00', 
                              pkey=['assetId'],
                              tag_filter={'assetId':'114|115'},
                              downsample='1d-avg')
    print(df1)

    # All turbines with multiple metrics (downsampling required to be guaranteed alignment)
    df2 = obj.getMetricRangeMulti( metrics=['kudle.gen_sp','kudle.active_pwr','WindSpeed','kudle.nacelle_pos'], 
                                   start='2016/01/01 00:00:00', 
                                   end='2016/02/01 00:00:00', 
                                   pkey=['assetId'],
                                   downsample='10m-avg')
    print(df2)

