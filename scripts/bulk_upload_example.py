#!/usr/bin/env python

''' example script to bulk upload measurements'''

from datetime import datetime, timedelta
import sys
import os

from squacapi_client.models.write_only_measurement_serializer \
    import WriteOnlyMeasurementSerializer

from squacapi_client.pnsn_utilities \
    import get_client, make_channel_map, make_metric_map, perform_bulk_create

# how many measurements to post per request
WRITE_CHUNK = 20

try:
    USER = os.environ['SQUACAPI_USER']
    PASSWORD = os.environ['SQUACAPI_PASSWD']

except KeyError:
    sys.exit("Requires ENV vars SQUACAPI_USER, SQUACAPI_PASSWD and HOST")

# create API client
client = get_client(USER, PASSWORD)

'''retrieve all metrics. Use params to filter:
    example: client.v1_0_measurement_metrics_list(name='some_metric')
'''
metrics = client.v1_0_measurement_metrics_list(name='rawrms')

'''retrieve all channels. Use params to filter:
    example:
    client.v1_0_measurement_channels_list(network='cc',channel=hhz,bhz)
'''
channels = client.v1_0_nslc_channels_list(network='uw', channel='hnn')
 
# # create lookup maps for fk relation
metric_map = make_metric_map(metrics)
channel_map = make_channel_map(channels)

# TEST DATA
################
now = datetime.utcnow()
starttime = datetime(now.year, now.month, now.day, 0, 0, 0)
starttime = starttime - timedelta(days=365)
endtime = datetime(now.year, now.month, now.day, 1, 0, 0)
endtime = endtime - timedelta(days=365)

# Keys for creating test data.
metric_keys = list(metric_map.keys())
channel_keys = list(channel_map.keys())

#################
# END TEST DATA


# create a test collection of measurements for bulk uploading
measurements = []
for ckey in channel_keys:
    print(ckey)
    for mkey in metric_keys:
        metric = metric_map[mkey]
        channel = channel_map[ckey]
        '''Create a WriteOnlyMeasurmentSerializer for each measurment
           in practice, the mkey and ckey will be derived from your data.
        '''

        measurement = WriteOnlyMeasurementSerializer(
            metric=metric_map[mkey],
            channel=channel_map[ckey],
            value=7.5,
            starttime=starttime,
            endtime=endtime
        )
        # add each WriteOnlyMeasurementSerialzer to collection
        measurements.append(measurement)
''' Once all WriteOnlyMeasurementSerializers are added to measurements
    update in 'chunks' '''
response, errors = perform_bulk_create(measurements, client,
                                       chunk=WRITE_CHUNK)
# example of error response
for error in errors:
    error_str = (
        f"Reason: {error['reason']}\n"
        f"Error: {error['body']}\n"
        f"HTTPStatus: {error['status']}\n"
        f"Headers: {error['headers']}"
    )
    print(error_str)
