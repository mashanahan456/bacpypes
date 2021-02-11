import os
import requests
import json
import sys
import time
from bacpypes.core import run, deferred
from bacpypes.task import recurring_function
from bacpypes.basetypes import DateTime
from bacpypes.object import AnalogValueObject, DateTimeValueObject
from bacpypes.consolelogging import ConfigArgumentParser
from bacpypes.apdu import WhoIsRequest, IAmRequest
from bacpypes.errors import DecodingError
from bacpypes.app import BIPSimpleApplication
from bacpypes.local.device import LocalDeviceObject
from configparser import ConfigParser

config = ConfigParser()
config.read('BACpypes.ini')


def get_facilities_devices(facility_id):
    payload = ""
    headers = {
        'facID': facility_id,
    }
    response = requests.request("GET", url=config['api']['dev_endpoint'], data=payload, headers=headers)
    return json.loads(response.text)


def get_device_data(mac):
    headers = {
        'mac': mac,
        'start_ts': str(int(time.time() - (3600 * 5))),
        'end_ts': str(int(time.time()))
    }
    response = requests.request("POST", url=config['api']['data_endpoint'], headers=headers)
    last_data = json.loads(response.text)
    if len(last_data) > 0:
        return last_data[-1]
    return


def get_facility_average_data(facility_data):
    all_val_dict = dict()
    for mac, data in facility_data.items():
        for poll, val in data.items():
            if poll not in all_val_dict:
                all_val_dict[poll] = list()
            all_val_dict[poll].append(val)

    avg_dict = dict()
    for poll, val in all_val_dict.items():
        try:
            avg_dict[poll] = sum(val) / len(val)
        except TypeError:
            continue
    return avg_dict


def get_last_facility_data():
    facility_id = config['api']['facility_id']
    facility_data = {}
    all_devices = get_facilities_devices(facility_id)
    for device in all_devices:
        device_data = get_device_data(device)
        if device_data is None:
            continue
        if device not in facility_data or facility_data[device] != device_data:
            facility_data[device] = device_data
    facility_data['facility_average'] = get_facility_average_data(facility_data)
    return facility_data


# units go into the request
APPUNITS = os.getenv("APPUNITS", "imperial")

# application interval is refresh time in minutes
APPINTERVAL = int(os.getenv("APPINTERVAL", 1)) * 10 * 1000
objects = {}

config = ConfigParser()
config.read('BACpypes.ini')


class WhoIsIAmApplication(BIPSimpleApplication):

    def __init__(self, *args):
        BIPSimpleApplication.__init__(self, *args)

        # keep track of requests to line up responses
        self._request = None

    def process_io(self, iocb):
        # save a copy of the request
        self._request = iocb.args[0]

        # forward it along
        BIPSimpleApplication.process_io(self, iocb)

    def confirmation(self, apdu):
        # forward it along
        BIPSimpleApplication.confirmation(self, apdu)

    def indication(self, apdu):
        if (isinstance(self._request, WhoIsRequest)) and (isinstance(apdu, IAmRequest)):
            device_type, device_instance = apdu.iAmDeviceIdentifier
            if device_type != 'device':
                raise DecodingError("invalid object type")

            if (self._request.deviceInstanceRangeLowLimit is not None) and \
                    (device_instance < self._request.deviceInstanceRangeLowLimit):
                pass
            elif (self._request.deviceInstanceRangeHighLimit is not None) and \
                    (device_instance > self._request.deviceInstanceRangeHighLimit):
                pass
            else:
                # print out the contents
                sys.stdout.write('pduSource = ' + repr(apdu.pduSource) + '\n')
                sys.stdout.write('iAmDeviceIdentifier = ' + str(apdu.iAmDeviceIdentifier) + '\n')
                sys.stdout.write('maxAPDULengthAccepted = ' + str(apdu.maxAPDULengthAccepted) + '\n')
                sys.stdout.write('segmentationSupported = ' + str(apdu.segmentationSupported) + '\n')
                sys.stdout.write('vendorID = ' + str(apdu.vendorID) + '\n')
                sys.stdout.flush()

        # forward it along
        BIPSimpleApplication.indication(self, apdu)


def do_iam():
    """iam"""
    # code lives in the device service
    global this_application
    this_application.i_am()


class LocalAnalogValueObject(AnalogValueObject):
    def _set_value(self, value):
        self.presentValue = value


# timezone offset is shared with the date time values
timezone_offset = 0


class LocalDateTimeValueObject(DateTimeValueObject):
    def _set_value(self, utc_time):
        # convert to a time tuple based on timezone offset
        time_tuple = time.gmtime(utc_time + timezone_offset)

        # extra the pieces
        date_quad = (
            time_tuple[0] - 1900,
            time_tuple[1],
            time_tuple[2],
            time_tuple[6] + 1,
        )
        time_quad = (time_tuple[3], time_tuple[4], time_tuple[5], 0)

        date_time = DateTime(date=date_quad, time=time_quad)

        self.presentValue = date_time


all_facility_devices = get_facilities_devices(config['api']['facility_id'])
parameters = [
    ("$.indoor.facilityAvg.cotwo", LocalAnalogValueObject, "ppm"),
    ("$.indoor.facilityAvg.pmten", LocalAnalogValueObject, "ug/m^3"),
    ("$.indoor.facilityAvg.pmtwo", LocalAnalogValueObject, "ug/m^3"),
    ("$.indoor.facilityAvg.voc", LocalAnalogValueObject, "ppm"),
    ("$.indoor.facilityAvg.t", LocalAnalogValueObject, "celcius"),
    ("$.indoor.facilityAvg.h", LocalAnalogValueObject, "percent"),
    ("$.indoor.facilityAvg.mold", LocalAnalogValueObject, "score"),
    ("$.indoor.facilityAvg.covid", LocalAnalogValueObject, "score"),
    ("$.indoor.facilityAvg.productivity", LocalAnalogValueObject, "score"),
    ("$.indoor.facilityAvg.comfort", LocalAnalogValueObject, "score"),
    ("$.indoor.facilityAvg.asthmaRisk", LocalAnalogValueObject, "score"),
]
for mac in all_facility_devices:
    parameters.extend([
        ("$.indoor.{0}.cotwo".format(mac), LocalAnalogValueObject, "ppm"),
        ("$.indoor.{0}.pmten".format(mac), LocalAnalogValueObject, "ug/m^3"),
        ("$.indoor.{0}.pmtwo".format(mac), LocalAnalogValueObject, "ug/m^3"),
        ("$.indoor.{0}.voc".format(mac), LocalAnalogValueObject, "ppm"),
        ("$.indoor.{0}.t".format(mac), LocalAnalogValueObject, "celcius"),
        ("$.indoor.{0}.h".format(mac), LocalAnalogValueObject, "percent"),
        ("$.indoor.{0}.mold".format(mac), LocalAnalogValueObject, "score"),
        ("$.indoor.{0}.covid".format(mac), LocalAnalogValueObject, "score"),
        ("$.indoor.{0}.productivity".format(mac), LocalAnalogValueObject, "score"),
        ("$.indoor.{0}.comfort".format(mac), LocalAnalogValueObject, "score"),
        ("$.indoor.{0}.asthmaRisk".format(mac), LocalAnalogValueObject, "score"),
    ])


def create_objects(app):
    global objects
    next_instance = 1
    for parms in parameters:
        if len(parms) == 2:
            units = None
        elif len(parms) == 3:
            units = parms[2]
        elif APPUNITS == "metric":
            units = parms[3]
        elif APPUNITS == "imperial":
            units = parms[4]
        else:
            units = parms[2]

        obj = parms[1](
            objectName=parms[0], objectIdentifier=(parms[1].objectType, next_instance)
        )
        if units is not None:
            obj.units = units
        app.add_object(obj)
        objects[parms[0]] = obj
        next_instance += 1


def flatten(x, prefix="$"):
    """Turn a JSON object into (key, value) tuples using JSON-Path like names
    for the keys."""
    if type(x) is dict:
        for a in x:
            yield from flatten(x[a], prefix + "." + a)
    elif type(x) is list:
        for i, y in enumerate(x):
            yield from flatten(y, prefix + "[" + str(i) + "]")
    else:
        yield prefix, x


@recurring_function(APPINTERVAL)
def update_weather_data():
    global objects, timezone_offset
    this_application.i_am()
    facility_data = get_last_facility_data()
    json_response = {'indoor': {}}
    for mac, data in facility_data.items():
        for poll, val in data.items():
            if mac not in json_response['indoor']:
                json_response['indoor'][mac] = dict()
            json_response['indoor'][mac][poll] = val
        print("Ran Update")
        dict_response = dict(flatten(json_response))
        print(dict_response)
        timezone_offset = dict_response.get("$.timezone", 0)
        for k, v in dict_response.items():
            if k in objects:
                objects[k]._set_value(v)


# def main():
args = ConfigArgumentParser(description='BACpypes.ini').parse_args()
this_device = LocalDeviceObject(ini=args.ini)
this_application = BIPSimpleApplication(this_device, args.ini.address)
create_objects(this_application)
deferred(update_weather_data)
run()
