import requests
import re

__pciids = {}

def __get_ids():
    global __pciids

    if not __pciids:
        __get_data_from_pciids()
        __get_data_from_pcidatabase()
        __get_data_byhand()

    return __pciids


def __get_data_from_pciids():
    global __pciids

    r = requests.get('https://raw.githubusercontent.com/pciutils/pciids/master/pci.ids')

    data = r.text.split('\n')

    # Syntax:
    # vendor  vendor_name
    # device  device_name				<-- single tab
    #		subvendor subdevice  subsystem_name	<-- two tabs
    for line in data:
        if line and not line.startswith('#'):
            if not line.startswith('\t'): # vendor  vendor_name
                line = line.strip()
                pos = line.index('  ')
                vendorID = line[:pos]
                vendorName = line[(pos + 2):]
                devices = {}
                __pciids[vendorID] = { 'name': vendorName, 'devices': devices }
            elif not line.startswith('\t\t'): # device  device_name
                line = line.strip()
                pos = line.index('  ')
                deviceID = line[:pos]
                deviceName = line[(pos + 2):]
                devices[deviceID] = deviceName
        elif line and line == '# List of known device classes, subclasses and programming interfaces':
            break

def __get_data_from_pcidatabase():
    global __pciids

    r = requests.get('http://pcidatabase.com/reports.php?type=csv')

    data = r.text.split('\n')

    #"0x0033","0x002F","Paradyne Corp.","MAIAM","Spitfire VGA Accelerator"
    pat = re.compile('\"0x([0-9a-fA-F]{4})\",\"0x([0-9a-fA-F]{4})\",\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\"')

    for line in data:
        if line:
            for m in pat.finditer(line):
                vendorID = m.group(1).lower()
                deviceID = m.group(2).lower()
                vendorName = m.group(3)
                desc1 = m.group(4)
                desc2 = m.group(5)
                desc = desc2 if desc2 else desc1
                desc = desc if desc else '%s Unknown' % vendorName 
                if vendorID in __pciids:
                    devices = __pciids[vendorID]['devices']
                    if deviceID not in devices:
                        devices[deviceID] = desc
                else:
                    __pciids[vendorID] = { 'name': vendorName, 'devices': { deviceID: desc } }

def __get_data_byhand():
    global __pciids

    __pciids['8086']['devices']['22b1'] = 'Intel(R) HD Graphics'
    __pciids['8086']['devices']['0d22'] = 'Intel(R) Iris(TM) Pro Graphics 5200'
    __pciids['8086']['devices']['22b0'] = 'Intel(R) HD Graphics'
    __pciids['8086']['devices']['4102'] = 'Intel(R) Graphics Media Accelerator 600'
    __pciids['1022']['devices']['68f9'] = 'AMD Radeon Graphics Processor'
    __pciids['1002']['devices']['6920'] = 'AMD RADEON R9 M395X'
    __pciids['1002']['devices']['714f'] = 'RV505'
    __pciids['10de']['devices']['0a6b'] = 'NVIDIA 9400 GT 512 MB BIOS'
    __pciids['1414']['devices']['02c1'] = 'Microsoft RemoteFX Graphics Device - WDDM'
    __pciids['1414']['devices']['008c'] = 'Microsoft Basic Render Driver'
    __pciids['1414']['devices']['fefe'] = '0xfefe'

def __mk_id(pciid):
    pciid = pciid.lower()

    if pciid.startswith('0x'):
        pciid = pciid[2:]

    l = len(pciid)

    if l < 4:
        return '0' * (4 - l) + pciid

    if l == 4:
        return pciid.replace(' ', '0')

    return ''

def get_vendor_name(vendorID):
    vendor = __get_ids().get(__mk_id(vendorID), None)
    if vendor:
        return vendor['name']
    return vendorID

def get_device_name(vendorID, deviceID):
    vendor = __get_ids().get(__mk_id(vendorID), None)
    if vendor:
        desc = vendor['devices'].get(__mk_id(deviceID), None)
        if desc:
            return desc

    return deviceID
