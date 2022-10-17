#
# Copyright (c) 2022, Massachusetts Institute of Technology All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# Redistributions in binary form must reproduce the above copyright notice, this
# list of conditions and the following disclaimer in the documentation and/or
# other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

import traceback
import MDSplus
import numpy as np
import socket
import threading
import time
import queue

from http.server import HTTPServer, BaseHTTPRequestHandler
from functools import partial

class DeviceSetup:

    def __init__(self, device):
        self.device = device

        css = '''
        html { font-family: sans-serif; }
        '''

        self.html = f'''
            <style>{css}</style>
            <h1>{self.device.__class__.__name__} Setup</h1>
            <div>
                Path: {self.device.path} <br />
                Tree: {self.device.tree.name} <br />
                Shot: {self.device.tree.shot} <br />
                {'Open for Edit' if self.device.tree.open_for_edit else ''}
            </div>
        '''

    def _generate_html_input_for_node(self, node):
        html = f'''
        <input type="{asdf}" />
        '''

    class HTTPRequestHandler(BaseHTTPRequestHandler):
        def __init__(self, setup, *args, **kwargs):
            self.html = setup.html
            super().__init__(*args, *kwargs)

        def do_HEAD(self):
            self.send_response(200)

        def do_GET(self):
            if self.path == '/':
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()
                self.wfile.write(self.html.encode())

        def do_POST(self):
            pass

    def run(self):
        handler = partial(self.HTTPRequestHandler, self)

        # server = HTTPServer(('', 0), self.get_handler)
        server = HTTPServer(('', 8000), handler)
        print('http://%s:%d' % server.server_address)

        server.serve_forever()


class ACQ2106(MDSplus.Device):
    '''
    # ACQ2106 Device Driver

    ## Configuring
    
    When you first add this driver to your tree, it is incomplete. You must do some initial configuration and then call the `configure` method.
    It is recommended to configure Online, meaning you can connect to your digitizer from the computer you are calling `configure` on. If you
    are not able to do so, you can configure Offline, but you must configure Online at least once before the driver can be used.

    ### Modes

    Before configuring, you must choose an operational mode. The default is `STREAM`. The mode controls how the device captures data, and what
    nodes get added to the tree for configuration.

    #### STREAM

    Data will stream continuously, storing data in segments of `STREAM:SEG_LENGTH` samples until either the `stop_stream` method is called, or
    the number of segments reaches `STREAM:SEG_COUNT`. In order to allow enough time to write each segment to disk, try experimenting with the
    value of `STREAM:SEG_LENGTH` until the writer thread is able to keep up. With `DEBUG_DEVICES >= 4` a message will be displayed every time
    the writer thread can't keep up, indicating that a new buffer was allocated. Every time a segment of data is written, an MDSplus Event with
    the name stored in `STREAM:EVENT_NAME` will be sent.

    Start with the `start_stream` method, and stop with the `stop_stream` method.
    If a soft trigger is configured, the data will start streaming immediately, otherwise it will wait for a trigger signal.

    #### TRANSIENT

    Data will be stored in a ring-buffer on the digitizer until a trigger occurs. At that point, the digitizer will keep `TRANSIENT:PRESAMPLES`
    samples and capture an additional `TRANSIENT:POSTSAMPLES` samples, and then write those segments to disk. This will trigger an MDSplus Event
    with the name stored in `TRANSIENT:EVENT_NAME`.

    Arm with the `arm_transient` method, and store with the `store_transient` method.
    If a soft trigger is configured, you can trigger it with the `soft_trigger` method, otherwise it will wait for a trigger signal.

    ### Online

    At a minimum, you must fill in the following nodes:
    * `MODE` must contain the intended operation mode, see Modes for a list of available modes.
    * `ADDRESS` must contain the DNS Name or IP Address of the digitizer.
    * `EPICS_NAME` must contain the EPICS name of the digitizer, usually the local hostname in the form of `ac2106_xyz`.

    Then, you must call the configure method, and the driver will contact the device to query what modules are installed and what functionalities
    are available.

    For example:
    ```
    TCL> edit TREE_NAME
    TCL> put DEVICE_NODE:MODE """STREAM"""
    TCL> put DEVICE_NODE:ADDRESS """192.168.0.100"""
    TCL> put DEVICE_NODE:EPICS_NAME """acq2106_100"""
    TCL> do /method DEVICE_NODE configure
    ```
    or
    ```py
    tree = MDSplus.Tree('EDIT', -1)
    tree.DEVICE_NODE.MODE.record = "STREAM"
    tree.DEVICE_NODE.ADDRESS.record =  "192.168.0.100"
    tree.DEVICE_NODE.EPICS_NAME.record =  "acq2106_100"
    tree.DEVICE_NODE.configure()
    ```

    ### Offline

    When configuring offline, you need to give the `configure` method most of the information it would otherwise query from the digitizer.
    At a minimum, you must fill in the following nodes:
    * `MODE` must contain the intended operation mode, see Modes for a list of available modes.
    * `MODULES` must contain a comma-separated list of the model names of the installed modules in the form of SiteNumber=ModelName.
      e.g. 1=ACQ423ELF,3=ACQ423ELF,6=DIO482 has ACQ423's in sites 1 and 3, and a DIO482 in site 6

    For example:
    ```
    TCL> edit TREE_NAME
    TCL> put DEVICE_NODE:MODE """STREAM"""
    TCL> put DEVICE_NODE:MODULES """1=ACQ423ELF 3=ACQ423ELF 6=DIO482"""
    TCL> do /method DEVICE_NODE configure # TODO: has_wr=True
    ```
    or
    ```py
    tree = MDSplus.Tree('EDIT', -1)
    tree.DEVICE_NODE.MODE.record = "STREAM"
    tree.DEVICE_NODE.MODULES.record = "1=ACQ423ELF 3=ACQ423ELF 6=DIO482"
    tree.DEVICE_NODE.configure()
    ```

    ## White Rabbit Trigger Distribution (WRTD)

    TODO:

    ## Setup
    The setup form for this driver is web-based. To access it, simply call the `setup` method and then open the resulting link.
    When you are done, simply Control+C the command or click Exit in the setup form.

    For example:
    ```
    TCL> set tree TREE_NAME
    TCL> do /method DEVICE_NODE setup
    http://localhost:54321
    ```
    or
    ```py
    tree = MDSplus.Tree('TREE_NAME', -1)
    tree.DEVICE_NODE.setup()
    http://localhost:54321
    ```

    ## Logging

    The environment variable `DEBUG_DEVICES` is used to control the verbosity of the logging. Set it to a number based on the list below to enable certain levels of logging.
    * `DEBUG_DEVICES <= 0` or not set will show no messages
    * `DEBUG_DEVICES >= 1` will show error messages
    * `DEBUG_DEVICES >= 2` will show warning messages
    * `DEBUG_DEVICES >= 3` will show info messages
    * `DEBUG_DEVICES >= 4` will show verbose messages

    '''

    ###
    ### Constants
    ###

    _MAX_SITES = 6
    """The maximum number of sites supported by this chasis"""

    _MAX_SPAD = 8
    """The maximum number of scratchpad (SPAD) channels that can be configured"""

    _MODE_OPTIONS = [
        'STREAM',
        'TRANSIENT',
        'SLOWFAST',
    ]
    """
    Operation Mode options, this controls what nodes are added during configure and what methods are available
    * STREAM: Streaming
    * TRANSIENT: Transient
    * SLOWFAST: Slow Monitor + Multi-Event
    """

    _TRIGGER_SOURCE_D0_OPTIONS = [
        'EXT',
        'HDMI',
        'GPG0',
        'WRTT0',
    ]
    """
    Trigger Source options for Signal Highway d0
    * EXT: External Trigger
    * HDMI: HDMI Trigger
    * GPG0: Gateway Pulse Generator Trigger
    * WRTT0: White Rabbit Trigger
    """

    _TRIGGER_SOURCE_D1_OPTIONS = [
        'STRIG',
        'HDMI_GPIO',
        'GPG1',
        'FP_SYNC',
        'WRTT1',
    ]
    """
    Trigger Source options for Signal Highway d1
    * STRIG: Software Trigger
    * HDMI_GPIO: HDMI General Purpose I/O Trigger
    * GPG1: Gateway Pulse Generator Trigger
    * FP_SYNC: Front Panel SYNC port
    * WRTT1: White Rabbit Trigger
    """

    _SYNC_ROLE_OPTIONS = [
        'master',
        'slave',
        'solo',
        'fpmaster',
        'rpmaster',
    ]
    """
    Synchronization Role options, this controls where the clock should be taken from
    * master: Master
    * slave: Slave
    * solo: Solo
    * fpmaster: Front-Panel Master
    * rpmaster: Rear-Panel Master
    """

    _MONITOR_DELAY_SECONDS = 30
    """The delay in seconds between each record made by the Monitor"""

    ###
    ### Parts
    ###

    parts = [
        {
            'path': ':COMMENT',
            'type': 'text',
            'options': ('no_write_shot',),
        },
        {
            'path': ':MODE',
            'type': 'text',
            'value': 'STREAM',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'The intended mode of operation.',
                'values': _MODE_OPTIONS,
            },
        },
        {
            'path': ':MODULES',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Comma-Separated list of modules, specified by model name or nothing if no module is present.',
            },
        },
        {
            'path': ':ADDRESS',
            'type': 'text',
            'value': '192.168.0.254',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'IP Address or DNS Name of the digitizer.',
            },
        },
        {
            'path': ':EPICS_NAME',
            'type': 'text',
            'value': 'acq2106_xxx',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'EPICS Name of the digitizer, usually the Hostname.',
            },
        },
        {
            'path': ':SERIAL',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Serial number of the ACQ2106, queried during configure().',
            },
        },
        {
            'path': ':FIRMWARE',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Firmware version of the ACQ2106, queried during configure().',
            },
        },
        {
            'path': ':FPGA_IMAGE',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'FPGA Image version of the ACQ2106, queried during configure().',
            },
        },
        {
            'path': ':SYNC_ROLE',
            'type': 'text',
            'value': 'master',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Used to determine what the clock is synchronized with/to.',
                'values': _SYNC_ROLE_OPTIONS,
            },
        },
        {
            'path': ':FREQUENCY',
            'type': 'numeric',
            'value': 20000,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Sample Frequency in Hertz',
            },
        },
        {
            'path': ':RUNNING',
            'type': 'numeric',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'On if running, Off otherwise.',
            },
        },
        {
            'path': ':TRIG_TIME',
            'type': 'numeric',
            'options': ('write_shot',),
            'ext_options': {
                'tooltip': 'Trigger Time', # TODO:
            },
        },
        {
            'path': ':TRIG_SOURCE',
            'type': 'text',
            'value': 'EXT',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Trigger Source, options will decided if the timing highway is d0 or d1. For a soft trigger use STRIG, and for a hard trigger use EXT.',
                'values': _TRIGGER_SOURCE_D0_OPTIONS + _TRIGGER_SOURCE_D1_OPTIONS,
            },
        },
        {
            'path': ':HARD_DECIM',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Hardware Decimation (NACC). Computed on the digitizer by averaging every N samples together. 1 = Disabled.',
                'values': [ 1, 2, 4, 8, 16, 32 ], # TODO: Are only powers of two allowed?
            },
        },
        {
            'path': ':SOFT_DECIM',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default Software Decimation, can be overridden per-input. Computed on the server by discarding every N-1 samples. 1 = Disabled.',
                'min': 1,
            },
        },
        {
            'path': ':RES_FACTOR',
            'type': 'numeric',
            'value': 100,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default Factor for Resampling, can be overridden per-input. 1 = Disabled.',
                'min': 1,
            },
        },
        {
            'path': ':TEMP',
            'type': 'signal',
            'ext_options': {
                'tooltip': 'Temperature of the main board in celsius.',
            },
        },
        {
            'path': ':TEMP_FPGA',
            'type': 'signal',
            'ext_options': {
                'tooltip': 'Temperature of the FPGA in celsius.',
            },
        },
        {
            'path': ':SCRATCHPAD',
            'type': 'structure',
        },
        {
            'path': ':SCRATCHPAD:SPAD0',
            'type': 'signal',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'If present, this scratchpad field will contain the sample number.',
            },
        },
        {
            'path': ':SCRATCHPAD:SPAD1',
            'type': 'signal',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'If present, this scratchpad field will part of the TAI timestamp.',
            },
        },
        {
            'path': ':SCRATCHPAD:SPAD2',
            'type': 'signal',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'If present, this scratchpad field will part of the TAI timestamp.',
            },
        },
    ]

    for i in range(3, _MAX_SPAD):
        parts.append({
            'path': ':SCRATCHPAD:SPAD%d' % (i,),
            'type': 'signal',
            'options': ('no_write_model',),
        })

    stream_parts = [
        {
            'path': ':STREAM',
            'type': 'structure',
        },
        {
            'path': ':STREAM:SEG_LENGTH',
            'type': 'numeric',
            'value': 8000,
            'options': ('write_model', 'write_shot',),
            'ext_options': {
                'tooltip': 'Segment Length, number of samples to take before calling makeSegment().',
                'min': 1024,
            },
        },
        {
            'path': ':STREAM:SEG_COUNT',
            'type': 'numeric',
            'value': 1000,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Segment Count, number of segments to capture before stopping. This will need to be a common denominator of all the decimations. If not, it will be automatically adjusted and updated in the tree.',
                'min': 1,
            },
        },
        {
            'path': ':STREAM:EVENT_NAME',
            'type': 'text',
            'value': 'STREAM',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Name of the event generated whenever a segment is captured.',
            },
        },
    ]

    transient_parts = [
        {
            'path': ':TRANSIENT',
            'type': 'structure',
        },
        {
            'path':':TRANSIENT:PRESAMPLES',
            'type':'numeric',
            'value': 10000,
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Number of samples to capture before the trigger.',
            },
        },
        {
            'path':':TRANSIENT:POSTSAMPLES',
            'type':'numeric',
            'value': 10000,
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Number of samples to capture after the trigger.',
            },
        },
        {
            'path':':TRANSIENT:EVENT_NAME',
            'type':'text',
            'value': 'TRANSIENT',
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Name of the event generated when the pre and post samples are stored.',
            },
        },
    ]

    wrtd_parts = [
        {
            'path': ':WRTD',
            'type': 'structure',
            'options': ('no_write_shot',)
        },
        {
            'path': ':WRTD:NS_PER_TICK',
            'type': 'numeric',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'The nanoseconds per tick of the clock, queried from the digitizer.',
            },
        },
        {
            'path': ':WRTD:TX_MESSAGE',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Message to send with WRTD when triggered.',
            },
        },
        {
            'path': ':WRTD:TX_DELTA_NS',
            'type': 'numeric',
            'value': 50000000,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Time in nanoseconds between when the WRTD message is sent, and when the trigger it describes should happen.',
                'min': 50000000,
            },
        },
        {
            'path': ':WRTD:RX0_FILTER',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Comma-Separated list of messages that will trigger WRTT0.',
            },
        },
        {
            'path': ':WRTD:RX1_FILTER',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Comma-Separated list of messages that will trigger WRTT1.',
            },
        },
    ]

    def _get_parts_for_site(self, site, model):
        site_path = ':SITE%d' % (site,)
        parts = [
            {
                'path': site_path,
                'type': 'structure'
            },
            {
                'path': site_path + ':MODEL',
                'type': 'text',
                'ext_options': {
                    'tooltip': 'Model of the card in this site.',
                },
            },
            {
                'path': site_path + ':SERIAL',
                'type': 'text',
                'ext_options': {
                    'tooltip': 'Serial Number of the card in this site.',
                },
            },
            {
                'path': site_path + ':TEMP',
                'type': 'signal',
                'ext_options': {
                    'tooltip': 'Temperature of the card in this site in celsius.',
                },
            },
        ]

        if model == 'ACQ435ELF' or model == 'ACQ423ELF':
            for i in range(32):
                input_path = site_path + ':INPUT_%02d' % (i + 1,)
                parts += [
                    {
                        'path': input_path,
                        'type': 'signal',
                        'valueExpr': 'head._set_input_segment_scale(node, node.COEFFICIENT, node.OFFSET)',
                    },
                    {
                        'path': input_path + ':RESAMPLED',
                        'type': 'signal',
                        'valueExpr': 'head._set_input_segment_scale(node, node.parent.COEFFICIENT, node.parent.OFFSET)',
                        'ext_options': {
                            'tooltip': 'Data for this input, resampled with makeSegmentResampled(RES_FACTOR).',
                        },
                    },
                    {
                        'path': input_path + ':RES_FACTOR',
                        'type': 'numeric',
                        'valueExpr': 'head.RES_FACTOR',
                        'ext_options': {
                            'tooltip': 'Factor for Resampling for this input. 1 = Disabled.',
                            'min': 1,
                        },
                    },
                    {
                        'path': input_path + ':COEFFICIENT',
                        'type': 'numeric',
                        'options':('no_write_model', 'write_once',),
                        'ext_options': {
                            'tooltip': 'Calibration coefficient (factor) for this input, queried from the digitizer.',
                        },
                    },
                    {
                        'path': input_path + ':OFFSET',
                        'type': 'numeric',
                        'options':('no_write_model', 'write_once',),
                        'ext_options': {
                            'tooltip': 'Calibration offset for this input, queried from the digitizer.',
                        },
                    },
                    {
                        'path': input_path + ':SOFT_DECIM',
                        'type': 'numeric',
                        'valueExpr': 'head.SOFT_DECIM',
                        'options':('no_write_shot',),
                        'ext_options': {
                            'tooltip': 'Software Decimation for this input. Computed on the server by discarding every N-1 samples. 1 = Disabled.',
                            'min': 1,
                        },
                    }
                ]

        elif model == 'ACQ424ELF':
            for i in range(32):
                parts.append({
                    'path': ':SITE%d:OUTPUT_%02d' % (site, i + 1,),
                    'type': 'signal',
                    'output': True,
                })

        elif model == 'AO424ELF':
            pass

        elif model == 'DIO482ELF':
            pass

        else:
            raise Exception('Unknown module %s in site %d' % (model, site,))

        return parts

    def _get_module_info(self, model):
        info = {
            'nchan': 0,
            'dtype': '',
            'input': False,
            'output': False,
        }

        if model == 'ACQ435ELF':
            info['nchan'] = 32
            info['input'] = True
        elif model == 'ACQ423ELF':
            info['nchan'] = 32
            info['input'] = True
        elif model == 'ACQ424ELF':
            info['nchan'] = 32
            info['output'] = True

        return info

    # def help(self):
    #     print("""
    #         This is a self-configuring device. First, fill in the :ADDRESS node with the IP or DNS of the AC2106.
    #         Next, open the tree for edit and run the following method:

    #             do /method NODE configure

    #         When you run configure(), it will record the Firmware version, FPGA Image, and all of the connected modules.
    #         If either the FPGA Image or the connected modules change, you must edit the tree and run configure again.
    #         If you attempt to run the device with a different FPGA Image or modules, it will error and fail.

    #     """)
    # HELP = help

    def setup(self):
        ds = DeviceSetup(self)
        ds.run()
    SETUP = setup

    def _get_calibration(self, uut):
        # In order to get the calibration per-site, we cannot use uut.fetch_all_calibration()
        for site in list(map(int, uut.get_aggregator_sites())):
            client = uut.modules[site]

            self._info('Reading Calibration for site %d' % (site,))

            coefficients = list(map(float, client.AI_CAL_ESLO.split()[3:]))
            offsets = list(map(float, client.AI_CAL_EOFF.split()[3:]))

            site_node = self.getNode('SITE%d' % (site,))
            for i in range(int(client.NCHAN)):
                input_node = site_node.getNode('INPUT_%02d' % (i + 1,))
                input_node.COEFFICIENT.record = coefficients[i]
                input_node.OFFSET.record = offsets[i]

    def _set_sync_role(self, uut):

        # Everything after the ; should not be trusted
        current_sync_role_parts = uut.s0.sync_role.split(';')[0].split(maxsplit=3)

        # Positional Arguments
        current_sync_role = current_sync_role_parts[0]
        current_frequency = int(current_sync_role_parts[1])

        # Keyword Arguments
        current_arguments = self._string_to_dict(current_sync_role_parts[2])

        requested_arguments = dict()

        trigger_source = str(self.TRIG_SOURCE.data()).upper()
        if trigger_source in self._TRIGGER_SOURCE_D0_OPTIONS:
            requested_arguments['TRG:DX'] = 'd0'
        elif trigger_source in self._TRIGGER_SOURCE_D1_OPTIONS:
            requested_arguments['TRG:DX'] = 'd1'

        changed = False

        requested_sync_role = str(self.SYNC_ROLE.data()).lower()
        if current_sync_role != requested_sync_role:
            changed = True
            self._info(f"Requested sync role of {requested_sync_role} differs from current configuration: {current_sync_role}")

        requested_frequency = int(self.FREQUENCY.data())
        if current_frequency != requested_frequency:
            changed = True
            self._info(f"Requested frequency of {requested_frequency} differs from current configuration: {current_frequency}")

        if current_arguments != requested_arguments:
            changed = True
            self._info(f"Requested sync role arguments '{self._dict_to_string(requested_arguments)}' differ from current configuration: '{self._dict_to_string(current_arguments)}'")

        if changed:
            self._info('Reconfiguring sync role, this may take some time.')
            uut.s0.sync_role = f"{self.SYNC_ROLE.data()} {int(self.FREQUENCY.data())} {self._dict_to_string(requested_arguments)}"

        # snyc_role will set a default trigger source, so we need to set these after
        self._info(f"Setting trigger source of timing highway {requested_arguments['TRG:DX']} to {trigger_source}")
        if requested_arguments['TRG:DX'] == 'd0':
            uut.s0.SIG_SRC_TRG_0 = trigger_source
        elif requested_arguments['TRG:DX'] == 'd1':
            uut.s0.SIG_SRC_TRG_1 = trigger_source

        # If we are configured for White Rabbit, query the Nanoseconds / Tick
        try:
            ns_per_tick_node = self.WRTD.NS_PER_TICK
            ns_per_tick_node.record = float(uut.cC.WRTD_TICKNS)
        except MDSplus.TreeNNF:
            pass

    def _set_hardware_decimation(self, uut):
        # TODO: Does setting it on site 0 set it on all of them?

        decimation = str(self.HARD_DECIM.data())
        self._info(f"Setting hardware decimation to {decimation}")

        for site, client in uut.modules.items():
            client.nacc = decimation

    class Monitor(threading.Thread):
        """Monitor thread for recording device temperature and voltage"""

        def __init__(self, device):
            super(ACQ2106.Monitor, self).__init__(name="Monitor")
            self.device = device.copy()

        def run(self):
            try:
                self.device.tree.open()

                temp_node = self.device.TEMP
                temp_fpga_node = self.device.TEMP_FPGA

                site_temp_nodes = {}
                for i in range(1, self.device._MAX_SITES + 1):
                    name = 'SITE%d' % (i,)
                    try:
                        site_temp_nodes[name] = self.device.getNode(name).TEMP
                    except MDSplus.TreeNNF:
                        pass

                uut = self.device._get_uut()
                if uut is None:
                    raise Exception(f"Unable to connect to digitizer ({self.device.ADDRESS.data()})")

                # One Segment = One Hour
                segment_size = 3600 / self.device._MONITOR_DELAY_SECONDS

                # TODO: Account for the time it takes to store the data
                while self.device.RUNNING.on:
                    now = time.time()

                    sys_temp = self.device._string_to_dict(uut.s0.SYS_TEMP)
                    # TODO: Voltages
                    # sys_volts = self.device._string_to_dict(uut.s0.SYS_VOLTS)

                    if 'mainboard' in sys_temp:
                        temp_node.putRow(segment_size, float(sys_temp['mainboard']), now)

                    if 'ZYNQ' in sys_temp:
                        temp_fpga_node.putRow(segment_size, float(sys_temp['ZYNQ']), now)

                    for name, node in site_temp_nodes.items():
                        if name in sys_temp:
                            node.putRow(segment_size, float(sys_temp[name]), now)

                    # TODO: Account for the time it took to write the temperatures 
                    time.sleep(self.device._MONITOR_DELAY_SECONDS)

            except Exception as e:
                self.exception = e
                traceback.print_exc()

    class StreamWriter(threading.Thread):
        def __init__(self, reader):
            super(ACQ2106.StreamWriter, self).__init__(name="StreamWriter")
            self.device = reader.device.copy()
            self.reader = reader

        def run(self):
            try:
                self.device.tree.open()

                uut = self.device._get_uut()
                if uut is None:
                    raise Exception(f"Unable to connect to digitizer ({self.device.ADDRESS.data()})")

                input_nodes = []
                resampled_nodes = []
                resample_factors = []
                software_decimations = []
                for site in list(map(int, uut.get_aggregator_sites())):
                    site_node = self.device.getNode('SITE%d' % (site,))
                    client = uut.modules[site]
                    for i in range(int(client.NCHAN)):
                        input_node = site_node.getNode('INPUT_%02d' % (i + 1,))
                        input_nodes.append(input_node)
                        resampled_nodes.append(input_node.RESAMPLED)
                        resample_factors.append(int(input_node.RES_FACTOR.data()))
                        software_decimations.append(int(input_node.SOFT_DECIM.data()))

                spad_nodes = []
                for i in range(self.device._MAX_SPAD):
                    spad_node = self.device.SCRATCHPAD.getNode('SPAD%d' % (i,))
                    if i < self.reader.nspad:
                        spad_nodes.append(spad_node)
                    else:
                        # Turn off the unused SPAD nodes
                        spad_node.on = False

                hardware_decimation = int(self.device.HARD_DECIM.data())
                delta_time = float(1.0 / self.device.FREQUENCY.data() * hardware_decimation)

                event_name = self.device.STREAM.EVENT_NAME.data()

                segment_index = 0
                while True:
                    try:
                        buffer = self.reader.full_buffer_queue.get(block=True, timeout=1)
                    except queue.Empty:
                        continue

                    # A buffer of None signals the end to streaming
                    if buffer is None:
                        break

                    benchmark_start = time.time()

                    # Used by both 32-bit input modules and the SPAD
                    data_int32 = np.frombuffer(buffer, dtype='int32')

                    data = None
                    samples_per_row = 0
                    if uut.data_size() == 4:
                        data = np.right_shift(data_int32, 8)
                        samples_per_row = self.reader.nchan + self.reader.nspad
                    else:
                        data = np.frombuffer(buffer, dtype='int16')
                        samples_per_row = self.reader.nchan + (self.reader.nspad * 2) # spad is 32bit, so we need to double it

                    # Credit to Mark W. for this masterpiece
                    # data for channel N is data_reshaped[:, N]
                    data_reshaped = np.reshape(data, (self.reader.segment_length, samples_per_row,))[:, : self.reader.nchan]

                    for i, input in enumerate(input_nodes):
                        if input.on:
                            segment_length = self.reader.segment_length / software_decimations[i]
                            input_delta_time = delta_time * software_decimations[i]
                            input_data = data_reshaped[:: software_decimations[i], i]

                            begin = segment_index * segment_length * input_delta_time
                            end = begin + ((segment_length - 1) * input_delta_time)
                            dim = MDSplus.Range(begin, end, input_delta_time)

                            input.makeSegmentResampled(begin, end, dim, input_data, resampled_nodes[i], resample_factors[i])

                    if self.reader.nspad > 0:
                        data_spad_reshaped = np.reshape(data_int32, (self.reader.segment_length, samples_per_row,))[:, self.reader.nchan :]

                        begin = segment_index * self.reader.segment_length * delta_time
                        end = begin + ((self.reader.segment_length - 1) * delta_time)
                        dim = MDSplus.Range(begin, end, delta_time)

                        for i in range(self.reader.nspad):
                            spad_nodes[i].makeSegment(begin, end, dim, data_spad_reshaped[:, i])

                    benchmark_end = time.time()
                    benchmark_elapsed = benchmark_end - benchmark_start

                    segment_index += 1
                    self.device._info(f"Finished writing segment {segment_index}/{self.reader.segment_count}, took {benchmark_elapsed}s")
                    self.reader.empty_buffer_queue.put(buffer)

                    MDSplus.Event(event_name)

            except Exception as e:
                self.exception = e
                traceback.print_exc()

    class StreamReader(threading.Thread):

        def __init__(self, device):
            super(ACQ2106.StreamReader, self).__init__(name="StreamReader")
            self.device = device.copy()

        def run(self):
            import acq400_hapi
            from fractions import gcd

            try:
                self.device.tree.open()

                uut = self.device._get_uut()
                if uut is None:
                    raise Exception(f"Unable to connect to digitizer ({self.device.ADDRESS.data()})")

                self.full_buffer_queue = queue.Queue()
                self.empty_buffer_queue = queue.Queue()

                software_decimations = []
                for site in list(map(int, uut.get_aggregator_sites())):
                    site_node = self.device.getNode('SITE%d' % (site,))
                    client = uut.modules[site]
                    for i in range(int(client.NCHAN)):
                        input_node = site_node.getNode('INPUT_%02d' % (i + 1,))
                        software_decimations.append(int(input_node.SOFT_DECIM.data()))

                # TODO: Test these assumptions
                # Determine how many extra SPAD channels there are
                _, spad, _ = uut.s0.spad.split(',') # 1,4,0 means 1=enabled??, 4=spad channels, 0=d0/d1 ???
                self.nspad = int(spad)

                # All remaining channels are actual data
                self.nchan = uut.nchan() - self.nspad

                bytes_per_row = (self.nchan * uut.data_size()) + (self.nspad * 4) # SPAD channels are always 32 bit

                # Find the lowest common decimator
                decimator = 1
                for soft_decim in software_decimations:
                    decimator = int(decimator * soft_decim / gcd(decimator, soft_decim))
                self.device._info(f"Calculated a greatest common decimator of {decimator}")

                self.segment_length = int(self.device.STREAM.SEG_LENGTH.data())
                self.segment_count = int(self.device.STREAM.SEG_COUNT.data())

                # If the decimator and segment length don't match, adjust the segment length
                if self.segment_length % decimator > 0:
                    old_segment_length = self.segment_length
                    self.segment_length = (self.segment_length // decimator + 1) * decimator
                    self.device._info(f"Adjusting segment length to match lowest common decimator, {old_segment_length} -> {self.segment_length}")
                    
                    self.device.STREAM.SEG_LENGTH.record = self.segment_length

                self.segment_size = self.segment_length * bytes_per_row

                self.writer = self.device.StreamWriter(self)
                self.writer.start()

                # When TRIG_SOURCE is set to STRIG, opening the socket will actually trigger the device, so we have to do this last
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(6) # TODO: Investigate making this configurable or something
                self.socket.connect((self.device.ADDRESS.data(), acq400_hapi.AcqPorts.STREAM))

                segment_index = 0
                first_recv = True
                while self.device.RUNNING.on and segment_index < self.segment_count:
                    buffer = None
                    try:
                        buffer = self.empty_buffer_queue.get(block=False)
                    except queue.Empty:
                        self.device._verbose(f"No empty buffers available, creating new one of {self.segment_size} bytes")
                        buffer = bytearray(self.segment_size)

                    bytes_needed = self.segment_size
                    try:
                        view = memoryview(buffer)
                        while bytes_needed > 0:
                            bytes_read = self.socket.recv_into(view, bytes_needed)

                            # TODO: Make more accurate, possibly account for the time it took to read the segment, possibly use the SPAD
                            if first_recv:
                                first_recv = False
                                self.device.TRIG_TIME.record = time.time()

                            view = view[bytes_read:]
                            bytes_needed -= bytes_read

                    except socket.timeout:
                        self.device._warning("Socket connection timed out, retrying")
                        continue

                    except socket.error:
                        # TODO: Handle Partial Segments?
                        # self.full_buffer_queue.put(buffer[:self.segment_size - bytes_needed])
                        self.full_buffer_queue.put(None)
                        raise

                    else:
                        self.full_buffer_queue.put(buffer)
                        
                        segment_index += 1
                        self.device._info(f"Finished reading segment {segment_index}/{self.segment_count}")

            except Exception as e:
                self.exception = e
                traceback.print_exc()

            # This will signal the StreamWriter that no more buffers will be coming
            self.full_buffer_queue.put(None)

            self.device.RUNNING.on = False

            # Wait for the StreamWriter to finish
            try:
                while self.writer.is_alive():
                    pass
            finally:
                self.writer.join()
                if hasattr(self.writer, "exception"):
                    self.exception = self.writer.exception

    def _init(self):
        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        self._get_calibration(uut)
        self._set_sync_role(uut)
        self._set_hardware_decimation(uut)

    ###
    ### Streaming Methods
    ###

    def start_stream(self):
        if self.MODE.data().upper() != 'STREAM':
            raise Exception('Device is not configured for streaming. Set MODE to "STREAM" and then run configure().')

        self._init()

        self.RUNNING.on = True

        monitor = self.Monitor(self)
        monitor.start()

        thread = self.StreamReader(self)
        thread.start()

    START_STREAM = start_stream

    def stop_stream(self):
        if self.MODE.data().upper() != 'STREAM':
            raise Exception('Device is not configured for streaming. Set MODE to "STREAM" and then run configure().')

        self.RUNNING.on = False

    STOP_STREAM = stop_stream

    ###
    ### Transient Methods
    ###

    def arm_transient(self):
        if self.MODE.data().upper() != 'TRANSIENT':
            raise Exception('Device is not configured for transient recording. Set MODE to "TRANSIENT" and then run configure().')

        self._init()

        self.RUNNING.on = True

        monitor = self.Monitor(self)
        monitor.start()

    ARM_TRANSIENT = arm_transient

    def store_transient(self):
        if self.MODE.data().upper() != 'TRANSIENT':
            raise Exception('Device is not configured for transient recording. Set MODE to "TRANSIENT" and then run configure().')

        self.RUNNING.on = False
        
        # TODO: Store

    STORE_TRANSIENT = store_transient

    def soft_trigger(self):
        pass

    SOFT_TRIGGER = soft_trigger

    def get_state(self):
        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        mode = str(self.MODE.data()).upper()
        if mode == 'STREAM':
            stream_state = uut.s0.CONTINUOUS_STATE.split()[1]
            print(f"State: {stream_state}")
        elif mode == 'TRANSIENT':
            transient_state = uut.s0.TRANS_ACT_STATE.split()[1]
            print(f"State: {transient_state}")
        else:
            print(f"Undefined Operation Mode {mode}, unable to get state")

    GET_STATE = get_state

    def configure(self, has_wr=False):
        if not self.tree.isOpenForEdit():
            raise Exception('The tree must be open for edit in order to configure a device')

        self._configure_nodes = [self]

        # Ensure that any new nodes in the parts array are taken care of, and add the original parts array to _configure_nodes
        self._add_parts(self.parts)

        ###
        ### General
        ###

        self.TRIG_SOURCE.record = str(self.TRIG_SOURCE.data()).upper()
        self.SYNC_ROLE.record = str(self.SYNC_ROLE.data()).lower()

        ###
        ### Mode-Specific
        ###

        self.MODE.record = str(self.MODE.data()).upper()

        mode = self.MODE.data()
        if mode == 'STREAM':
            self._add_parts(self.stream_parts)
            self.RUNNING.on = False
        elif mode == 'TRANSIENT':
            self._add_parts(self.transient_parts)

        ###
        ### Module-Specific
        ###

        aggregator_sites = []

        # The default timeout is quite long, so we shorten that to a second
        socket.setdefaulttimeout(1)
        
        uut = self._get_uut()
        if uut is None:
            ###
            ### Offline
            ###

            self._info('Configuring offline, you will need to run configure with access to the ACQ at least once before running.')

            modules = self.MODULES.getDataNoRaise()
            if modules is None or modules == '':
                raise Exception(f"When configuring offline, you must specify the modules manually.")

            modules = self._string_to_dict(modules)

            for site in range(1, self._MAX_SITES + 1):
                if site in modules and modules[site] != '':
                    model = modules[site]
                    self._info(f"Module assumed to be in site {site}: {model}")

                    parts = self._get_parts_for_site(site, model)
                    self._add_parts(parts)

                    site_node = self.getNode('SITE%d' % (site,))
                    site_node.MODEL = model

                else:
                    print(f"No module assumed to be in site {site}")

            if has_wr:
                self._info('Assuming White Rabbit capabilities')

            # TODO: Approximate uut.get_aggregator_sites
            # aggregator_sites =

        else:
            ###
            ### Online
            ###

            self.SERIAL.record = uut.s0.SERIAL

            self._info(f"Firmware: {uut.s0.software_version}")
            self.FIRMWARE.record = uut.s0.software_version

            self._info(f"FPGA Image: {uut.s0.fpga_version}")
            self.FPGA_IMAGE.record = uut.s0.fpga_version

            modules = dict()
            for site, client in sorted(uut.modules.items()):
                model = client.MODEL.split(' ')[0]
                modules[site] = model

                self._info(f"Module found in site {site}: {model}")

                parts = self._get_parts_for_site(site, model)
                self._add_parts(parts)

                siteNode = self.getNode('SITE%d' % (site,))
                siteNode.SERIAL.record = client.SERIAL

            self.MODULES.record = self._dict_to_string(modules)

            has_wr = (uut.s0.has_wr != 'none')
            if has_wr:
                self._info('Detected White Rabbit capabilities')

            aggregator_sites = list(map(int, uut.get_aggregator_sites()))

        ###
        ### White Rabbit
        ###

        if has_wr:
            self._add_parts(self.wrtd_parts)

        ###
        ### TIGA
        ###

        has_tiga = False
        if has_tiga:
            pass

        ###
        ### HUDP
        ###

        has_hudp = False
        if has_hudp:
            pass

        ###
        ### Aggregated Inputs
        ###

        # TODO: Replace with Node Hardlinks when available

        all_inputs = []
        for site in aggregator_sites:
            model = modules[site]
            info = self._get_module_info(model)

            site_node = self.getNode('SITE%d' % (site,))
            for i in range(info['nchan']):
                input_node = site_node.getNode('INPUT_%02d' % (i + 1,))
                all_inputs.append(input_node)

        self._info(f"Found a total of {len(all_inputs)} aggregated inputs")

        if len(all_inputs) > 0:
            input_parts = [
                {
                    'path': ':INPUTS',
                    'type': 'structure',
                }
            ]
            for i, input in enumerate(all_inputs):
                input_path = ':INPUTS:INPUT_%03d' % (i + 1,)
                input_parts += [
                    {
                        'path': input_path,
                        'type': 'signal',
                        'value': input,
                    },
                    {
                        'path': input_path + ':RESAMPLED',
                        'type': 'signal',
                        'value': input.RESAMPLED,
                    },
                ]
            self._add_parts(input_parts)

        # print('Found a total of %d inputs' % (len(self._configure_outputs),))

        # if len(self._configure_outputs) > 0:
        #     output_parts = [
        #         {
        #             'path': ':OUTPUTS',
        #         }
        #     ]
        #     for i, output in enumerate(self._configure_outputs):
        #         output_parts.append({
        #             'path': 'OUTPUTS:OUTPUT_%03d' % (i + 1,),
        #             'type': 'signal',
        #             'value': output,
        #         })
        #     self._add_parts(output_parts)

        # TODO: SPAD

        # TODO: Compare and delete self._configure_nodes
        all_nids = self.getNodeWild('***').data()
        new_nids = [ node.nid for node in self._configure_nodes ]

        bad_nids = set(all_nids) - set(new_nids)
        bad_paths = [ str(MDSplus.TreeNode(nid, self.tree).path) for nid in bad_nids ]
        for path in bad_paths:
            try:
                print(f"Removing {path}")
                node = self.tree.getNode(path)
                node.delete()
            except MDSplus.TreeNNF:
                # node.delete() deletes all child nodes as well, so the node may already be gone
                pass
            except MDSplus.SsSUCCESS:
                # For some reason, node.delete() always throws this
                pass

    CONFIGURE = configure

    def verify(self):

        # Verify connectivity

        uut = None
        try:
            # TODO: Reduce crazy long timeout time?
            uut = self._get_uut()
        except (socket.timeout, socket.error):
            pass

        if uut is None:
            raise Exception('Unable to communicate with the ACQ2106, please verify the value of the ADDRESS node and try again.')

        # Check the FPGA Image, this isn't a hard requirement but still

        firmware = uut.s0.software_version
        if firmware != self.FIRMWARE.data():
            self._warning('The Firmware Version has changed, it is recommended that you run configure() again.')

        fpga_image = uut.s0.fpga_version
        if fpga_image != self.FPGA_IMAGE.data():
            self._warning('The FPGA image has changed, it is recommended that you run configure() again.')

        # Verify the configured modules

        modules = dict()
        for site, client in uut.modules.items():
            model = client.MODEL.split(' ')[0]
            modules[site] = model

        if self._string_to_dict(self.MODULES.data()) != modules:
            raise Exception('The modules in the device have changed since the last call to configure(). You must run configure() again.')

        # Verify the values of all nodes

        for node in self.getNodeWild('***'):
            self._verbose(f"Verifying configuration for {node.path}")
            value = node.getDataNoRaise()
            if value is not None:
                if node.usage == 'NUMERIC':
                    min_value = node.getExtendedAttribute('min')
                    if min_value is not None:
                        if value < min_value:
                            raise Exception(f"Node {node.path} has invalid value of {value}, must be >= {min_value}")

                    max_value = node.getExtendedAttribute('max')
                    if max_value is not None:
                        if value > max_value:
                            raise Exception(f"Node {node.path} has invalid value of {value}, must be <= {max_value}")

                    values = node.getExtendedAttribute('values')
                    if values is not None:
                        if value not in values:
                            raise Exception(f"Node {node.path} has invalid value of {value}, must be one of {list(values)}")

                elif node.usage == 'TEXT':
                    values = node.getExtendedAttribute('values')
                    # TODO: Maybe compare case insensitive?
                    if values is not None:
                        if value not in values:
                            raise Exception(f"Node {node.path} has invalid value of {value}, must be one of {list(values)}")

        # Verify the configured mode based on the nodes that should be there

        mode = self.MODE.data()
        mode_node_exists = True
        try:
            if mode == 'STREAM':
                self.getNode('STREAM')
            elif mode == 'TRANSIENT':
                self.getNode('TRANSIENT')

        except MDSplus.TreeNNF:
            mode_node_exists = False

        if not mode_node_exists:
            raise Exception('The mode has changed since the last call to configure(). You must run configure() again.')

        # Verify White Rabbit

        wrtd_node_exists = True
        try:
            self.getNode('WRTD')

        except MDSplus.TreeNNF:
            wrtd_node_exists = False

        if wrtd_node_exists:
            if uut.s0.has_wr == 'none':
                raise Exception('The last call to configure() had White Rabbit enabled, but the current FPGA Image does not support White Rabbit. You must run configure() again or change your FPGA Image and reboot.')

    VERIFY = verify

    ###
    ### Helper Methods
    ###

    def _verbose(self, format, *args):
        self.dprint(4, format, *args)

    def _info(self, format, *args):
        self.dprint(3, format, *args)

    def _warning(self, format, *args):
        self.dprint(2, format, *args)

    def _error(self, format, *args):
        self.dprint(1, format, *args)

    def _get_uut(self):
        import acq400_hapi

        # If there is no address, we don't need to try to connect
        address = self.ADDRESS.getDataNoRaise()
        if address is None or address == '':
            self._verbose('ADDRESS is blank, will configure offline.')
            return None

        # Attempt to resolve the name so we can fail fast
        try:
            address = socket.gethostbyname(str(address))
        except:
            self._verbose(f"ADDRESS '{address}' failed to resolve to an IP, will configure offline.")
            return None

        return acq400_hapi.factory(self.ADDRESS.data())

    # TODO: Move this into MDSplus.Device?
    def _add_parts(self, parts, overwrite=False):
        # See MDSplus.Device.Add

        # Configure tree, path, and head as global variables, to be accessed from valueExpr
        eval_globals = MDSplus._mimport('__init__').load_package({})
        eval_globals['tree'] = self.tree
        eval_globals['path'] = self.path
        eval_globals['head'] = self

        # First, add all the nodes
        for part in parts:
            try:
                node = self.addNode(part['path'], part.get('type', 'none'))
                self._verbose(f"Adding {node.path}")
            except MDSplus.mdsExceptions.TreeALREADY_THERE:
                node = self.getNode(part['path'])
                self._verbose(f"Found {node.path}")

        # Then you can reference them in valueExpr
        for part in parts:
            node = self.getNode(part['path'])
            self._configure_nodes.append(node)

            eval_globals['node'] = node

            usage = part.get('type', 'none').upper()
            if node.getUsage() != usage:
                node.setUsage(usage)

            if 'options' in part:
                # print('Setting options of %s: %s' % (node.path, part['options'],))
                for option in part['options']:
                    node.__setattr__(option, True)

            data = node.getDataNoRaise()

            if 'ext_options' in part:
                # If no_write_model is set, you are unable to set XNCIs, so we temporarily disable it
                no_write_model = node.no_write_model
                node.no_write_model = False

                if isinstance(part['ext_options'], dict):
                    for option, value in part['ext_options'].items():
                        node.setExtendedAttribute(option, value)

                node.no_write_model = no_write_model

            if data is not None:
                node.record = data

            # Hack: For things that run a function in valueExpr, the data will still be None even if we don't want it to run again
            if overwrite or node.time_inserted == 0:
                if 'value' in part:
                    self._verbose(f"Setting value of {node.path} to: {str(part['value'])}")
                    node.record = part['value']
                elif 'valueExpr' in part:
                    self._verbose(f"Setting value of {node.path} to expression: {part['valueExpr']}")
                    node.record = eval(part['valueExpr'], eval_globals)

    def _set_input_segment_scale(self, node, coefficient, offset):
        node.setSegmentScale(
            MDSplus.ADD(
                MDSplus.MULTIPLY(
                    coefficient,
                    MDSplus.dVALUE()
                ),
                offset
            )
        )

    def _dict_to_string(self, dictionary):
        """Convert a dictionary into a string in the form key1=value1,key2=value2. See _string_to_dict as for the inverse."""
        return ' '.join([ f"{key}={value}" for key, value in dictionary.items() ])

    def _string_to_dict(self, string):
        """Convert a string in the form key1=value1,key2=value2 into a dictionary. See _dict_to_string for the inverse."""
        dictionary = dict()
        
        pairs = string.split(' ')
        if len(pairs) == 1:
            pairs = string.split(',')
        
        for pair in pairs:
            if '=' in pair:
                key, value = pair.split('=')
                
                if key.isnumeric():
                    key = int(key)
                    
                if value.isnumeric():
                    value = int(value)
                    
                dictionary[key] = value
            else:
                dictionary[pair] = None
        return dictionary

    _SECONDS_TO_NANOSECONDS = 1_000_000_000
    """One second in nanoseconds"""

    # NOTE: Update when this value is changed globally
    _TAI_TO_UTC_OFFSET_NANOSECONDS = 37 * _SECONDS_TO_NANOSECONDS
    """The offset needed to convert TAI timestamps to UTC and back"""

    def _tai_to_utc(self, tai_timestamp):
        """Convert nanosecond TAI timestamp to UTC"""
        return tai_timestamp - self._TAI_TO_UTC_OFFSET_NANOSECONDS

    def _utc_to_tai(self, utc_timestamp):
        """Convert nanosecond UTC timestamp to TAI"""
        return utc_timestamp + self._TAI_TO_UTC_OFFSET_NANOSECONDS

    def _parse_spad_tai_timestamp(self, spad, ns_per_tick):
        """Parse the nanosecond TAI timestamp stored in SPAD[1] and SPAD[2]"""

        tai_seconds = np.uint32(spad[1])
        tai_ticks = np.uint32(spad[2]) & 0x0FFFFFFF # The vernier
        tai_nanoseconds = (tai_ticks * ns_per_tick)

        # Calculate the TAI time in nanoseconds
        tai_timestamp = (tai_seconds * self._SECONDS_TO_NANOSECONDS) + tai_nanoseconds
        return int(tai_timestamp)