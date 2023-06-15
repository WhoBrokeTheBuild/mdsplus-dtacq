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

    # TODO: Note that HARD_DECIM is for 32-bit cards only

    ## Configuring

    When you first add this driver to your tree, it is incomplete. You must do some initial configuration and then call the `configure` method.
    It is recommended to configure Online, meaning you can connect to your digitizer from the computer you are calling `configure` on. If you
    are not able to do so, you can configure Offline, but you must configure Online at least once before the driver can be used.

    # TODO: Detail various arguments to configure()

    ### Modes

    Before configuring, you must choose an operational mode. The mode controls how the device captures data, and what nodes get added to the
    tree for configuration.

    #### STREAM

    # TODO: Explain SEGLEN_CONF -> SEGLEN_ACT

    Data will stream continuously, storing data in segments of `STREAM:SEGLEN_ACT` samples until either the `stop_stream` method is called, or
    the number of segments reaches `STREAM:SEG_COUNT`. In order to allow enough time to write each segment to disk, try experimenting with the
    value of `STREAM:SEGLEN_CONF` until the writer thread is able to keep up. With `DEBUG_DEVICES >= 4` a message will be displayed every time
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

    # TODO: Explain that it will try to configure online first
    When configuring offline, you need to give the `configure` method most of the information it would otherwise query from the digitizer.
    At a minimum, you must fill in the following nodes:
    * `MODE` must contain the intended operation mode, see Modes for a list of available modes.
    * `MODULES` must contain a comma-separated list of the model names of the installed modules in the form of SiteNumber=ModelName.
      e.g. "1=ACQ423ELF 3=ACQ423ELF 6=DIO482" has ACQ423's in sites 1 and 3, and a DIO482 in site 6

    For example:
    ```
    TCL> edit TREE_NAME
    TCL> put DEVICE_NODE:MODE """STREAM"""
    TCL> put DEVICE_NODE:MODULES """1=ACQ423ELF 3=ACQ423ELF 6=DIO482"""
    TCL> do /method DEVICE_NODE configure /ARGUMENT="""has_wr=1 has_hudp=0 overwrite_data=1"""
    ```
    or
    ```py
    tree = MDSplus.Tree('EDIT', -1)
    tree.DEVICE_NODE.MODE.record = "STREAM"
    tree.DEVICE_NODE.MODULES.record = "1=ACQ423ELF 3=ACQ423ELF 6=DIO482"
    tree.DEVICE_NODE.configure(has_wr=True, has_hudp=False, overwrite_data=True)
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

    _TRIGGER_SOURCE_WR_OPTIONS = [
        'FPTRG',
        'HDMI',
    ]
    """
    Trigger Source options for WRTD
    * FPTRG: Front Panel Trigger port
    * HDMI: HDMI Trigger
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

    # Nodes in the standard parts array are considered part of the conglomorate that makes up the device.
    # This causes issues when you try to remove or change these nodes, so we only add the bare essentials.
    parts = [
        {
            'path': ':COMMENT',
            'type': 'text',
            'options': ('no_write_shot',),
        },
        {
            'path': ':MODE',
            'type': 'text',
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
                'tooltip': 'Comma-separated list of modules, specified by model name, or nothing if no module is present.'
                  'This is queried from the device if configuring online, or is a required input if configuring offline.',
            },
        },
        {
            'path': ':ADDRESS',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'IP address or DNS name of the digitizer. Leave this blank to configure offline.',
            },
        },
        {
            'path': ':EPICS_NAME',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'EPICS name of the digitizer, usually the hostname in the form of acq2106_xyz.',
            },
        },
    ]

    # Any additional parts that are needed, but are not included in the bare bones parts array above go here.
    _default_parts = [
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
                'tooltip': 'FPGA image version of the ACQ2106, queried during configure().',
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
                'tooltip': 'Sample frequency in Hertz.',
            },
        },
        {
            'path': ':RUNNING',
            'type': 'numeric',
            # 'on': False,
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'On if running, or Off otherwise.',
            },
        },
        {
            'path': ':TRIGGER',
            'type': 'structure',
        },
        {
            'path': ':TRIGGER:TIMESTAMP', # Or TIME_OF_DAY
            'type': 'numeric',
            'options': ('write_shot',),
            'ext_options': {
                'tooltip': 'Recorded trigger time as a UNIX timestamp in seconds.',
            },
        },
        {
            'path': ':TRIGGER:TIME_AT_0',
            'type': 'numeric',
            'value': 0,
            'options': ('write_shot',),
            'ext_options': {
                'tooltip': 'Time offset in seconds, used when building the Window(start, end, TIME_AT_0)',
            },
        },
        {
            'path': ':TRIGGER:SOURCE',
            'type': 'text',
            'value': 'EXT',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Trigger source. These options will decide if the timing highway is d0 or d1. For a soft trigger use STRIG, and for a hard trigger use EXT.',
                'values': _TRIGGER_SOURCE_D0_OPTIONS + _TRIGGER_SOURCE_D1_OPTIONS,
            },
        },
        {
            'path': ':TEMP',
            'type': 'signal',
            'ext_options': {
                'tooltip': 'Recorded temperature of the main board in Celsius.',
            },
        },
        {
            'path': ':TEMP_FPGA',
            'type': 'signal',
            'ext_options': {
                'tooltip': 'Recorded temperature of the FPGA in Celsius.',
            },
        },
        {
            'path': ':INIT_ACTION',
            'type': 'action',
            'valueExpr': "Action(Dispatch('MDSIP_SERVER','INIT',50,None),Method(None,'INIT',head,'auto'))",
            'options': ('no_write_shot',)
        },
        {
            'path': ':DEFAULTS',
            'type': 'structure',
            'ext_options': {
                'tooltip': 'Contains defaults for settings that can be overridden per-input.',
            },
        },
        {
            'path': ':SCRATCHPAD',
            'type': 'structure',
            'ext_options': {
                'tooltip': 'Contains the scratchpad (SPAD) metadata recorded alongside the data.',
            },
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
                'tooltip': 'If present, this scratchpad field will contain part of the TAI timestamp.',
            },
        },
        {
            'path': ':SCRATCHPAD:SPAD2',
            'type': 'signal',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'If present, this scratchpad field will contain part of the TAI timestamp.',
            },
        },
    ]

    for _spad_index in range(3, _MAX_SPAD):
        _default_parts.append({
            'path': f":SCRATCHPAD:SPAD{_spad_index}",
            'type': 'signal',
            'options': ('no_write_model',),
        })

    _stream_parts = [
        {
            'path': ':STREAM',
            'type': 'structure',
            'ext_options': {
                'tooltip': 'Contains settings for use with MODE=STREAM.',
            },
        },
        {
            'path': ':STREAM:SEGLEN_CONF',
            'type': 'numeric',
            'value': 8000,
            'options': ('write_model',),
            'ext_options': {
                'tooltip': 'Configured segment length. The actual segment length will be recorded in SEGLEN_ACT.'
                    'This is the number of samples configured to be taken before calling makeSegment().'
                    'SEGLEN_ACT is computed from this number, but might increase to match a common divisor with all decimations.'
                    'Segments taken per second can be calculated as FREQUENCY/SEGLEN_ACT.',
                'min': 1024,
            },
        },
        {
            'path': ':STREAM:SEGLEN_ACT',
            'type': 'numeric',
            'options': ('write_shot',),
            'ext_options': {
                'tooltip': 'Actual segment length. This is the number of samples that will actually be taken before calling makeSegment().'
                    'This is computed from SEGLEN_CONF and possibly increased to match a common divisor with all decimations. '
                    'Segments taken per second can be calculated as FREQUENCY/SEGLEN_ACT.',
            },
        },
        {
            'path': ':STREAM:SEG_COUNT',
            'type': 'numeric',
            'value': 1000,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Segment count. This is the number of segments that will be captured before stopping.'
                    'This will need to be a common denominator of all decimations. If not, it will be automatically adjusted and updated in the tree.',
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
        {
            'path': ':DEFAULTS:SOFT_DECIM',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default software decimation, which can be overridden per-input. Computed on the server by discarding every N-1 samples.'
                    'Set to 1 to disable.',
                'min': 1,
            },
        },
        {
            'path': ':DEFAULTS:RES_FACTOR',
            'type': 'numeric',
            'value': 100,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default factor for resampling, which can be overridden per-input. Set to 1 to disable.',
                'min': 1,
            },
        },
    ]

    _transient_parts = [
        {
            'path': ':TRANSIENT',
            'type': 'structure',
            'ext_options': {
                'tooltip': 'Contains settings for use with MODE=TRANSIENT.',
            },
        },
        {
            'path':':TRANSIENT:PRESAMPLES',
            'type':'numeric',
            'value': 10000,
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Number of samples that will be captured before the trigger.',
            },
        },
        {
            'path':':TRANSIENT:POSTSAMPLES',
            'type':'numeric',
            'value': 10000,
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Number of samples that will be captured after the trigger.',
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

    _wr_parts = [
        {
            'path': ':WR',
            'type': 'structure',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Contains settings for controlling WRTD and WRTT.',
            },
        },
        {
            'path': ':WR:NS_PER_TICK',
            'type': 'numeric',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'The number of nanoseconds per tick of the clock, which is queried from the digitizer.',
            },
        },
        {
            'path': ':WR:TX_MESSAGE',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Message to send with WR when triggered.',
            },
        },
        {
            'path': ':WR:TX_DELTA_NS',
            'type': 'numeric',
            'value': 50000000,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Time in nanoseconds between when the WR message is sent and when the trigger it describes should happen.',
                'min': 50000000,
            },
        },
        {
            'path': ':WR:RX0_FILTER',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Comma-separated list of messages that will trigger WRTT0.',
            },
        },
        {
            'path': ':WR:RX1_FILTER',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Comma-separated list of messages that will trigger WRTT1.',
            },
        },
        {
            'path': ':WR:RX_ENABLE',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Turn on receiver.',
            },
        },
        {
            'path': ':WR:TX_ENABLE',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Turn on transmitter.',
            },
        },
        {
            'path': ':TRIGGER:WRTD_SOURCE',
            'type': 'text',
            'value': 'FPTRG',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Source of the trigger: FPTRG.',
                'values': _TRIGGER_SOURCE_WR_OPTIONS,
            },
        },
        # If we have White Rabbit, we can decode the TAI timestamps in SPAD1, SPAD2
        {
            'path': ':SCRATCHPAD:TIMESTAMPS',
            'type': 'any',
            'options': ('no_write_shot',),
            'valueExpr': 'TdiCompile("ACQ2106_PARSE_SPAD_TIMESTAMPS($, $, $)", node.parent.SPAD1, node.parent.SPAD2, head.WR.NS_PER_TICK)'
        },
    ]

    _sc_parts = [
        {
            'path': ':DEFAULTS:SC_GAIN1',
            'type': 'numeric',
            'value': 1,
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default signal conditioning gain #1, applied before the offset (SC_OFFSET). Can be overridden per-input.',
                'values': [ 1, 10, 100, 1000 ],
            },
        },
        {
            'path': ':DEFAULTS:SC_GAIN2',
            'type': 'numeric',
            'value': 1,
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default signal conditioning gain #2, applied after the offset (SC_OFFSET). Can be overridden per-input.',
                'values': [ 1, 2, 5, 10 ],
            },
        },
        {
            'path': ':DEFAULTS:SC_OFFSET',
            'type': 'numeric',
            'value': 0,
            'options':('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default signal conditioning offset, which is applied after the first gain (SC_GAIN1) and before the second gain (SC_GAIN2). '
                    'Can be overridden per-input.',
                'min': -2.5,
                'max': 2.5,
            },
        },
    ]

    _32bit_parts = [
        {
            'path': ':HARD_DECIM',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Hardware decimation (NACC). Computed on the digitizer by averaging every N samples together. Set to 1 to disable.',
                'min': 1,
                'max': 32,
            },
        },
    ]

    def _get_parts_for_site(self, site, model, has_sc=False):
        mode = str(self.MODE.data())
        
        site_path = f":SITE{site}"
        parts = [
            {
                'path': site_path,
                'type': 'structure'
            },
            {
                'path': site_path + ':MODEL',
                'type': 'text',
                'value': model,
                'ext_options': {
                    'tooltip': 'Model of the card in this site.',
                },
            },
            {
                'path': site_path + ':SERIAL',
                'type': 'text',
                'ext_options': {
                    'tooltip': 'Serial number of the card in this site.',
                },
            },
            {
                'path': site_path + ':TEMP',
                'type': 'signal',
                'ext_options': {
                    'tooltip': 'Recorded temperature of the card in this site in Celsius.',
                },
            },
        ]

        if model == 'ACQ435ELF' or model == 'ACQ423ELF':
            for input_index in range(32):
                input_path = site_path + f":INPUT_{input_index + 1:02}"
                parts += [
                    {
                        'path': input_path,
                        'type': 'signal',
                        'valueExpr': 'head._set_input_segment_scale(node, node.COEFFICIENT, node.OFFSET)',
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
                    }
                ]
                
                if mode == 'STREAM':
                    parts += [
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
                            'valueExpr': 'head.DEFAULTS.RES_FACTOR',
                            'ext_options': {
                                'tooltip': 'Factor for resampling for this input. Set to 1 to disable.',
                                'min': 1,
                            },
                        },
                        {
                            'path': input_path + ':SOFT_DECIM',
                            'type': 'numeric',
                            'valueExpr': 'head.DEFAULTS.SOFT_DECIM',
                            'options':('no_write_shot',),
                            'ext_options': {
                                'tooltip': 'Software decimation for this input, which is computed on the server by discarding every N-1 samples. Set to 1 to disable.',
                                'min': 1,
                            },
                        },
                    ]

                # if has_slow:
                #     parts += [
                #         {
                #             'path': input_path + ':SLOW',
                #             'type': 'signal',
                #             'options':('no_write_model',),
                #             'ext_options': {
                #                 'tooltip': 'The 1Hz slow data, downsampled on the digitizer.',
                #             },
                #         },
                #     ]

                if has_sc:
                    parts += [
                        {
                            'path': input_path + ':SC_GAIN1',
                            'type': 'numeric',
                            'valueExpr': 'head.DEFAULTS.SC_GAIN1',
                            'options':('no_write_shot',),
                            'ext_options': {
                                'tooltip': 'Default signal conditioning gain #1 for this input, which is applied before the offset (SC_OFFSET).',
                                'values': [ 1, 10, 100, 1000 ],
                            },
                        },
                        {
                            'path': input_path + ':SC_GAIN2',
                            'type': 'numeric',
                            'valueExpr': 'head.DEFAULTS.SC_GAIN2',
                            'options':('no_write_shot',),
                            'ext_options': {
                                'tooltip': 'Signal conditioning gain #2 for this input, which is applied after the offset (SC_OFFSET).',
                                'values': [ 1, 2, 5, 10 ],
                            },
                        },
                        {
                            'path': input_path + ':SC_OFFSET',
                            'type': 'numeric',
                            'valueExpr': 'head.DEFAULTS.SC_OFFSET',
                            'options':('no_write_shot',),
                            'ext_options': {
                                'tooltip': 'Signal conditioning offset for this input, which is applied after the first gain (SC_GAIN1) and before the second gain (SC_GAIN2).',
                                'min': -2.5,
                                'max': 2.5,
                            },
                        },
                    ]

        elif model == 'ACQ424ELF':
            for output_index in range(32):
                output_path = site_path + f":OUTPUT_{output_index + 1:02}"
                parts.append({
                    'path': output_path,
                    'type': 'signal',
                    'output': True,
                })

        elif model == 'AO424ELF':
            pass

        elif model == 'DIO482ELF':
            pass

        elif model == 'DIO482ELF_TD':
            parts += [
                {
                    'path': site_path + ':TX_MESSAGE', # TODO: Better name
                    'type': 'text',
                    'options': ('no_write_shot',),
                    'ext_options': {
                        'tooltip': 'Message to send with WR when triggered.',
                    },
                },
                {
                    'path': site_path + ':WRTD_SOURCE', # TODO: Better name
                    'type': 'text',
                    'value': 'FPTRG',
                    'options': ('no_write_shot',),
                    'ext_options': {
                        'tooltip': 'Source of the trigger: FPTRG.',
                        'values': self._TRIGGER_SOURCE_WR_OPTIONS,
                    },
                },
            ]

            for output_index in range(4):
                output_path = site_path + f":OUTPUT_{output_index + 1:02}"
                parts += [
                    {
                        'path': output_path,
                        'type': 'signal',
                        'ext_options': {
                            'tooltip': 'Data for this ',
                        },
                    },
                ]

        else:
            raise Exception(f"Unknown module {model} in site {site}")

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

    # TODO:
    # def setup(self):
    #     ds = DeviceSetup(self)
    #     ds.run()
    # SETUP = setup

    def _init(self, uut):
        # TODO: Put all the startup shit here
        # site_node = self.getNode(f"SITE{site}")
        # site_node.SERIAL.record = client.SERIAL

        # TODO: Improve
        #has_sc = False
        #for site, client in sorted(uut.modules.items()):
        #    if not has_sc and 'ELFX32' in client.knobs:
        #        has_sc = self._to_bool(client.ELFX32)

        #if has_sc:
        #    self._set_signal_conditioning_gains(uut)

        self._get_calibration(uut)
        self._set_sync_role(uut)
        self._set_hardware_decimation(uut)
        self._setup_wr(uut)

    def _get_calibration(self, uut):
        # In order to get the calibration per-site, we cannot use uut.fetch_all_calibration()
        for site in list(map(int, uut.get_aggregator_sites())):
            client = uut.modules[site]

            self._log_info(f"Reading Calibration for site {site}")

            coefficients = list(map(float, client.AI_CAL_ESLO.split()[3:]))
            offsets = list(map(float, client.AI_CAL_EOFF.split()[3:]))

            site_node = self.getNode(f"SITE{site}")
            for input_index in range(int(client.NCHAN)):
                input_node = site_node.getNode(f"INPUT_{input_index + 1:02}")
                input_node.COEFFICIENT.record = coefficients[input_index]
                input_node.OFFSET.record = offsets[input_index]

    _SECONDS_TO_NANOSECONDS = 1_000_000_000
    """One second in nanoseconds"""

    def _set_sync_role(self, uut):

        # Everything after the ; should not be trusted
        current_sync_role_parts = uut.s0.sync_role.split(';')[0].split(maxsplit=3)

        # Positional Arguments
        current_sync_role = current_sync_role_parts[0]
        current_frequency = int(current_sync_role_parts[1])

        # Keyword Arguments
        current_arguments = dict()
        if len(current_sync_role_parts) > 2:
            current_arguments = self._string_to_dict(current_sync_role_parts[2])

        requested_arguments = dict()

        trigger_source = str(self.TRIGGER.SOURCE.data()).upper()
        if trigger_source in self._TRIGGER_SOURCE_D0_OPTIONS:
            requested_arguments['TRG:DX'] = 'd0'
        elif trigger_source in self._TRIGGER_SOURCE_D1_OPTIONS:
            requested_arguments['TRG:DX'] = 'd1'

        changed = False

        requested_sync_role = str(self.SYNC_ROLE.data()).lower()
        if current_sync_role != requested_sync_role:
            changed = True
            self._log_info(f"Requested sync role of {requested_sync_role} differs from current configuration: {current_sync_role}")

        requested_frequency = int(self.FREQUENCY.data())
        if current_frequency != requested_frequency:
            changed = True
            self._log_info(f"Requested frequency of {requested_frequency} differs from current configuration: {current_frequency}")

        if current_arguments != requested_arguments:
            changed = True
            self._log_info(f"Requested sync role arguments '{self._dict_to_string(requested_arguments)}' differ from current configuration: '{self._dict_to_string(current_arguments)}'")

        if changed:
            self._log_info('Reconfiguring sync role, this may take some time.')
            uut.s0.sync_role = f"{self.SYNC_ROLE.data()} {int(self.FREQUENCY.data())} {self._dict_to_string(requested_arguments)}"

        # snyc_role will set a default trigger source, so we need to set these after
        self._log_info(f"Setting trigger source of timing highway {requested_arguments['TRG:DX']} to {trigger_source}")
        if requested_arguments['TRG:DX'] == 'd0':
            uut.s0.SIG_SRC_TRG_0 = trigger_source
        elif requested_arguments['TRG:DX'] == 'd1':
            uut.s0.SIG_SRC_TRG_1 = trigger_source

    def _set_hardware_decimation(self, uut):
        # HARD_DECIM only exists for 32-bit modules
        try:
            decimation = str(self.HARD_DECIM.data())
            self._log_info(f"Setting hardware decimation to {decimation}")

            for _, client in uut.modules.items():
                client.nacc = decimation

        except AttributeError:
            pass

    def _setup_wr(self, uut):

       # TODO: Check if this is even set
        motherboard_clock_rate = float(uut.s0.SIG_CLK_MB_FREQ.split(' ')[1])
        ns_per_tick = 1.0 / motherboard_clock_rate * self._SECONDS_TO_NANOSECONDS

        # This is done automatically at boot, but if you change the sync role while running, it needs to be recalculated
        uut.cC.WRTD_TICKNS = ns_per_tick

        try:
            ns_per_tick_node = self.WR.NS_PER_TICK
            ns_per_tick_node.record = float(ns_per_tick)

            uut.cC.WRTD_RX = int(self.WR.RX_ENABLE.on)
            uut.cC.WRTD_TX = int(self.WR.TX_ENABLE.on)

            try:
                uut.cC.WRTD_RX_MATCHES = str(self.WR.RX0_FILTER.data())
            except MDSplus.TreeNODATA:
                pass
            
            try:
                uut.cC.WRTD_RX_MATCHES1 = str(self.WR.RX1_FILTER.data())
            except MDSplus.TreeNODATA:
                pass

            uut.cC.wrtd_commit_rx = 1
            uut.cC.wrtd_commit_tx = 1

        except MDSplus.TreeNNF:
            pass

    def _set_signal_conditioning_gains(self, uut):
        import epics

        for site in list(map(int, uut.get_aggregator_sites())):
            client = uut.modules[site]

            self._log_info(f"Setting Signal Conditioning Gains and Offsets for site {site}")

            site_node = self.getNode(f"SITE{site}")
            epics_prefix = self.EPICS_NAME.data() + f":{site}:SC32:"

            for input_index in range(int(client.NCHAN)):
                input_node = site_node.getNode(f"INPUT_{input_index + 1:02}")

                pv = epics.PV(epics_prefix + f"G1:{input_index:02}")
                pv.put(str(input_node.SC_GAIN1.data()), wait=True)

                pv = epics.PV(epics_prefix + f"G2:{input_index:02}")
                pv.put(str(input_node.SC_GAIN2.data()), wait=True)

                pv = epics.PV(epics_prefix + f"OFFSET:{input_index:02}")
                pv.put(str(input_node.SC_OFFSET.data()), wait=True)

            self._log_verbose(f"Comitting Signal Conditioning Gains and Offsets for site {site}")

            pv = epics.PV(epics_prefix + 'GAIN:COMMIT')
            pv.put('1')

    def _setup_pulse_generators(self, uut):
        _DIO_MODELS = ['DIO482ELF', 'DIO482ELF_TD']

        for site, client in sorted(uut.modules.items()):
            site_node = self.device.getNode(f"SITE{site}")
            model = str(site_node.MODEL.data()) # or get it from the uut ?

            is_gpg = model == 'GPG ???'

            # if is_gpg:
                # client.GPG_ENABLE = 1
                # client.GPG_MODE   = 'LOOP'

                # client.GPG_TRG        = 'enable'
                # client.GPG_TRG_DX     = str(self.gpg_trg_dx.data())
                # client.GPG_TRG_SENSE  = 'rising'

                # self._log_info('Setting Trigger for GPG')

            if model in _DIO_MODELS:
                client.TRG        = 'enable'
                client.TRG_DX     = str(self.TRIGGER.WRTD_SOURCE.data())
                client.TRG_SENSE  = 'rising'

                self._log_info(f"Setting Trigger for DIO482 in site {site}")
                
            nchan = int(client.NCHAN)

            data_by_chan = []
            all_times = []

            for output_index in range(nchan):
                chan_node = site_node.getNode(f"OUTPUT_{output_index + 1:02}")

                times = chan_node.dim_of().data()
                data = chan_node.data()

                # Build dictionary of times -> states
                data_dict = dict(zip(times, data))

                data_by_chan.append(data_dict)
                all_times.extend(times)

            all_times = sorted(list(set(all_times)))

            # initialize the state matrix
            state_matrix = np.zeros((len(all_times), nchan), dtype='int')

            for c, data in enumerate(data_by_chan):
                for t, time in enumerate(all_times):
                    if time in data:
                        state_matrix[t][c] = data[time]
                    else:
                        state_matrix[t][c] = state_matrix[t - 1][c]

            # Building the string of 1s and 0s for each transition time:
            binary_rows = []
            times_usecs = []
            last_row = None
            for time, row in zip(all_times, state_matrix):
                if not np.array_equal(row, last_row):
                    rowstr = [ str(i) for i in np.flip(row) ]  # flipping the bits so that chan 1 is in the far right position
                    binary_rows.append(''.join(rowstr))
                    times_usecs.append(int(time * 1E7)) # Converting the original units of the transtion times in seconds, to 1/10th micro-seconds
                    last_row = row

            # TODO: depending on the hardware there is a limit number of states allowed.
            # The lines below limits the number of the CMOD's 1800 states table to just 510:
            # binary_rows = binary_rows[0:510]
            # times_usecs = times_usecs[0:510]

            # Write to a list with states in HEX form.
            stl  = ''
            for time, state in zip(times_usecs, binary_rows):
                stl += f"{time},{int(state, 2):08X}\n"

            self._log_verbose(f"STL Table for site {site}")
            self._log_verbose(stl)

            # What follows checks if the system is a GPG module (site 0) or a PG module (site 1..6)
            if is_gpg:
                uut.load_wrpg(stl)
                self._log_info('Uploaded STL for GPG')
            else:
                uut.load_dio482pg(site, stl)
                self._log_info(f"Uploaded STL for site {site}")

    class Monitor(threading.Thread):
        """Monitor thread for recording device temperature and voltage"""

        def __init__(self, device):
            super(ACQ2106.Monitor, self).__init__(name="Monitor")
            self.tree_name = device.tree.name
            self.tree_shot = device.tree.shot
            self.node_path = device.path

        def run(self):
            try:
                self.tree = MDSplus.Tree(self.tree_name, self.tree_shot)
                self.device = self.tree.getNode(self.node_path)

                uut = self.device._get_uut()
                if uut is None:
                    raise Exception(f"Unable to connect to digitizer ({self.device.ADDRESS.data()})")

                temp_node = self.device.TEMP
                temp_fpga_node = self.device.TEMP_FPGA

                site_temp_nodes = {}
                for site in range(1, self.device._MAX_SITES + 1):
                    name = f"SITE{site}"
                    try:
                        site_temp_nodes[name] = self.device.getNode(name).TEMP
                    except MDSplus.TreeNNF:
                        pass

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
            self.tree_name = reader.device.tree.name
            self.tree_shot = reader.device.tree.shot
            self.node_path = reader.device.path
            self.reader = reader

        def run(self):
            try:
                self.tree = MDSplus.Tree(self.tree_name, self.tree_shot)
                self.device = self.tree.getNode(self.node_path)

                uut = self.device._get_uut()
                if uut is None:
                    raise Exception(f"Unable to connect to digitizer ({self.device.ADDRESS.data()})")

                input_nodes = []
                resampled_nodes = []
                resample_factors = []
                software_decimations = []
                for site in list(map(int, uut.get_aggregator_sites())):
                    site_node = self.device.getNode(f"SITE{site}")
                    client = uut.modules[site]
                    for input_index in range(int(client.NCHAN)):
                        input_node = site_node.getNode(f"INPUT_{input_index + 1:02}")
                        input_nodes.append(input_node)
                        
                        resampled_nodes.append(input_node.RESAMPLED)
                        resample_factors.append(int(input_node.RES_FACTOR.data()))
                        software_decimations.append(int(input_node.SOFT_DECIM.data()))

                spad_nodes = []
                for spad_index in range(self.device._MAX_SPAD):
                    spad_node = self.device.SCRATCHPAD.getNode(f"SPAD{spad_index}")
                    if i < self.reader.nspad:
                        spad_nodes.append(spad_node)
                    else:
                        # Turn off the unused SPAD nodes
                        spad_node.on = False

                # HARD_DECIM only exists for 32-bit modules
                hardware_decimation = 1
                try:
                    hardware_decimation = int(self.device.HARD_DECIM.data())
                except AttributeError:
                    pass

                delta_time = float(1.0 / self.device.FREQUENCY.data() * hardware_decimation)
                time_at_0 = self.device.TRIGGER.TIME_AT_0.data()

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

                            begin = (segment_index * segment_length * input_delta_time) + time_at_0
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
                    self.device._log_info(f"Finished writing segment {segment_index}/{self.reader.segment_count}, took {benchmark_elapsed}s")
                    self.reader.empty_buffer_queue.put(buffer)

                    MDSplus.Event(event_name)

            except Exception as e:
                self.exception = e
                traceback.print_exc()

    class StreamReader(threading.Thread):

        def __init__(self, device):
            super(ACQ2106.StreamReader, self).__init__(name="StreamReader")
            self.tree_name = device.tree.name
            self.tree_shot = device.tree.shot
            self.node_path = device.path

        def run(self):
            import acq400_hapi
            from fractions import gcd

            try:
                self.tree = MDSplus.Tree(self.tree_name, self.tree_shot)
                self.device = self.tree.getNode(self.node_path)

                uut = self.device._get_uut()
                if uut is None:
                    raise Exception(f"Unable to connect to digitizer ({self.device.ADDRESS.data()})")

                self.full_buffer_queue = queue.Queue()
                self.empty_buffer_queue = queue.Queue()

                software_decimations = []
                for site in list(map(int, uut.get_aggregator_sites())):
                    site_node = self.device.getNode(f"SITE{site}")
                    client = uut.modules[site]
                    for input_index in range(int(client.NCHAN)):
                        input_node = site_node.getNode(f"INPUT_{input_index + 1:02}")
                        software_decimations.append(int(input_node.SOFT_DECIM.data()))

                # Determine how many extra SPAD channels there are
                # [0] is 1=enabled/0=disabled
                # [1] is the number of SPAD channels
                # [2] can be ignored
                spad_enabled, nspad, _ = uut.s0.spad.split(',')
                self.nspad = int(nspad) if spad_enabled == '1' else 0

                # All remaining channels are actual data
                self.nchan = uut.nchan() - self.nspad

                bytes_per_row = (self.nchan * uut.data_size()) + (self.nspad * 4) # SPAD channels are always 32 bit

                # Find the lowest common decimator
                decimator = 1
                for soft_decim in software_decimations:
                    decimator = int(decimator * soft_decim / gcd(decimator, soft_decim))
                self.device._log_info(f"Calculated a greatest common decimator of {decimator}")

                self.segment_length = int(self.device.STREAM.SEGLEN_CONF.data())
                self.segment_count = int(self.device.STREAM.SEG_COUNT.data())

                # If the decimator and segment length don't match, adjust the segment length
                if self.segment_length % decimator > 0:
                    old_segment_length = self.segment_length
                    self.segment_length = (self.segment_length // decimator + 1) * decimator
                    self.device._log_info(f"Adjusting segment length to match lowest common decimator, {old_segment_length} -> {self.segment_length}")

                    self.device.STREAM.SEGLEN_ACT.record = self.segment_length

                self.segment_size = self.segment_length * bytes_per_row

                self.writer = self.device.StreamWriter(self)
                self.writer.setDaemon(True)
                self.writer.start()

                # When TRIGGER.SOURCE is set to STRIG, opening the socket will actually trigger the device, so we have to do this last
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
                        self.device._log_verbose(f"No empty buffers available, creating new one of {self.segment_size} bytes")
                        buffer = bytearray(self.segment_size)

                    bytes_needed = self.segment_size
                    try:
                        view = memoryview(buffer)
                        while bytes_needed > 0:
                            bytes_read = self.socket.recv_into(view, bytes_needed)

                            # TODO: Make more accurate, possibly account for the time it took to read the segment, possibly use the SPAD
                            if first_recv:
                                first_recv = False
                                self.device.TRIGGER.TIMESTAMP.record = time.time()

                            view = view[bytes_read:]
                            bytes_needed -= bytes_read

                    except socket.timeout:
                        self.device._log_warning("Socket connection timed out, retrying")
                        self.empty_buffer_queue.put(buffer)
                        continue

                    except socket.error:
                        # TODO: Handle Partial Segments?
                        # self.full_buffer_queue.put(buffer[:self.segment_size - bytes_needed])
                        self.full_buffer_queue.put(None)
                        raise

                    else:
                        self.full_buffer_queue.put(buffer)

                        segment_index += 1
                        self.device._log_info(f"Finished reading segment {segment_index}/{self.segment_count}")

            except Exception as e:
                self.exception = e
                traceback.print_exc()

            # This will stop the digitizer from streaming
            self.socket.close()

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

    ###
    ### Streaming Methods
    ###

    def start_stream(self):
        if self.MODE.data().upper() != 'STREAM':
            raise Exception('Device is not configured for streaming. Set MODE to "STREAM" and then run configure().')

        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        self._init(uut)

        self.RUNNING.on = True

        monitor = self.Monitor(self)
        monitor.setDaemon(True)
        monitor.start()

        thread = self.StreamReader(self)
        thread.start()

    START_STREAM = start_stream

    def stop_stream(self):
        if self.MODE.data().upper() != 'STREAM':
            raise Exception('Device is not configured for streaming. Set MODE to "STREAM" and then run configure().')

        self.RUNNING.on = False

    STOP_STREAM = stop_stream

    def abort_stream(self):
        if self.MODE.data().upper() != 'STREAM':
            raise Exception('Device is not configured for streaming. Set MODE to "STREAM" and then run configure().')

        self.RUNNING.on = False
        
        # TODO:

    ABORT_STREAM = abort_stream

    ###
    ### Transient Methods
    ###

    def arm_transient(self):
        mode = str(self.MODE.data()).upper()
        if mode != 'TRANSIENT':
            raise Exception('Device is not configured for transient recording. Set MODE to "TRANSIENT" and then run configure().')

        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        self._init(uut)

        self.RUNNING.on = True

        monitor = self.Monitor(self)
        monitor.setDaemon(True)
        monitor.start()

        trigger_source = str(self.TRIGGER.SOURCE.data()).upper()
        if (trigger_source == 'STRIG'):
            uut.s0.TRANSIENT_SET_ARM = '1'

    ARM_TRANSIENT = arm_transient

    def store_transient(self):
        mode = str(self.MODE.data()).upper()
        if mode != 'TRANSIENT':
            raise Exception('Device is not configured for transient recording. Set MODE to "TRANSIENT" and then run configure().')

        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        self.RUNNING.on = False

        # Wait for post-processing to finish
        # TODO: Timeout?
        while uut.statmon.get_state() != 0:
            pass
        
        presamples = int(self.TRANSIENT.PRESAMPLES.data())
        postsamples = int(self.TRANSIENT.POSTSAMPLES.data())
        
        start_index = -presamples + 1
        end_index = postsamples
        
        clock_period = 1.0 / self.FREQUENCY.data()
        
        mds_window = MDSplus.Window(start_index, end_index, 0)
        mds_range = MDSplus.Range(None, None, clock_period)
        mds_dim = MDSplus.Dimension(mds_window, mds_range)

        raw_data = uut.read_channels()
        
        channel_offset = 0
        for site in list(map(int, uut.get_aggregator_sites())): # TODO: sorted() ?
            site_node = self.getNode(f"SITE{site}")
            client = uut.modules[site]
            nchan = int(client.NCHAN)
            
            for input_index in range(nchan):
                input_node = site_node.getNode(f"INPUT_{input_index + 1:02}")
                
                signal = MDSplus.Signal(raw_data[channel_offset + input_index], None, mds_dim)
                input_node.putData(signal)
            
            channel_offset += nchan
        
        # Determine how many extra SPAD channels there are
        # [0] is 1=enabled/0=disabled
        # [1] is the number of SPAD channels
        # [2] can be ignored
        spad_enabled, nspad, _ = uut.s0.spad.split(',')
        nspad = int(nspad) if spad_enabled == '1' else 0
            
        for spad_index in range(self._MAX_SPAD):
            spad_node = self.SCRATCHPAD.getNode(f"SPAD{spad_index}")
            if spad_index < nspad:
                signal = MDSplus.Signal(raw_data[channel_offset + spad_index], None, mds_dim)
                spad_node.putData(signal)
            else:
                # Turn off the unused SPAD nodes
                spad_node.on = False

        event_name = str(self.TRANSIENT.EVENT_NAME.data())
        MDSplus.Event(event_name)

    STORE_TRANSIENT = store_transient

    def soft_trigger(self):
        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        mode = str(self.MODE.data()).upper()
        if mode == 'TRANSIENT':
            uut.s0.soft_trigger = '1'
        elif mode == 'STREAM':
            uut.s0.CONTINUOUS = '1'

    SOFT_TRIGGER = soft_trigger

    # TODO: Improve based on user feedback
    def arm_pulse_generators(self):
        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        self._setup_pulse_generators(uut)

    ARM_PULSE_GENERATORS = arm_pulse_generators

    def wrtd_trigger(self):
        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        try:
            uut.cC.WRTD_ID = str(self.WR.TX_MESSAGE.data())
        except MDSplus.TreeNODATA:
            pass

        uut.s0.WR_TRG = 1
        uut.s0.WR_TRG_DX = str(self.TRIGGER.WRTD_SOURCE.data())

        for site, client in sorted(uut.modules.items()):
            site_node = self.device.getNode(f"SITE{site}")
            model = str(site_node.MODEL.data())

            if model == 'DIO482ELF_TD': # TIGA
                self._log_info(f"Configuring WR TIming Generator Appliance (TIGA) Triggers for site {site}")

                try:
                    client.WRTD_ID = str(site_node.TX_MESSAGE.data())
                except MDSplus.TreeNODATA:
                    pass
                
                client.WRTD_TX_MASK = (1 << (site + 1)) # TODO: Investigate

                client.TRG = 1
                client.TRG_DX = str(site_node.WRTD_SOURCE.data()) # TODO: Better name

    WRTD_TRIGGER = wrtd_trigger

    def send_wrtd_message(self, message):
        uut = self._get_uut()
        if uut is None:
            raise Exception(f"Unable to connect to digitizer ({self.ADDRESS.data()})")

        uut.cC.wrtd_txi = message

    SEND_WRTD_MESSAGE = send_wrtd_message

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
            self._log_warning(f"Undefined Operation Mode {mode}, unable to get state")

    GET_STATE = get_state

    def configure(self, tcl_arguments=None, **kwargs):
        import acq400_hapi
        import socket

        if not self.tree.isOpenForEdit():
            raise Exception('The tree must be open for edit in order to configure a device')

        # Parse any arguments from TCL into the standard kwargs
        # TODO: Case insensitive?
        if tcl_arguments is not None:
            kwargs.update(self._string_to_dict(tcl_arguments))

        overwrite_data = False
        if 'overwrite_data' in kwargs and self._to_bool(kwargs['overwrite_data']):
            overwrite_data = True
            
        delete_nodes = False
        if 'delete_nodes' in kwargs and self._to_bool(kwargs['delete_nodes']):
            delete_nodes = True

        # The list of all nodes added during configure() has to contain the root device node as well
        self._configure_nodes = [self]

        # Ensure that any new nodes in the parts array are taken care of, and add the original parts array to _configure_nodes
        self._add_parts(self.parts, overwrite_data)

        # Add the default parts that are not included in the parts array
        self._add_parts(self._default_parts, overwrite_data)

        # Ensure the RUNNING node is off by default
        # TODO: Add on/off to the parts array?
        self.RUNNING.on = False

        # General Configuration

        self.TRIGGER.SOURCE.record = str(self.TRIGGER.SOURCE.data()).upper()
        self.SYNC_ROLE.record = str(self.SYNC_ROLE.data()).lower()

        # Mode-Specific Configuration

        self.MODE.record = str(self.MODE.data()).upper()

        mode = self.MODE.data()
        if mode == 'STREAM':
            self._add_parts(self._stream_parts, overwrite_data)
        elif mode == 'TRANSIENT':
            self._add_parts(self._transient_parts, overwrite_data)

        # Determine if we are configuring Online or Offline

        online = True

        # If there is no address, we don't need to try to connect
        address = self.ADDRESS.getDataNoRaise()
        if address is None or address == '':
            self._log_warning('ADDRESS is blank, will configure offline.')
            online = False

        if online:
            # Attempt to resolve the name so we can give a better error
            try:
                socket.gethostbyname(str(address))
            except:
                self._log_warning(f"ADDRESS '{address}' failed to resolve to an IP, will configure offline.")
                online = False

        if online:
            # Attempt to connect to the device to avoid the long default timeout
            try:
                self._log_info(f"Testing connection to ADDRESS '{address}'...")
                test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                test_socket.settimeout(5)
                test_socket.connect((self.ADDRESS.data(), acq400_hapi.AcqPorts.SITE0))
                test_socket.close()
            except OSError:
                self._log_warning(f"Unable to connect to ADDRESS '{address}', will configure offline.")
                online = False

        uut = None
        if online:
            uut = self._get_uut()

        if uut is None:
            online = False

        # Online/Offline Configuration and Module-Specific Configuration
        has_tiga = False
        has_wr = False
        has_hudp = False
        data_size = 0
        site_parts = []
        aggregator_sites = []

        if online:
            # Online

            self.SERIAL.record = uut.s0.SERIAL

            self._log_info(f"Firmware: {uut.s0.software_version}")
            self.FIRMWARE.record = uut.s0.software_version

            self._log_info(f"FPGA Image: {uut.s0.fpga_version}")
            self.FPGA_IMAGE.record = uut.s0.fpga_version

            has_wr = (uut.s0.has_wr != 'none')
            if has_wr:
                self._log_info('Detected White Rabbit capabilities')

            # If any site has Signal Conditioning, assume they all do
            # TODO: Possibly improve?
            has_sc = False
            for site, client in sorted(uut.modules.items()):
                if not has_sc and 'ELFX32' in client.knobs:
                    has_sc = self._to_bool(client.ELFX32)
                    self._log_info(f"Detected Signal Conditioning capabilities in site {site}")

            modules = dict()
            for site, client in sorted(uut.modules.items()):
                model = client.MODEL.split(' ')[0]
                modules[site] = model

                self._log_info(f"Module found in site {site}: {model}")

                site_parts += self._get_parts_for_site(site, model, has_sc=has_sc)

            self.MODULES.record = self._dict_to_string(modules)

            data_size = uut.data_size()

            # aggregator_sites = list(map(int, uut.get_aggregator_sites()))

        else:

            # Offline

            self._log_info('Configuring offline, you will need to run configure with access to the ACQ at least once before running.')

            if 'has_wr' in kwargs and self._to_bool(kwargs['has_wr']):
                self._log_info('Assuming White Rabbit capabilities')
                has_wr = True

            if 'has_sc' in kwargs and self._to_bool(kwargs['has_sc']):
                self._log_info('Assuming Signal Conditioning capabilities')
                has_sc = True

            modules = self.MODULES.getDataNoRaise()
            if modules is None or modules == '':
                raise Exception(f"When configuring offline, you must specify the modules manually.")

            modules = self._string_to_dict(modules)

            for site in range(1, self._MAX_SITES + 1):
                if site in modules and modules[site] != '':
                    model = modules[site]
                    self._log_info(f"Module assumed to be in site {site}: {model}")

                    site_parts += self._get_parts_for_site(site, model, has_sc=has_sc)

                else:
                    print(f"No module assumed to be in site {site}")

            # TODO: Approximate uut.data_size()
            data_size = 4

            # TODO: Approximate uut.get_aggregator_sites
            # aggregator_sites =

        ###
        ### White Rabbit Nodes
        ###

        if has_wr:
            self._add_parts(self._wr_parts, overwrite_data)

        ###
        ### Signal Conditioning Nodes
        ###

        if has_sc:
            self._add_parts(self._sc_parts, overwrite_data)

        ###
        ### HUDP Nodes
        ###

        has_hudp = False
        if has_hudp:
            pass

        ###
        ### 32bit-only Nodes
        ###

        if data_size == 4:
            self._add_parts(self._32bit_parts, overwrite_data)

        ###
        ### Site Nodes
        ###

        self._add_parts(site_parts)

        ###
        ### Aggregated Inputs
        ###

        # TODO: Replace with Node Hardlinks when available

        # all_inputs = []
        # for site in aggregator_sites:
        #     model = modules[site]
        #     info = self._get_module_info(model)

        #     site_node = self.getNode(f"SITE{site}")
        #     for input in range(info['nchan']):
        #         input_node = site_node.getNode(f"INPUT_{input + 1:02}")
        #         all_inputs.append(input_node)

        # self._log_info(f"Found a total of {len(all_inputs)} aggregated inputs")

        # if len(all_inputs) > 0:
        #     input_parts = [
        #         {
        #             'path': ':INPUTS',
        #             'type': 'structure',
        #         }
        #     ]
        #     for input_index, input_node in enumerate(all_inputs):
        #         input_path = f":INPUTS:INPUT_{input_index + 1:03}"
        #         input_parts += [
        #             {
        #                 'path': input_path,
        #                 'type': 'signal',
        #                 'value': input_node,
        #             },
        #             {
        #                 'path': input_path + ':RESAMPLED',
        #                 'type': 'signal',
        #                 'value': input.RESAMPLED,
        #             },
        #         ]
        #     self._add_parts(input_parts, overwrite_data)

        # print(f"Found a total of {len(self._configure_outputs)} inputs")

        # if len(self._configure_outputs) > 0:
        #     output_parts = [
        #         {
        #             'path': ':OUTPUTS',
        #         }
        #     ]
        #     for output_index, output_node in enumerate(self._configure_outputs):
        #         output_parts.append({
        #             'path': f"OUTPUTS:OUTPUT_{output_index + 1:03}"
        #             'type': 'signal',
        #             'value': output_node,
        #         })
        #     self._add_parts(output_parts, overwrite_data)

        # TODO: SPAD

        # TODO: Compare and delete self._configure_nodes
        all_nids = self.getNodeWild('***').data()
        new_nids = [ node.nid for node in self._configure_nodes ]

        bad_nids = set(all_nids) - set(new_nids)
        bad_paths = [ str(MDSplus.TreeNode(nid, self.tree).path) for nid in bad_nids ]
        
        if delete_nodes:
            for path in bad_paths:
                try:
                    self._log_info(f"Removing {path}")
                    node = self.tree.getNode(path)
                    node.delete()
                except MDSplus.TreeNNF:
                    # node.delete() deletes all child nodes as well, so the node may already be gone
                    pass
                except MDSplus.SsSUCCESS:
                    # TODO: For some reason, node.delete() always throws this
                    pass
        else:
            for path in bad_paths:
                self._log_info(f"Disabling {path}")
                node = self.tree.getNode(path)
                node.on = False
            

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
            self._log_warning('The Firmware Version has changed, it is recommended that you run configure() again.')

        fpga_image = uut.s0.fpga_version
        if fpga_image != self.FPGA_IMAGE.data():
            self._log_warning('The FPGA image has changed, it is recommended that you run configure() again.')

        # Verify the configured modules

        modules = dict()
        for site, client in uut.modules.items():
            model = client.MODEL.split(' ')[0]
            modules[site] = model

        if self._string_to_dict(self.MODULES.data()) != modules:
            raise Exception('The modules in the device have changed since the last call to configure(). You must run configure() again.')

        # Verify the values of all nodes

        # if self.HARD_DECIM.data() > 16 and self.FREQUENCY.data() < 10000:
        #     self._log_warning('Using Hardware Decimation > 16 with a Frequency of < 10000 will cause data loss.')

        for node in self.getNodeWild('***'):
            self._log_verbose(f"Verifying configuration for {node.path}")

            if node.usage not in [ 'NUMERIC', 'TEXT' ]:
                continue

            try:
                value = node.data()
            except MDSplus.TreeNODATA:
                continue

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

        wr_node_exists = True
        try:
            self.getNode('WR')

        except MDSplus.TreeNNF:
            wr_node_exists = False

        if wr_node_exists:
            if uut.s0.has_wr == 'none':
                raise Exception('The last call to configure() had White Rabbit enabled, but the current FPGA Image does not support White Rabbit. You must run configure() again or change your FPGA Image and reboot.')

    VERIFY = verify

    ###
    ### Helper Methods
    ###

    def _log_verbose(self, format, *args):
        self.dprint(4, format, *args)

    def _log_info(self, format, *args):
        self.dprint(3, format, *args)

    def _log_warning(self, format, *args):
        self.dprint(2, format, *args)

    def _log_error(self, format, *args):
        self.dprint(1, format, *args)

    def _get_uut(self):
        import acq400_hapi
        return acq400_hapi.factory(self.ADDRESS.data())

    # TODO: Move this into MDSplus.Device?
    def _add_parts(self, parts, overwrite_data=False):
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
                self._log_verbose(f"Adding {node.path}")
            except MDSplus.mdsExceptions.TreeALREADY_THERE:
                node = self.getNode(part['path'])
                self._log_verbose(f"Found {node.path}")

        # Then you can reference them in valueExpr
        for part in parts:
            node = self.getNode(part['path'])
            self._configure_nodes.append(node)

            # TODO: Port this back into Tree.py
            eval_globals['node'] = node

            usage = part.get('type', 'none').upper()
            if node.getUsage() != usage:
                node.setUsage(usage)

            if 'options' in part:
                # print(f"Setting options of {node.path}: {part['options']}")
                for option in part['options']:
                    node.__setattr__(option, True)

            # HACK: Part of a hack to circumvent XNCI's from destroying data
            # Can be removed after https://github.com/MDSplus/mdsplus/pull/2498 is merged
            try:
                data = node.getDataNoRaise()
            except MDSplus.TreeBADRECORD:
                pass

            if 'ext_options' in part:
                # HACK: If no_write_model is set, you are unable to set XNCIs, so we temporarily disable it
                no_write_model = node.no_write_model
                node.no_write_model = False

                if isinstance(part['ext_options'], dict):
                    for option, value in part['ext_options'].items():
                        node.setExtendedAttribute(option, value)

                node.no_write_model = no_write_model

            # HACK: For things that run a function in valueExpr, the data will still be None even if we don't want it to run again
            if overwrite_data or node.time_inserted == 0:
                if 'value' in part:
                    self._log_verbose(f"Setting value of {node.path} to: {str(part['value'])}")
                    node.record = part['value']
                elif 'valueExpr' in part:
                    self._log_verbose(f"Setting value of {node.path} to expression: {part['valueExpr']}")
                    node.record = eval(part['valueExpr'], eval_globals)

            # If we are not overwriting the data, set it back to the original
            elif data is not None:
                node.record = data

    def _set_input_segment_scale(self, node, coefficient_node, offset_node):
        node.setSegmentScale(
            MDSplus.ADD(
                MDSplus.MULTIPLY(
                    coefficient_node,
                    MDSplus.dVALUE()
                ),
                offset_node
            )
        )

    def _to_bool(self, value):
        if value is str:
            return value.lower() in [ "1", "true" ]
        return bool(value)

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


class ACQ1001(ACQ2106):
    _MAX_SITES = 1
