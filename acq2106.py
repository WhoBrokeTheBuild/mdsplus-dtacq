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
    """
    """

    ###
    ### Constants
    ###

    MAX_SITES = 6
    
    MODE_OPTIONS = [
        'STREAM',       # Streaming
        'TRANSIENT',    # Transient
        'SLOWFAST',     # Slow Monitor + Multi-Event
    ]

    # Trigger Source Options for Signal Highway d0
    TRIGGER_SOURCE_D0_OPTIONS = [
        'EXT',          # External Trigger
        'HDMI',         # HDMI Trigger
        'GPG0',         # Gateway Pulse Generator Trigger
        'WRTT0'         # White Rabbit Trigger
    ]

    # Trigger Source Options for Signal Highway d1
    TRIGGER_SOURCE_D1_OPTIONS = [
        'STRIG',        # Software Trigger
        'HDMI_GPIO',    # HDMI General Purpose I/O Trigger
        'GPG1',         # Gateway Pulse Generator Trigger
        'FP_SYNC',      # Front Panel SYNC
        'WRTT1'         # White Rabbit Trigger
    ]

    SYNC_ROLE_OPTIONS = [
        'master',       # Master
        'slave',        # Slave
        'solo',         # Solo
        'fpmaster',     # Front-Panel Master
        'rpmaster'      # Rear-Panel Master
    ]

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
                'values': MODE_OPTIONS,
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
                'values': SYNC_ROLE_OPTIONS,
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
                'values': TRIGGER_SOURCE_D0_OPTIONS + TRIGGER_SOURCE_D1_OPTIONS,
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
            'path': ':HARD_DECIM',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Default Hardware Decimation (NACC), can be overridden per-site. Computed on the digitizer by averaging every N samples together. 1 = Disabled.',
                'min': 1,
                'max': 32,
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
    ]
    
    stream_parts = [
        {
            'path': ':STREAM',
            'type': 'structure',
        },
        {
            'path': ':STREAM:RUNNING',
            'type': 'numeric',
            'options': ('no_write_model',),
            'ext_options': {
                'tooltip': 'On if running, Off otherwise.',
            },
        },
        {
            'path': ':STREAM:SEG_LENGTH',
            'type': 'numeric',
            'value': 8000,
            'options': ('no_write_shot',),
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
                'tooltip': 'Segment Count, number of segments to capture before stopping.',
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
            'path': ':WRTD:TX_MESSAGE',
            'type': 'text',
            'options': ('no_write_shot',),
            'ext_options': {
                'tooltip': 'Message to send with WRTD when triggered.',
            },
        },
        {
            'path': ':WRTD:TX_DELTANS',
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
                'path': site_path + ':SERIAL',
                'type': 'text',
                'ext_options': {
                    'tooltip': 'Serial Number of the card in this site.',
                },
            },
            {
                'path': site_path + ':HARD_DECIM',
                'type': 'numeric',
                'valueExpr': 'head.HARD_DECIM',
                'ext_options': {
                    'tooltip': 'Hardware Decimation (NACC) for this site. Computed on the digitizer by averaging every N samples together. 1 = Disabled.',
                    'min': 1,
                    'max': 32,
                },
            }
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


    def help(self):
        print("""
            This is a self-configuring device. First, fill in the :ADDRESS node with the IP or DNS of the AC2106.
            Next, open the tree for edit and run the following method:

                do /method NODE configure

            When you run configure(), it will record the Firmware version, FPGA Image, and all of the connected modules.
            If either the FPGA Image or the connected modules change, you must edit the tree and run configure again.
            If you attempt to run the device with a different FPGA Image or modules, it will error and fail.

        """)
    HELP = help

    def setup(self):
        ds = DeviceSetup(self)
        ds.run()
    SETUP = setup

    def _get_calibration(self, uut):
        # In order to get the calibration per-site, we cannot use uut.fetch_all_calibration()
        for site in list(map(int, uut.get_aggregator_sites())):
            client = uut.modules[site]

            print('Reading Calibration for site %d' % (site,))

            coefficients = list(map(float, client.AI_CAL_ESLO.split()[3:]))
            offsets = list(map(float, client.AI_CAL_EOFF.split()[3:]))

            site_node = self.getNode('SITE%d' % (site,))
            for i in range(int(client.NCHAN)):
                input_node = site_node.getNode('INPUT_%02d' % (i + 1,))
                input_node.COEFFICIENT.record = coefficients[i]
                input_node.OFFSET.record = offsets[i]

    def _set_sync_role(self, uut):
        # USAGE sync_role {fpmaster|rpmaster|master|slave|solo} [CLKHZ] [FIN]
        # modifiers [CLK|TRG:SENSE=falling|rising] [CLK|TRG:DX=d0|d1]
        # modifiers [TRG=int|ext]
        # modifiers [CLKDIV=div]

        # Everything after the ; should not be trusted
        sync_role_parts = uut.s0.sync_role.split(';')[0].split()

        # TODO: FIN?

        # Positional Arguments
        current_sync_role = sync_role_parts[0]
        current_frequency = int(sync_role_parts[1])

        # Keyword Arguments
        current_trigger_intext = ''     # TRG=int|ext
        current_clock_dx = ''           # CLK:DX=d0|d1
        current_trigger_dx = ''         # TRG:DX=d0|d1
        current_clock_sense = ''        # CLK:SENSE=falling|rising
        current_trigger_sense = ''      # TRG:SENSE=falling|rising
        current_clockdiv = ''           # CLKDIV=div
        for part in sync_role_parts[2:]:
            if part.startswith('TRG='):
                current_trigger_intext = part
            elif part.startswith('CLK:DX='):
                current_clock_dx = part
            elif part.startswith('TRG:DX='):
                current_trigger_dx = part
            elif part.startswith('CLK:SENSE='):
                current_clock_sense = part
            elif part.startswith('TRG:SENSE='):
                current_trigger_sense = part
            elif part.startswith('CLKDIV='):
                current_clockdiv = part

        trigger_source = str(self.TRIG_SOURCE.data()).upper()

        trigger_dx = ''
        if trigger_source in self.TRIGGER_SOURCE_D0_OPTIONS:
            trigger_dx = 'TRG:DX=d0'
        elif trigger_source in self.TRIGGER_SOURCE_D1_OPTIONS:
            trigger_dx = 'TRG:DX=d1'

        current = [
            current_sync_role,
            current_frequency,
            current_trigger_dx,
            current_clock_dx,
            current_trigger_sense,
            current_clock_sense,
            current_trigger_intext,
            current_clockdiv,
        ]

        requested = [
            self.SYNC_ROLE.data(),
            self.FREQUENCY.data(),
            trigger_dx,
            '',
            '',
            '',
            '',
            '',
        ]

        if current != requested:
            print('The requested sync role differs from the current ACQ configuration. Reconfiguring, this may take some time.')
            uut.s0.sync_role = ' '.join(str(part) for part in requested)
            
        # snyc_role will set a default trigger source, so we need to set these after
        if trigger_dx == 'TRG:DX=d0':
            uut.s0.SIG_SRC_TRG_0 = trigger_source
        elif trigger_dx == 'TRG:DX=d1':
            uut.s0.SIG_SRC_TRG_1 = trigger_source
            
    def _set_hardware_decimation(self, uut):
        # TODO: Allow configuring HARD_DECIM per-site
        
        for site, client in uut.modules.items():
            site_node = self.getNode('SITE%d' % (site,))
            decimation = site_node.HARD_DECIM.data()
            print('Setting hardware decimation for site %d to %d' % (site, decimation,))
            client.nacc = str(decimation)

    class Stream(threading.Thread):
        class Writer(threading.Thread):
            def __init__(self, stream, full_queue, empty_queue):
                super(ACQ2106.Stream.Writer, self).__init__(name="Stream.Writer")
                self.stream = stream
                self.full_queue = full_queue
                self.empty_queue = empty_queue


            def run(self):
                try:
                    while True:
                        try:
                            buffer = self.queue.get(block=True, timeout=1)
                        except queue.Empty:
                            if self.stream.on:
                                continue
                            break

                        if buffer is None:
                            break



                except Exception as e:
                    self.exception = e
                    traceback.print_exc()

        def __init__(self, device):
            super(ACQ2106.Stream, self).__init__(name="Stream")
            self.device = device.copy()
            self.stopped = threading.Event()

        def stop(self):
            self.stopped.set()

        @property
        def on(self):
            return not self.stopped.is_set()

        def run(self):
            import acq400_hapi

            try:
                self.device.tree.open()
                
                self.device.STREAM.RUNNING.on = True

                self.queue = queue.Queue()
                self.writer = self.Writer(self, queue)
                self.writer.start()

                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.connect((self.device.ADDRESS.data(), acq400_hapi.AcqPorts.STREAM))
                self.socket.settimeout(6) # TODO: Investigate

                try:
                    while self.writer.is_alive():
                        pass

                finally:
                    self.writer.join()
                    if hasattr(self.writer, "exception"):
                        self.exception = self.writer.exception

            except Exception as e:
                self.exception = e
                traceback.print_exc()

    def init(self):
        uut = self._get_uut()

        self._get_calibration(uut)
        self._set_sync_role(uut)
        self._set_hardware_decimation(uut)
        
        mode = self.MODE.data().upper()
        if mode == 'STREAM':
            pass
            # thread = self.StreamClient(self)
            # thread.start()
        elif mode == 'TRANSIENT':
            pass
    
    INIT = init
    
    # def stop(self):
    #     self.RUNNING.on = False

    # STOP = stop

    def get_state(self):
        uut = self._get_uut()

        mode = self.MODE.data().upper()
        if mode == 'STREAM':
            stream_state = uut.s0.CONTINUOUS_STATE.split()[1]
            print('State: %s' % (stream_state,))
        elif mode == 'TRANSIENT':
            transient_state = uut.s0.TRANS_ACT_STATE.split()[1]
            print('State: %s' % (transient_state,))
        else:
            print('Undefined Operation Mode %s, unable to get state' % (mode,))

    GET_STATE = get_state

    def configure(self, mode=None, modules=None, has_wr=False):
        if not self.tree.isOpenForEdit():
            raise Exception('The tree must be open for edit in order to configure a device')

        self._configure_nodes = []
        # self._configure_inputs = []
        # self._configure_outputs = []

        self._configure_nodes.append(self)
        for part in self.parts:
            self._configure_nodes.append(self.getNode(part['path']))
            
        if mode:
            mode = str(mode).upper()
            if mode not in self.MODE_OPTIONS:
                raise Exception('Invalid Operation Mode %s, must be one of: %s' % (mode, self.MODE_OPTIONS,))
            
            self.MODE.record = mode

        if modules:
            ###
            ### Offline
            ###

            print('Configuring offline, you will need to run configure with access to the ACQ at least once before running.')

            if not isinstance(modules, list):
                modules = modules.split(',')

            if len(modules) != self.MAX_SITES:
                raise Exception('When configuring offline, you must specify %d modules' % (self.MAX_SITES,))

            for site in range(1, self.MAX_SITES + 1):
                model = modules[site - 1]
                if model:
                    print('Module assumed to be in site %d: %s' % (site, model,))

                    parts = self._get_parts_for_site(site, model)
                    self._add_parts(parts)

                else:
                    print('No module assumed to be in site %d' % (site,))

            self.MODULES.record = ','.join(modules).upper()

            if has_wr:
                print('Assuming White Rabbit capabilities')

        else:
            ###
            ### Online
            ###

            uut = self._get_uut()

            self.SERIAL.record = uut.s0.SERIAL

            print('Firmware:', uut.s0.software_version)
            self.FIRMWARE.record = uut.s0.software_version

            print('FPGA Image:', uut.s0.fpga_version)
            self.FPGA_IMAGE.record = uut.s0.fpga_version

            modules = [''] * self.MAX_SITES
            for site in range(1, self.MAX_SITES + 1):
                if site in uut.modules:
                    client = uut.modules[site]

                    model = client.MODEL.split(' ')[0]

                    print('Module found in site %d: %s' % (site, model,))
                    modules[site - 1] = model

                    parts = self._get_parts_for_site(site, model)
                    self._add_parts(parts)

                    siteNode = self.getNode('SITE%d' % (site,))
                    siteNode.SERIAL.record = client.SERIAL

                else:
                    print('No module found in site %d' % (site,))

            self.MODULES.record = ','.join(modules)

            has_wr = (uut.s0.has_wr != 'none')
            if has_wr:
                print('Detected White Rabbit capabilities')
                
        ###
        ### Mode-Specific
        ###
        
        mode = self.MODE.data().upper()
        if mode == 'STREAM':
            self._add_parts(self.stream_parts)
            self.STREAM.RUNNING.on = False
        elif mode == 'TRANSIENT':
            self._add_parts(self.transient_parts)

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

        all_inputs = []
        for site in list(map(int, uut.get_aggregator_sites())):
            client = uut.modules[site]

            site_node = self.getNode('SITE%d' % (site,))
            for i in range(int(client.NCHAN)):
                input_node = site_node.getNode('INPUT_%02d' % (i + 1,))
                all_inputs.append(input_node)

        print('Found a total of %d aggregated inputs' % (len(all_inputs),))

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
        for nid in bad_nids:
            node = MDSplus.TreeNode(nid, self.tree)
            print('Removing %s' % (node.path,))

    CONFIGURE = configure

    def verify(self):

        uut = self._get_uut()

        fpga_image = uut.s0.fpga_version
        if fpga_image != self.FPGA_IMAGE.data():
            raise Exception('The FPGA image has changed, you must edit the tree and run configure() again')

        # TODO: Verify modules against FPGA Image

    VERIFY = verify

    def _get_uut(self):
        import acq400_hapi
        return acq400_hapi.factory(self.ADDRESS.data())

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
                print('Adding %s' % (node.path,))
            except MDSplus.mdsExceptions.TreeALREADY_THERE:
                node = self.getNode(part['path'])
                print('Found %s' % (node.path,))

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

            if 'ext_options' in part:
                # If no_write_model is set, you are unable to set XNCIs, so we temporarily disable it
                no_write_model = node.no_write_model
                node.no_write_model = False

                if isinstance(part['ext_options'], dict):
                    for option, value in part['ext_options'].items():
                        node.setExtendedAttribute(option, value)

                node.no_write_model = no_write_model

            data = node.getData(None)
            if data == None or overwrite:
                if 'value' in part:
                    print('Setting value of %s to: %s' % (node.path, str(part['value']),))
                    node.record = part['value']
                elif 'valueExpr' in part:
                    print('Setting value of %s to expression: %s' % (node.path, part['valueExpr'],))
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
