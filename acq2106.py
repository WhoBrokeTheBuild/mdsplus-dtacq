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

import MDSplus
import numpy as np
import socket
import threading
import time

class ACQ2106(MDSplus.Device):
    """
    """
    
    parts = [
        {
            'path': ':COMMENT',
            'type': 'text',
            'options': ('no_write_shot',)
        },
        {
            'path': ':ADDRESS',
            'type': 'text',
            'value': '192.168.0.254',
            'options': ('no_write_shot',)
        },
        {
            'path': ':EPICS_NAME',
            'type': 'text',
            'value': 'acq2106_xxx',
            'options': ('no_write_shot',)
        },
        {
            'path': ':RUNNING',
            'type': 'numeric',
            'options': ('no_write_model',)
        },
        {
            'path': ':SERIAL',
            'type': 'any', # ???
            'options': ('no_write_shot',)
        },
        {
            'path': ':FIRMWARE',
            'type': 'any', # ???
            'options': ('no_write_shot',)
        },
        {
            'path': ':FPGA_IMAGE',
            'type': 'any', # ???
            'options': ('no_write_shot',)
        },
        {
            'path': ':TRIG_TIME',
            'type': 'numeric',
            'options': ('write_shot',)
        },
        {
            'path': ':SYNC_ROLE',
            'type': 'text',
            'options': ('no_write_shot',),
            'values': [
                'master', 'slave', 'solo', 'fpmaster', 'rpmaster'
            ]
        },
        {
            'path': ':TRIG_SOURCE',
            'type': 'text',
            'options': ('no_write_shot',),
            'values': [
                'ext', 'hdmi', 'gpg0', 'wrtt0', # d0, hard
                'strig', 'hdmi_gpio', 'gpg1', 'fp_sync', 'wrtt1', #d1, soft-ish
            ]
        },
        {
            'path': ':MODULES',
            'type': 'text',
            'options': ('no_write_shot',),
        },
        {
            'path': ':HARD_DECIM',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
        },
        {
            'path': ':SOFT_DECIM',
            'type': 'numeric',
            'value': 1,
            'options': ('no_write_shot',),
        },
        {
            'path': ':RES_FACTOR',
            'type': 'numeric',
            'value': 100,
            'options': ('no_write_shot',),
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
            'options': ('no_write_shot',)
        },
        {
            'path': ':WRTD:TX_DELTANS',
            'type': 'numeric',
            'value': 50000000,
            'options': ('no_write_shot',)
        },
        {
            'path': ':WRTD:RX0_FILTER',
            'type': 'text',
            'options': ('no_write_shot',)
        },
        {
            'path': ':WRTD:RX1_FILTER',
            'type': 'text',
            'options': ('no_write_shot',)
        },
    ]
    
    MAX_SITES = 6
    
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
    
    def init(self):
        uut = self._getUUT()
        
        sync_role_parts = uut.s0.sync_role.split()
        current_sync_role = sync_role_parts[0]
        current_frequency = int(sync_role_parts[1])
        current_ = sync_role_parts[0]
        
        sync_role = '%s %d TRG:DX=%s' % (self.SYNC_ROLE, self.FREQ)
        
    INIT = init
    
    def configure(self, mode='streaming', modules=None, has_wr=False):
        if not self.tree.isOpenForEdit():
            raise Exception('The tree must be open for edit in order to configure a device')
        
        self.RUNNING.on = False
        
        self._configure_nodes = []
        self._configure_inputs = []
        self._configure_outputs = []
        
        self._configure_nodes.append(self)
        for part in self.parts:
            self._configure_nodes.append(self.getNode(part['path']))
        
        if modules:
            print('Configuring offline, you will need to run configure with access to the ACQ at least once before running.')
            
            if not isinstance(modules, list):
                modules = modules.split(',')
            
            if len(modules) != self.MAX_SITES:
                raise Exception('When configuring offline, you must specify %d modules' % (self.MAX_SITES,))
                
            for site in range(1, self.MAX_SITES + 1):
                model = modules[site - 1]
                if model:
                    print('Module assumed to be in site %d: %s' % (site, model,))
                
                    parts = self._getPartsForSite(site, model)
                    self._addParts(parts)
                
                else:
                    print('No module assumed to be in site %d' % (site,))
                
            self.MODULES.record = ','.join(modules).upper()
            
            if has_wr:
                print('Assuming White Rabbit capabilities')
        else:
            uut = self._getUUT()
            
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
                    
                    parts = self._getPartsForSite(site, model)
                    self._addParts(parts)
                    
                    siteNode = self._getSite(site)
                    siteNode.SERIAL.record = client.SERIAL
                
                else:
                    print('No module found in site %d' % (site,))
                
            self.MODULES.record = ','.join(modules)
            
            has_wr = (uut.s0.has_wr != 'none')
            if has_wr:
                print('Detected White Rabbit capabilities')
        
        if has_wr:
            self._addParts(self.wrtd_parts)
        
        has_tiga = False
        if has_tiga:
            pass
        
        has_hudp = False
        if has_hudp:
            pass
        
        print('Found a total of %d inputs' % (len(self._configure_inputs),))
        
        if len(self._configure_inputs) > 0:
            input_parts = [
                {
                    'path': ':INPUTS',
                }
            ]
            for i, input in enumerate(self._configure_inputs):
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
            self._addParts(input_parts)
            
        print('Found a total of %d inputs' % (len(self._configure_outputs),))
        
        if len(self._configure_outputs) > 0:
            output_parts = [
                {
                    'path': ':OUTPUTS',
                }
            ]
            for i, output in enumerate(self._configure_outputs):
                output_parts.append({
                    'path': 'OUTPUTS:OUTPUT_%03d' % (i + 1,),
                    'type': 'signal',
                    'value': output,
                })
            self._addParts(output_parts)
        
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
        
        uut = self._getUUT()
        
        fpga_image = uut.s0.fpga_version
        if fpga_image != self.FPGA_IMAGE.data():
            raise Exception('The FPGA image has changed, you must edit the tree and run configure() again')
        
        # TODO: Verify modules against FPGA Image
        
    VERIFY = verify
    
    def _getUUT(self):
        import acq400_hapi
        return acq400_hapi.factory(self.ADDRESS.data())
        
    def _getSite(self, site):
        try:
            return self.getNode('SITE%d' % (site,))
        except:
            return None
    
    def _addParts(self, parts, overwrite=False):
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
                # print('Setting extended options of %s: %s' % (node.path, part['ext_options'],))
                for option, value in part['ext_options'].items():
                    node.setExtendedAttribute(option, value)
            
            data = node.getData(None)
            if data == None or overwrite:
                if 'value' in part:
                    print('Setting value of %s to: %s' % (node.path, str(part['value']),))
                    node.record = part['value']
                elif 'valueExpr' in part:
                    print('Setting value of %s to expression: %s' % (node.path, part['valueExpr'],))
                    node.record = eval(part['valueExpr'], eval_globals)
            
            if 'input' in part and part['input']:
                self._configure_inputs.append(node)
            
            if 'output' in part and part['output']:
                self._configure_outputs.append(node)
            
    def _setChannelSegmentScale(self, node, coefficient, offset):
        node.setSegmentScale(
            MDSplus.ADD(
                MDSplus.MULTIPLY(
                    coefficient,
                    MDSplus.dVALUE()
                ),
                offset
            )
        )
    
    def _getPartsForSite(self, site, model):
        site_path = ':SITE%d' % (site,)
        parts = [
            {
                'path': site_path,
                'type': 'structure'
            },
            {
                'path': site_path + ':SERIAL',
                'type': 'text',
            }
        ]
        
        if model == 'ACQ435ELF' or model == 'ACQ423ELF':
            for i in range(32):
                input_path = site_path + ':INPUT_%02d' % (i + 1,)
                parts += [
                    {
                        'path': input_path,
                        'type': 'signal',
                        'valueExpr': 'head._setChannelSegmentScale(node, node.COEFFICIENT, node.OFFSET)',
                        'input': True,
                    },
                    {
                        'path': input_path + ':COEFFICIENT',
                        'type': 'numeric',
                        'options':('no_write_model', 'write_once',)
                    },
                    {
                        'path': input_path + ':OFFSET',
                        'type': 'numeric',
                        'options':('no_write_model', 'write_once',)
                    },
                    {
                        'path': input_path + ':RESAMPLED',
                        'type': 'signal',
                        'valueExpr': 'head._setChannelSegmentScale(node, node.parent.COEFFICIENT, node.parent.OFFSET)'
                    },
                    {
                        'path': input_path + ':RES_FACTOR',
                        'type': 'numeric',
                        'valueExpr': 'head.RES_FACTOR'
                    },
                    {
                        'path': input_path + ':SOFT_DECIM',
                        'type': 'numeric',
                        'valueExpr': 'head.SOFT_DECIM',
                        'options':('no_write_shot',)
                    }
                ]
                
        elif model == 'ACQ424ELF':
            for i in range(32):
                parts.append({
                    'path': ':SITE%d:OUTPUT_%02d' % (site, i + 1,),
                    'type': 'signal',
                    'output': True,
                })
        
        else:
            raise Exception('Unknown module %s in site %d' % (model, site,))
        
        return parts
    