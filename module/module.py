#!/usr/bin/python

# -*- coding: utf-8 -*-

# Copyright (C) 2009-2014:
#    Gabes Jean, naparuba@gmail.com
#    Karfusehr Andreas, frescha@unitedseed.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.


# This Class is a plugin for the Shinken Broker. It is in charge
# to brok log into the Elasticsearch

import rawes
from rawes.elastic_exception import ElasticException
import re
import sys
from time import gmtime, strftime

from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['broker'],
    'type': 'es-log',
    'external': False,
    'phases': ['running'],
}


# called by the plugin manager to get a broker
def get_instance(plugin):
    name = plugin.get_name()
    elasticsearch_uri = plugin.elasticsearch_uri
    index = plugin.index
    doc_type  = plugin.doc_type

    logger.info("[ES Log] Get a Elasticsearch broker for plugin %s" % (name))
    instance = ESlog_broker(plugin)
    return instance


# Class for the ES Log Broker
# Get log broks and send them to Elasticsearch
class ESlog_broker(BaseModule):
    def __init__(self, modconf):
        BaseModule.__init__(self, modconf)
        self.elasticsearch_uri = getattr(modconf, 'elasticsearch_uri', None)
        self.index = getattr(modconf, 'index', 'shinken')
        self.doc_type = getattr(modconf, 'doc_type', 'shinken')

    def manage_log_brok(self, b):
        es = rawes.Elastic(self.elasticsearch_uri)
        constructor = self.index + '/' + self.doc_type

        logger.debug("[ES Log] Module is loaded")

        data = b.data
        line = data['log']
        
        if re.search("^\[[0-9]*\]", line):
            logger.debug("[ES Log] Non extensive data")

            try:
                SearchStr = '^\[(.*)\] (INFO|WARNING|ERROR|DEBUG)\: \[(.*)\](.*)$'
                matchObj = re.search(SearchStr.decode('utf-8'), line.decode('utf-8'), re.I | re.U)

                es.post(constructor, data={
                    'datetime':     '',
                    'timestamp':    matchObj.group(1),
                    'severity':     matchObj.group(2),
                    'module' :      matchObj.group(3),
                    'message' :     matchObj.group(4),
                    })

                logger.debug("[ES Log] Data record are written to database")

            except ElasticException as e:
                logger.error("[ES Log] An error occurred: %s:" % e.result)
                logger.error("[ES Log] DATABASE ERROR!!!!!!!!!!!!!!!!!")

        else
            logger.debug("[ES Log] Nothing to commit...")

