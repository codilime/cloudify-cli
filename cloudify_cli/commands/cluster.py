########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
############

import time

from .. import env
from .. import table
from ..cli import cfy
from ..exceptions import CloudifyCliError
from ..execution_events_fetcher import WAIT_FOR_EXECUTION_SLEEP_INTERVAL


def _verify_not_in_cluster(client):
    status = client.cluster.get()
    if status.initialized:
        raise CloudifyCliError('This manager machine is already part '
                               'of a HA cluster')


@cfy.group(name='ha')
@cfy.options.verbose()
def cluster():
    if not env.is_initialized():
        env.raise_uninitialized()


@cluster.command(name='status',
                 short_help='Show the current cluster status')
@cfy.pass_client()
@cfy.pass_logger
def status(client, logger):
    status = client.cluster.status()
    if not status.initialized:
        logger.error('This manager is not part of a Cloudify HA cluster')
    else:
        logger.info('The cluster is initialized.\nEncryption key: {0}'
                    .format(status.encryption_key))


@cluster.command(name='start',
                 short_help='Start a HA manager cluster.')
@cfy.pass_client()
@cfy.pass_logger
@cfy.options.timeout()
@cfy.options.cluster_host_ip
@cfy.options.cluster_node_name
@cfy.options.cluster_virtual_ip
@cfy.options.cluster_consul_key(with_default=True)
@cfy.options.cluster_network_interface
def start(client,
          logger,
          timeout,
          cluster_host_ip,
          cluster_node_name,
          cluster_virtual_ip,
          cluster_consul_key,
          cluster_network_interface):
    _verify_not_in_cluster(client)

    logger.info('Creating a new Cloudify HA cluster')

    client.cluster.start(config={
        'host_ip': cluster_host_ip,
        'node_name': cluster_node_name,
        'consul_key': cluster_consul_key,
        'network_interface': cluster_network_interface,
        'virtual_ip': cluster_virtual_ip
    })
    status = _wait_for_cluster_initialized(client)

    if status.error:
        logger.error('Error while configuring the HA cluster')
        raise CloudifyCliError(status.error)

    logger.info('HA cluster started at {0}.\n'
                'Encryption key used is: {1}'
                .format(cluster_host_ip, cluster_consul_key))


@cluster.command(name='join',
                 short_help='Join a HA manager cluster.')
@cfy.pass_client()
@cfy.pass_logger
@cfy.options.timeout()
@cfy.options.cluster_host_ip
@cfy.options.cluster_node_name
@cfy.options.cluster_join
@cfy.options.cluster_consul_key(with_default=False)
@cfy.options.cluster_network_interface
def join(client,
         logger,
         timeout,
         cluster_host_ip,
         cluster_node_name,
         cluster_consul_key,
         cluster_join,
         cluster_network_interface):
    _verify_not_in_cluster(client)

    logger.info('Joining the Cloudify HA cluster: {0}'.format(cluster_join))

    client.cluster.join(node_id=cluster_node_name, config={
        'host_ip': cluster_host_ip,
        'node_name': cluster_node_name,
        'consul_key': cluster_consul_key,
        'network_interface': cluster_network_interface,
        'join_addrs': cluster_join
    })
    status = _wait_for_cluster_initialized(client)

    if status.error:
        logger.error('Error while joining the HA cluster')
        raise CloudifyCliError(status.error)

    # TODO: do something with the current profile?
    logger.info('HA cluster joined successfully!')


@cluster.command(name='update',
                 short_help='Join a HA manager cluster.')
@cfy.pass_client()
@cfy.pass_logger
def update(client, logger):
    """
    Update the cluster configuration
    """
    raise NotImplementedError('Not implemented yet')


@cluster.group(name='nodes')
def nodes():
    pass


@nodes.command(name='get',
               short_help='Retrieve the cluster node status and configuration')
@cfy.pass_client()
@cfy.pass_logger
def node_details(client, logger):
    raise NotImplementedError('Not implemented yet')
    response = client.cluster.nodes.get()
    logger.log('Node details: {0}'.format(response))


@nodes.command(name='list',
               short_help='List the nodes in the cluster')
@cfy.pass_client()
@cfy.pass_logger
def list_nodes(client, logger):
    response = client.cluster.nodes.list()
    nodes_table = table.generate(['host_ip', 'master'], response,
                                 {'master': False})
    table.log('HA Cluster nodes', nodes_table)


@nodes.command(name='delete',
               short_help='Delete a node from the cluster')
@cfy.pass_client()
@cfy.pass_logger
@cfy.options.cluster_node_name
def delete_node(client, logger, node_name):
    raise NotImplementedError('Not implemented yet')
    client.cluster.nodes.delete(node_name)
    logger.info('Node {0} was deleted from the cluster'.format(node_name))


def _wait_for_cluster_initialized(client, timeout=900):
    if timeout is not None:
        deadline = time.time() + timeout

    while True:
        if timeout is not None:
            if time.time() > deadline:
                raise CloudifyCliError('Timed out waiting for the HA '
                                       'cluster to be initialized.')

        status = client.cluster.get()
        if status.initialized or status.error:
            break
        time.sleep(WAIT_FOR_EXECUTION_SLEEP_INTERVAL)

    return status
