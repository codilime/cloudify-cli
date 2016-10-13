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

from .. import env
from .. import table
from ..cli import cfy
from ..logger import get_events_logger
from ..exceptions import CloudifyCliError, ExecutionTimeoutError, \
    SuppressedCloudifyCliError
from ..execution_events_fetcher import wait_for_execution


def _verify_not_in_cluster(client):
    status = client.cluster.status()
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
        logger.info('Cluster virtual IP: {0}\nEncryption key: {1}'
                    .format(status.virtual_ip, status.encryption_key))


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

    execution = client.cluster.start(config={
        'host_ip': cluster_host_ip,
        'node_name': cluster_node_name,
        'consul_key': cluster_consul_key,
        'network_interface': cluster_network_interface,
        'virtual_ip': cluster_virtual_ip
    })
    events_logger = get_events_logger(json_output=False)
    try:
        wait_for_execution(
            client,
            execution,
            events_handler=events_logger)
    except ExecutionTimeoutError as e:
        logger.info('HA cluster creation execution timed out')
        events_tail_message = "* Run 'cfy events list --tail --include-logs " \
                              "--execution-id {0}' to retrieve the " \
                              "execution's events/logs"
        logger.info(events_tail_message.format(e.execution_id))
        raise SuppressedCloudifyCliError()

    # TODO change the current profile to use the virtual ip
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

    execution = client.cluster.join(node_id=cluster_node_name, config={
        'host_ip': cluster_host_ip,
        'node_name': cluster_node_name,
        'consul_key': cluster_consul_key,
        'network_interface': cluster_network_interface,
        'join_addrs': cluster_join
    })

    events_logger = get_events_logger(json_output=False)
    try:
        wait_for_execution(
            client,
            execution,
            events_handler=events_logger)
    except ExecutionTimeoutError as e:
        logger.info('HA cluster join execution timed out')
        events_tail_message = "* Run 'cfy events list --tail --include-logs " \
                              "--execution-id {0}' to retrieve the " \
                              "execution's events/logs"
        logger.info(events_tail_message.format(e.execution_id))
        raise SuppressedCloudifyCliError()

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
