########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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

from itertools import chain
import os
import shutil
import tempfile
from urlparse import urlparse
from wagon import wagon

from cloudify_rest_client.plugins import Plugin
from cloudify.utils import LocalCommandRunner
from cloudify.workflows import local
from dsl_parser import constants as dsl_constants
from dsl_parser.parser import parse_from_path

from cloudify_cli import utils
from cloudify_cli import constants
from cloudify_cli.commands.plugins import validate, extract_plugin_metadata
from cloudify_cli.exceptions import CloudifyCliError
from cloudify_cli.logger import get_logger
from cloudify_cli.utils import get_rest_client


def initialize_blueprint(blueprint_path,
                         name,
                         storage,
                         install_plugins=False,
                         repository_addr=None,
                         inputs=None,
                         resolver=None):
    """Initialize local environment for the blueprint."""
    if install_plugins:
        install_blueprint_plugins(
            blueprint_path=blueprint_path,
            repository_addr=repository_addr
        )

    config = utils.CloudifyConfig()
    inputs = utils.inputs_to_dict(inputs, 'inputs')
    return local.init_env(
        blueprint_path=blueprint_path,
        name=name,
        inputs=inputs,
        storage=storage,
        ignored_modules=constants.IGNORED_LOCAL_WORKFLOW_MODULES,
        provider_context=config.local_provider_context,
        resolver=resolver,
        validate_version=config.validate_definitions_version)


def _get_plugins(blueprint_path):
    parsed_dsl = parse_from_path(dsl_file_path=blueprint_path)

    plugins = parsed_dsl[
        dsl_constants.DEPLOYMENT_PLUGINS_TO_INSTALL
    ]
    plugins.extend(
        chain.from_iterable(node['plugins'] for node in parsed_dsl['nodes']))
    for plugin in plugins:
        if plugin[dsl_constants.PLUGIN_INSTALL_KEY]:
            yield plugin


def _deduplicate_plugins(plugins):
    seen = set()
    for plugin in plugins:
        key = plugin['name']
        if key in seen:
            continue
        seen.add(key)
        yield plugin


def _is_source_plugin(plugin):
    return plugin.get(dsl_constants.PLUGIN_SOURCE_KEY)


def install_blueprint_plugins(blueprint_path, repository_addr=None):
    """Install plugins specified in the blueprint.

    This routine separates the "source plugins" (plugins that specify a source
    address) from "repository plugins" (ones that don't), and installs them
    separately. Source plugins are downloaded and batch installed using pip.
    Each repository plugin is looked up in the repository (local or
    remote, as specified with repository_addr), and installed separately,
    using a wagon distribution.
    """
    # TODO check virtualenv, pass virtualenv to wagon
    # TODO add install_arguments
    source_plugins = []
    repository_plugins = []

    for plugin in _deduplicate_plugins(_get_plugins(blueprint_path)):
        if _is_source_plugin(plugin):
            source_plugins.append(plugin)
        else:
            repository_plugins.append(plugin)

    _install_source_plugins(blueprint_path, source_plugins)
    repository = _make_plugins_repository(repository_addr)
    _install_repository_plugins(repository, repository_plugins)


def _plugins_to_requirements(blueprint_path, plugins):
    sources = set()
    for plugin in plugins:
        if plugin[dsl_constants.PLUGIN_INSTALL_KEY]:
            source = plugin[
                dsl_constants.PLUGIN_SOURCE_KEY
            ]
            if not source:
                continue
            if '://' in source:
                # URL
                sources.add(source)
            else:
                # Local plugin (should reside under the 'plugins' dir)
                plugin_path = os.path.join(
                    os.path.abspath(os.path.dirname(blueprint_path)),
                    'plugins',
                    source)
                sources.add(plugin_path)
    return sources


def _install_source_plugins(blueprint_path, plugins):
    requirements = _plugins_to_requirements(blueprint_path, plugins)
    runner = LocalCommandRunner(get_logger())
    # dump the requirements to a file
    # and let pip install it.
    # this will utilize pip's mechanism
    # of cleanup in case an installation fails.
    tmp_path = tempfile.mkstemp(suffix='.txt', prefix='requirements_')[1]
    utils.dump_to_file(collection=requirements, file_path=tmp_path)
    runner.run(command='pip install -r {0}'.format(tmp_path),
               stdout_pipe=False)


def _install_repository_plugin(repository, repository_plugin):
    logger = get_logger()
    plugin_id = repository_plugin.id
    wagon_dir = tempfile.mkdtemp(prefix='{0}-'.format(plugin_id))
    wagon_path = os.path.join(wagon_dir, 'wagon.tar.gz')
    try:
        logger.debug('Downloading plugin {0} from manager into {1}'
                     .format(plugin_id, wagon_path))
        repository.download(plugin_id=plugin_id,
                            output_file=wagon_path)
        logger.debug('Installing plugin {0} using wagon'
                     .format(plugin_id))
        w = wagon.Wagon(source=wagon_path)
        args = ''
        w.install(ignore_platform=True,
                  install_args=args)
    finally:
        logger.debug('Removing directory: {0}'
                     .format(wagon_dir))
        shutil.rmtree(wagon_dir)


def _install_repository_plugins(repository, plugins):
    for plugin in plugins:
        repository_plugin = repository.find_plugin(plugin)
        _install_repository_plugin(repository, repository_plugin)


class ManagerPluginRepository(object):
    def __init__(self, rest_client):
        self._rest_client = rest_client

    def download(self, plugin_id, output_file):
        return self._rest_client.plugins.download(plugin_id, output_file)

    def _list(self):
        return self._rest_client.plugins.list()

    def find_plugin(self, plugin):
        repository_plugins = self._list()
        for repository_plugin in repository_plugins:
            if (plugin['package_name'] == repository_plugin['package_name'] and
                plugin['package_version'] ==
                    repository_plugin['package_version']):
                return repository_plugin


class LocalFilePluginRepository(object):
    def __init__(self, directory):
        self._directory = directory
        self._metadata_cache = None

    def _build_metadata_cache(self):
        cache = {}
        for filename in os.listdir(self._directory):
            path = os.path.join(self._directory, filename)
            try:
                validate(path)
            except CloudifyCliError:
                continue

            metadata = extract_plugin_metadata(path)
            cache[filename] = metadata
        return cache

    @property
    def metadata(self):
        if self._metadata_cache is None:
            self._metadata_cache = self._build_metadata_cache()
        return self._metadata_cache

    def download(self, plugin_id, output_file):
        plugin_filename = os.path.join(self._directory, plugin_id)
        shutil.copy(plugin_filename, output_file)

    def _match(self, plugin_metadata, search_params):
        for key_to_check in ['package_name', 'package_version']:
            if key_to_check not in search_params:
                continue
            if plugin_metadata[key_to_check] != search_params[key_to_check]:
                return False
        return True

    def find_plugin(self, plugin):
        for plugin_filename, plugin_metadata in self.metadata.items():
            if self._match(plugin_metadata, plugin):
                return Plugin({'id': plugin_filename})
        else:
            raise ValueError('No wagon found for plugin '
                             '{plugin[package_name]}-{plugin[package_version]}'
                             ' in directory {directory}'.format(
                                 plugin=plugin,
                                 directory=os.path.abspath(self._directory)))


def _make_plugins_repository(repository_addr=None):
    if repository_addr is None:
        return LocalFilePluginRepository('plugins')

    parsed_url = urlparse(repository_addr)

    if parsed_url.scheme in {'http', 'https'}:
        host, _, port = parsed_url.netloc.partition(':')
        rest_client = get_rest_client(manager_ip=host, rest_port=port,
                                      protocol=parsed_url.scheme)
        return ManagerPluginRepository(rest_client)
    elif parsed_url.scheme:
        raise ValueError('Unsupported plugin repository scheme: {0}'
                         .format(parsed_url.scheme))
    else:
        return LocalFilePluginRepository(repository_addr)
