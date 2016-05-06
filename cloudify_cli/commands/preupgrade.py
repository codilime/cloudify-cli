from cloudify_cli.bootstrap.bootstrap import load_env
from cloudify.workflows import local

def preupgrade(blueprint_path, inputs=None):
    """Run sanity checks."""
    env = load_env()
    nodes = env.storage.get_nodes()
    storage = local.FileStorage()
    for node in nodes:
        if node['type'] == 'manager.nodes.RabbitMQ':
            print node['properties']
