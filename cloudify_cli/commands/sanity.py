from cloudify_cli.bootstrap.bootstrap import load_env


def sanity(*a, **kw):
    """Run sanity checks."""
    env = load_env()
    env.execute(workflow='sanitycheck')
