import os
import random
import subprocess

IS_BUILDKITE = os.getenv("BUILDKITE") is not None


def image_pull_policy():
    # This is because when running local tests, we need to load the image into the kind cluster (and
    # then not attempt to pull it) because we don't want to require credentials for a private
    # registry / pollute the private registry / set up and network a local registry as a condition
    # of running tests
    if IS_BUILDKITE:
        return "Always"
    else:
        return "IfNotPresent"


def check_output(*args, **kwargs):
    try:
        return subprocess.check_output(*args, **kwargs)
    except subprocess.CalledProcessError as exc:
        output = exc.output.decode("utf-8")
        raise Exception(output) from exc


def which_(exe):
    """Uses distutils to look for an executable, mimicking unix which"""
    from distutils import spawn  # pylint: disable=no-name-in-module

    # https://github.com/PyCQA/pylint/issues/73
    return spawn.find_executable(exe)


def get_test_namespace():
    namespace_suffix = hex(random.randint(0, 16 ** 6))[2:]
    return "dagster-test-%s" % namespace_suffix


def within_docker():
    """detect if we're running inside of a docker container

    from: https://stackoverflow.com/a/48710609/11295366
    """
    cgroup_path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv")
        or os.path.isfile(cgroup_path)
        and any("docker" in line for line in open(cgroup_path))
    )
