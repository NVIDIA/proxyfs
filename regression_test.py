#!/usr/bin/env python

"""
A script to lint and test ProxyFS project code.
"""

from __future__ import print_function, unicode_literals
from threading import Timer

import os
import argparse
import functools
import logging
import platform
import contextlib
import subprocess
import shutil
import sys
import tempfile
import time


PACKAGES = ["blunder",
            "cleanproxyfs",
            "conf",
            "dlm",
            "fs",
            "fsworkout",
            "fuse",
            "headhunter",
            "httpserver",
            "inode",
            "inodeworkout",
            "jrpcfs",
            "logger",
            "mkproxyfs", "mkproxyfs/mkproxyfs",
            "pfs-stress",
            "pfsconfjson", "pfsconfjsonpacked",
            "pfsworkout",
            "platform",
            "proxyfsd", "proxyfsd/proxyfsd",
            "ramswift", "ramswift/ramswift",
            "stats",
            "statslogger",
            "swiftclient",
            "utils"]


COLORS = {"bright red": '1;31', "bright green": '1;32'}


@contextlib.contextmanager
def return_to_wd():
    curdir = os.getcwd()
    try:
        yield
    finally:
        os.chdir(curdir)


@contextlib.contextmanager
def self_cleaning_tempdir(*args, **kwargs):
    our_tempdir = tempfile.mkdtemp(*args, **kwargs)
    try:
        yield our_tempdir
    finally:
        shutil.rmtree(our_tempdir, ignore_errors=True)


def proxyfs_binary_path(binary):
    try:
        gopath = os.environ["GOPATH"]
    except KeyError:
        color_print("$GOPATH must be set", 'bright red')
        os.exit(1)
    return os.path.join(gopath, "bin", binary)


def color_print(content, color=None):
    print("\x1b[{color}m{content}\x1b[0m".format(content=content,
                                                 color=COLORS[color]))


def proxyfs_package_path(package):
    try:
        gopath = os.environ["GOPATH"]
    except KeyError:
        color_print("$GOPATH must be set", 'bright red')
        os.exit(1)
    return os.path.join(gopath, "src/github.com/swiftstack/ProxyFS", package)


def color_print(content, color=None):
    print("\x1b[{color}m{content}\x1b[0m".format(content=content,
                                                 color=COLORS[color]))


def report(task, success=False):
    printer = color_print if sys.stdout.isatty() else lambda *a, **kw: print(*a)
    if success:
        printer("{} {}".format(task, "succeeded!"), color="bright green")
    else:
        printer("{} {}".format(task, "failed!"), color="bright red")


class GoCommand(object):
    def __init__(self, command, project_path,
                 options=None, report_as=None, skip=False):
        self.command = command
        self.project_path = project_path
        self.options = options or []
        self.report_as = report_as
        self.skip = skip

    def execute(self, package):
        if self.skip:
            return None
        package_path = "{}{}".format(self.project_path, package)
        command_line = ["go", self.command] + self.options + [package_path]
        logging.info(' '.join("'{}'".format(s) if ' ' in s else s
                              for s in command_line))
        success = not bool(subprocess.call(command_line))
        return success


def get_go_commands(options, project_path, skip_tests=False):
    commands = [
        GoCommand('fmt', project_path),
        GoCommand('get', project_path, ['-t', '-u'], skip=not options.get),
        # Source code has to be `go install`ed before `stringer` can run on
        # it, so we install before generate, which mysteriously does work?
        # see https://github.com/golang/go/issues/10249
        GoCommand('install', project_path, ['-gcflags', '-N -l'], report_as="`go install`"),
        GoCommand('generate', project_path, report_as="`go generate`"),
    ]
    if not options.deb_builder:
        commands.append(
            GoCommand('test', project_path,
                      filter(lambda x: x, [options.bench, options.cover]),
                      report_as="`go test`",
                      skip=skip_tests)
        )
    commands.append(GoCommand('vet', project_path, report_as="`go vet`"))
    return commands


def execute_go_commands(commands, packages):
    reports = []
    failures = 0
    for command in commands:
        successes = filter(lambda success: success is not None,
                           [command.execute(package) for package in packages])
        success = all(successes)
        failures += not success
        if command.report_as is not None:
            reports.append(functools.partial(report, command.report_as,
                                             success=success))
    for reporter in reports:
        reporter()
    return failures


def test_pfs_middleware(options):
    failures = 0
    with return_to_wd():
        proxyfs_dir = os.path.dirname(os.path.abspath(__file__))
        pfs_middleware_dir = os.path.join(proxyfs_dir, "pfs_middleware")
        os.chdir(pfs_middleware_dir)
        failures += bool(subprocess.call((['python', 'setup.py', 'test'])))
    report("test_pfs_middleware()", not failures)
    return failures


def build_proxyfs(options):
    commands = get_go_commands(options, "github.com/swiftstack/ProxyFS/")
    if options.packages is None:
        selected_packages = PACKAGES
    else:
        selected_packages = options.packages
    return execute_go_commands(commands, selected_packages)


def build_dependencies(options):
    failures = 0
    proxyfs_dir = os.path.dirname(os.path.abspath(__file__))
    stringer_path = 'golang.org/x/tools/cmd/stringer'
    full_stringer_path = os.path.join(proxyfs_dir, "vendor", stringer_path)
    print("Building " + stringer_path)
    install_cmd = ("go", "install")
    with return_to_wd():
        os.chdir(full_stringer_path)
        success = not(bool(subprocess.call(install_cmd)))
        failures += not success
    report("build_dependencies()", not failures)
    return failures


def build_jrpcclient(options):
    proxyfs_dir = os.path.dirname(os.path.abspath(__file__))
    jrpcclient_dir = os.path.join(proxyfs_dir, "jrpcclient")
    command = ['./regression_test.py'] + sys.argv[1:]
    return bool(subprocess.call(command, cwd=jrpcclient_dir))


def build_vfs(options):
    proxyfs_dir = os.path.dirname(os.path.abspath(__file__))
    vfs_dir = os.path.join(proxyfs_dir, "vfs")
    failures = 0
    failures += bool(subprocess.call('make', cwd=vfs_dir))
    if failures:
        return failures
    distro = platform.linux_distribution()[0]
    if 'centos' in distro.lower():
        make_option = 'installcentos'
    else:
        make_option = 'install'
    failures += bool(subprocess.call(('make', make_option), cwd=vfs_dir))
    return failures


def main(options):
    failures = ""
    go_version = subprocess.check_output((['go', 'version']))
    color_print(go_version[:-1], "bright green")

    if not options.quiet:
        logging.basicConfig(format="%(message)s", level=logging.INFO)

    failures = build_dependencies(options)
    if failures:
        return failures

    failures += build_proxyfs(options)
    if failures:
        return failures

    failures += build_jrpcclient(options)
    if failures:
        return failures

    failures += build_vfs(options)
    if failures:
        return failures

    return failures


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument('--bench', '-bench',
                            action='store_const', const='-bench=.',
                            help="include benchmark measurements in test output")
    arg_parser.add_argument('--cover', '-cover',
                            action='store_const', const='-cover',
                            help="include coverage statistics in test output")
    arg_parser.add_argument('--get', '-get', action='store_true',
                            help="invoke `go get` to retrieve new dependencies")
    arg_parser.add_argument('--packages', '-p', action='store', nargs='*',
                            help="specific packages to process")
    arg_parser.add_argument('--deb-builder', action='store_true',
                            help="Modify commands to run inside "
                                 "swift-deb-builder")
    arg_parser.add_argument('--quiet', '-q', action='store_true',
                            help="suppress printing of what commands are being run")
    options = arg_parser.parse_args()

    exit(main(options))
