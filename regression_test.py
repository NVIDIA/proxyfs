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
            "swiftclient",
            "utils"]

LIBS = ["jrpcclient", "vfs"]

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


def build_libs(options):
    failures = 0
    proxyfs_dir = os.path.dirname(os.path.abspath(__file__))
    for lib in LIBS:
        print("Building " + lib)
        full_lib_path = os.path.join(proxyfs_dir, lib)
        with return_to_wd():
            os.chdir(full_lib_path)
            make_success = not(bool(subprocess.call((['make', 'clean']))))
            failures += not make_success
            make_success = not(bool(subprocess.call((['make', 'all']))))
            failures += not make_success
            if not options.no_install:
                if 'Ubuntu' == platform.linux_distribution()[0]:
                    install_cmd = ['make', 'install']
                    if not options.deb_builder:
                        install_cmd.insert(0, 'sudo')
                    make_success = not(bool(subprocess.call(install_cmd)))
                    failures += not make_success
                if 'CentOS Linux' == platform.linux_distribution()[0]:
                    install_cmd = ['make', 'installcentos']
                    if not options.deb_builder:
                        install_cmd.insert(0, 'sudo')
                    make_success = not(bool(subprocess.call(install_cmd)))
                    failures += not make_success
    report("build_libs()", not failures)
    return failures


def wait_for_proxyfsd(address, port, interval=0.5, max_iterations=60):
    # We're importing requests here to allow build process to work without
    # requests.
    import requests

    current_iteration = 0
    is_proxyfs_up = False
    while not is_proxyfs_up and current_iteration < max_iterations:
        time.sleep(interval)
        try:
            r = requests.get('http://{}:{}'.format(address, port), timeout=3)
            if r.status_code == 200:
                is_proxyfs_up = True
        except Exception:
            pass
        current_iteration += 1
    return is_proxyfs_up


def test_jrpcclient():
    private_ip_addr  = "127.0.0.1"
    ramswift_port    =  4592 # arbitrary
    jsonrpc_port     = 12347 # 12347 instead of 12345 so that test can run if proxyfsd is already running
    jsonrpc_fastport = 32347 # 32347 instead of 32345 so that test can run if proxyfsd is already running
    http_port        = 15347 # 15347 instead of 15346 so that test can run if proxyfsd is already running

    color_printer = color_print if sys.stdout.isatty() else lambda *a, **kw: print(*a)

    with self_cleaning_tempdir() as our_tempdir, open(os.devnull) as dev_null:
        ramswift = subprocess.Popen(
            [proxyfs_binary_path("ramswift"),
             "saioramswift0.conf",
             "Peer0.PrivateIPAddr={}".format(private_ip_addr),
             "SwiftClient.NoAuthTCPPort={}".format(ramswift_port)],
             stdout=dev_null, stderr=dev_null,
             cwd=proxyfs_package_path("ramswift")
        )

        proxyfsd = subprocess.Popen(
            [proxyfs_binary_path("proxyfsd"),
             "saioproxyfsd0.conf",
             "Logging.LogFilePath={}/{}".format(our_tempdir, "proxyfsd_jrpcclient.log"),
             "Peer0.PrivateIPAddr={}".format(private_ip_addr),
             "SwiftClient.NoAuthTCPPort={}".format(ramswift_port),
             "JSONRPCServer.TCPPort={}".format(jsonrpc_port),
             "JSONRPCServer.FastTCPPort={}".format(jsonrpc_fastport),
             "JSONRPCServer.DontWriteConf=true",
             "HTTPServer.TCPPort={}".format(http_port)],
            stdout=dev_null, stderr=dev_null,
            cwd=proxyfs_package_path("proxyfsd")
        )

        # Make sure proxyfsd hasn't exited before we start the tests
        proxyfsd.poll()
        if proxyfsd.returncode:
            color_printer("Before starting test, nonzero exit status returned from proxyfsd daemon: {}".format(proxyfsd.returncode), color="bright red")
            report("jrpcclient tests", not proxyfsd.returncode)

            # Print out proxyfsd's stdout since it exited unexpectedly
            proxyfsd_logfile = "{}/{}".format(our_tempdir, "proxyfsd_jrpcclient.log")
            logfile = open(proxyfsd_logfile, 'r')
            print(logfile.read())
            logfile.close()

            # Clean up
            ramswift.terminate()
            return proxyfsd.returncode

        config_override_string = "{}:{}/{}".format(private_ip_addr,
                                                   jsonrpc_port,
                                                   jsonrpc_fastport)

        # wait a moment for proxyfsd to get set "Up()"
        # wait_for_proxyfs(...) returns a boolean, but we'll let the rest of
        # this script manage everything, just as it has been done until now and
        # specifically manage the case where ProxyFS isn't up.
        wait_for_proxyfsd(private_ip_addr, http_port)

        jrpcclient_tests = subprocess.Popen(
            [os.path.join(".", "test"),
             "-o", config_override_string],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            cwd=proxyfs_package_path("jrpcclient")
        )

        # Put a time limit on the tests, in case they hang
        def kill_proc(p):
            color_printer("jrpcclient tests timed out!", color="bright red")
            p.kill()

        timeout_sec = 200
        timer = Timer(timeout_sec, kill_proc, [jrpcclient_tests])

        try:
            timer.start()

            if not options.verbose_jrpcclient:
                # This line gets all jrpcclient stdout at once, waits till it's over
                jrpcclient_test_stdout, _ = jrpcclient_tests.communicate()

                # Emit test stdout only if there was a failure
                if jrpcclient_tests.returncode:
                    print(jrpcclient_test_stdout)

            else:
                # I'm not confident in this code yet; deadlock may be possible.

                # Get all jrpcclient stdout line by line.
                # Doesn't continue until the test is done.
                # (if thread is still running, it won't return)
                while True:
                    line = jrpcclient_tests.stdout.readline()
                    print(line, end="")
                    if (line == '' and jrpcclient_tests.poll() != None):
                        break
        finally:
            timer.cancel()

        proxyfsd.terminate()
        time.sleep(0.5)  # wait a moment for proxyfsd to get set "Down()"
        ramswift.terminate()

    report("jrpcclient tests", not jrpcclient_tests.returncode)

    return jrpcclient_tests.returncode


def main(options):
    failures = ""
    go_version = subprocess.check_output((['go', 'version']))
    color_print(go_version[:-1], "bright green")

    if not options.quiet:
        logging.basicConfig(format="%(message)s", level=logging.INFO)

    if not options.just_libs and not options.just_build_libs:
        failures = build_dependencies(options)
        if failures:
            return failures

        failures += build_proxyfs(options)
        if failures:
            return failures

    if platform.system() != "Darwin":
        if not options.no_libs:
            if options.just_libs or options.just_build_libs:
                failures = build_libs(options)
            else:
                failures += build_libs(options)
            if not options.deb_builder and not options.just_build_libs:
                failures += test_jrpcclient()

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
    libs_group = arg_parser.add_mutually_exclusive_group()
    libs_group.add_argument('--no-libs', action='store_true',
                            help="don't build C libraries or run C tests")
    libs_group.add_argument('--just-build-libs', action='store_true',
                            help="only build C libraries")
    libs_group.add_argument('--just-libs', action='store_true',
                            help="only build C libraries and run C tests")
    arg_parser.add_argument('--verbose-jrpcclient', action='store_true',
                            help="EXPERIMENTAL, DO NOT USE! "
                            "emit jrpcclient test stdout even if no failures")
    arg_parser.add_argument('--no-install', action='store_true',
                            help="When building C libraries, do not attempt "
                                 "to install resulting objects")
    arg_parser.add_argument('--deb-builder', action='store_true',
                            help="Modify commands to run inside "
                                 "swift-deb-builder")
    arg_parser.add_argument('--quiet', '-q', action='store_true',
                            help="suppress printing of what commands are being run")
    options = arg_parser.parse_args()

    exit(main(options))
