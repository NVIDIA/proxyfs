#!/usr/bin/env python

"""
A script to lint and test sortedmap package.
"""

from __future__ import print_function, unicode_literals

import os
import argparse
import functools
import logging
import platform
import contextlib
import subprocess
import sys


PACKAGES = ["sortedmap"]

COLORS = {"bright red": '1;31', "bright green": '1;32'}

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
        logging.info(' '.join(command_line))
        success = not bool(subprocess.call(command_line))
        return success


def get_go_commands(options, project_path, skip_tests=False):
    commands = [
        GoCommand('fmt', project_path),
        GoCommand('get', project_path, ['-t', '-u'], skip=not options.get),
        # Source code has to be `go install`ed before `stringer` can run on
        # it, so we install before generate, which mysteriously does work?
        # see https://github.com/golang/go/issues/10249
        GoCommand('install', project_path),
        GoCommand('generate', project_path, report_as="`go generate`"),
        GoCommand('test', project_path,
                  filter(lambda x: x, [options.bench, options.cover]),
                  report_as="`go test`",
                  skip=skip_tests),
        GoCommand('vet', project_path, report_as="`go vet`")
    ]
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


def build_it(options):
    commands = get_go_commands(options, "github.com/swiftstack/")
    selected_packages = PACKAGES
    return execute_go_commands(commands, selected_packages)


def main(options):
    if not options.quiet:
        logging.basicConfig(format="%(message)s", level=logging.INFO)

    failures = build_it(options)
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
    arg_parser.add_argument('--quiet', '-q', action='store_true',
                            help="suppress printing of what commands are being run")
    options = arg_parser.parse_args()

    exit(main(options))
