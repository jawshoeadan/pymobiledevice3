#!/usr/bin/env python3
# nythepegasus 2024
import asyncio
from functools import partial
from subprocess import Popen, PIPE
    
import click
import inquirer3
from flask import Flask, request

import pymobiledevice3
from pymobiledevice3.bonjour import DEFAULT_BONJOUR_TIMEOUT
from pymobiledevice3.cli.cli_common import BaseCommand, RSDCommand, print_json, prompt_device_list, sudo_required, \
    user_requested_colored_output
from pymobiledevice3.common import get_home_folder
from pymobiledevice3.exceptions import NoDeviceConnectedError
from pymobiledevice3.pair_records import PAIRING_RECORD_EXT, get_remote_pairing_record_filename
from pymobiledevice3.remote.common import ConnectionType, TunnelProtocol
from pymobiledevice3.remote.module_imports import MAX_IDLE_TIMEOUT, start_tunnel, verify_tunnel_imports
from pymobiledevice3.remote.remote_service_discovery import RSD_PORT, RemoteServiceDiscoveryService
from pymobiledevice3.remote.tunnel_service import get_core_device_tunnel_services, get_remote_pairing_tunnel_services
from pymobiledevice3.remote.utils import get_rsds, install_driver_if_required
from pymobiledevice3.tunneld import TUNNELD_DEFAULT_ADDRESS, TunneldRunner
from pymobiledevice3.tunneld import get_tunneld_devices
from pymobiledevice3.services.installation_proxy import InstallationProxyService
from pymobiledevice3.services.dvt.dvt_secure_socket_proxy import DvtSecureSocketProxyService
from pymobiledevice3.services.dvt.instruments.process_control import ProcessControl


def prompt_list(var: str, message: str, items: list):
    app_question = [inquirer3.List(var, message=message, choices=items, carousel=True)]
    try:
        result = inquirer3.prompt(app_question, raise_keyboard_interrupt=True)
        return result[var]
    except KeyboardInterrupt:
        raise NoDeviceSelectedError()

def enable_jit(device, app: str):
    debugserver_host, debugserver_port = device.service.address[0], device.get_service_port(service_name)

    with DvtSecureSocketProxyService(lockdown=device) as dvt:
        process_control = ProcessControl(dvt)
        pid = process_control.launch(bundle_id=app, arguments={},
                                     kill_existing=False, start_suspended=False,
                                     environment={})

    #click.echo(f"enable_jit [{debugserver_host}]:{debugserver_port} {pid}")
    p = Popen(['lldb'], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    output, error = p.communicate(input=f'settings set interpreter.require-overwrite false\ncommand script import ./enable_jit.py\nenable_jit [{debugserver_host}]:{debugserver_port} {pid}\n'.encode())
    p.kill()
    return True

service_name = 'com.apple.internal.dt.remote.debugproxy'

@click.command()
@click.option('--server', '-s', is_flag=True, help="Starts a web server that enables JIT on demand in the background for you :)")
@click.option('--bundle_id', default=None, help='Specify a Bundle ID to enable JIT for')
@click.option('--app', default=None, help='Specify part of/an app name to enable JIT for')
def main(server, bundle_id, app):
    """
    A basic script that accepts a bundle ID
    """
    #click.echo(f"Bundle ID: {bundle_id}")
    devices = get_tunneld_devices()
    if len(devices) > 1:
        device = prompt_list('device', 'Select device:', devices)
    else:
        device = devices[0]

    if bundle_id is None:
        apps = InstallationProxyService(lockdown=device).get_apps()
        bundles = {apps[app]['CFBundleDisplayName']: app for app in apps if 'Entitlements' in apps[app] and 'get-task-allow' in apps[app]['Entitlements'] and apps[app]['Entitlements']['get-task-allow']}

        if app is not None:
            apps = [a for a in list(bundles.keys()) if app in a or app == a]
            if len(apps) >= 1:
                app = apps[0]
            else:
                raise Exception(f"{app} doesn't appear to be installed!")
        else:
            if server:
                fap = Flask(__name__)

                @fap.route('/', methods=['GET', 'POST'])
                def hello():
                    if request.method == 'POST':
                        if 'app' in request.form:
                            app = request.form['app']
                            apps = [a for a in list(bundles.keys()) if app in a or app == a]
                            if len(apps) >= 1:
                                enable_jit(device, bundles[apps[0]])
                                return f"Enabled JIT for {app!r}!"
                            else:
                                return f"No such app {app!r}!"
                        elif 'bundle' in request.form:
                            bundle = request.form['bundle']
                            apps = [a for a in list(bundles.values()) if a == bundle or bundle in a]
                            if len(apps) >= 1:
                                enable_jit(device, apps[0])
                                return f"Enabled JIT for {bundles[apps[0]]!r}!"
                            else:
                                return f"No such bundle {bundle!r}!"
                        return request.form
                    else:
                        return bundles

                if bundle_id is None and app is None:
                    fap.run(host='0.0.0.0', port=8080)
            else:
                app = prompt_list('app', 'Select application:', list(bundles.keys()))
    else:
        app = bundle_id

    enable_jit(device, bundles[app])
    click.echo(f"Enabled JIT for {app!r}!")

    if server:
        fap.run(port=8080)


if __name__ == '__main__':
    main()
