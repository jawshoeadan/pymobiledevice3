import asyncio
import dataclasses
import json
import logging
import multiprocessing
import os
import plistlib
import signal
import time
import traceback
import base64
from contextlib import asynccontextmanager, suppress
from typing import Optional, Union

import construct
import fastapi
from pymobiledevice3.services.heartbeat import HeartbeatService
import uvicorn
from construct import StreamError
from fastapi import FastAPI
from packaging.version import Version

from pymobiledevice3 import usbmux, pair_records, common
from pymobiledevice3.bonjour import REMOTED_SERVICE_NAMES, browse

from pymobiledevice3.lockdown import create_using_tcp, create_using_usbmux, get_mobdev2_lockdowns

from pymobiledevice3.exceptions import ConnectionFailedError, ConnectionFailedToUsbmuxdError, DeviceNotFoundError, \
    GetProhibitedError, InvalidServiceError, LockdownError, MuxException, PairingError
from pymobiledevice3.lockdown import create_using_usbmux, get_mobdev2_lockdowns
from pymobiledevice3.osu.os_utils import get_os_utils
from pymobiledevice3.remote.common import TunnelProtocol
from pymobiledevice3.remote.module_imports import start_tunnel
from pymobiledevice3.remote.remote_service_discovery import RSD_PORT, RemoteServiceDiscoveryService
from pymobiledevice3.remote.tunnel_service import CoreDeviceTunnelProxy, RemotePairingProtocol, TunnelResult, \
    create_core_device_tunnel_service_using_rsd, get_remote_pairing_tunnel_services
from pymobiledevice3.remote.utils import get_rsds, stop_remoted
from pymobiledevice3.utils import asyncio_print_traceback

logger = logging.getLogger(__name__)

# bugfix: after the device reboots, it might take some time for remoted to start answering the bonjour queries
REATTEMPT_INTERVAL = 5
REATTEMPT_COUNT = 5

REMOTEPAIRING_INTERVAL = 5
MOVDEV2_INTERVAL = 5

USBMUX_INTERVAL = 2
OSUTILS = get_os_utils()


@dataclasses.dataclass
class TunnelTask:
    task: asyncio.Task
    udid: Optional[str] = None
    tunnel: Optional[TunnelResult] = None


class TunneldCore:
    def __init__(self, protocol: TunnelProtocol = TunnelProtocol.DEFAULT, wifi_monitor: bool = True,
                 usb_monitor: bool = True, usbmux_monitor: bool = True, mobdev2_monitor: bool = True) -> None:
        self.protocol = protocol
        self.tasks: list[asyncio.Task] = []
        self.tunnel_tasks: dict[str, TunnelTask] = {}
        self.usb_monitor = usb_monitor
        self.wifi_monitor = wifi_monitor
        self.usbmux_monitor = usbmux_monitor
        self.mobdev2_monitor = mobdev2_monitor

    def start(self) -> None:
        """ Register all tasks """
        self.tasks = []
        if self.usb_monitor:
            self.tasks.append(asyncio.create_task(self.monitor_usb_task(), name='monitor-usb-task'))
        if self.wifi_monitor:
            self.tasks.append(asyncio.create_task(self.monitor_wifi_task(), name='monitor-wifi-task'))
        if self.usbmux_monitor:
            self.tasks.append(asyncio.create_task(self.monitor_usbmux_task(), name='monitor-usbmux-task'))
        if self.mobdev2_monitor:
            self.tasks.append(asyncio.create_task(self.monitor_mobdev2_task(), name='monitor-mobdev2-task'))

    def tunnel_exists_for_udid(self, udid: str) -> bool:
        for task in self.tunnel_tasks.values():
            if (task.udid == udid) and (task.tunnel is not None):
                return True
        return False

    @asyncio_print_traceback
    async def monitor_usb_task(self) -> None:
        previous_ips = []
        while True:
            current_ips = OSUTILS.get_ipv6_ips()
            added = [ip for ip in current_ips if ip not in previous_ips]
            removed = [ip for ip in previous_ips if ip not in current_ips]

            previous_ips = current_ips

            logger.debug(f'added interfaces: {added}')
            logger.debug(f'removed interfaces: {removed}')

            for ip in removed:
                if ip in self.tunnel_tasks:
                    self.tunnel_tasks[ip].task.cancel()
                    await self.tunnel_tasks[ip].task

            for ip in added:
                self.tunnel_tasks[ip] = TunnelTask(
                    task=asyncio.create_task(self.handle_new_potential_usb_cdc_ncm_interface_task(ip),
                                             name=f'handle-new-potential-usb-cdc-ncm-interface-task-{ip}'))

            # wait before re-iterating
            await asyncio.sleep(1)

    @asyncio_print_traceback
    async def monitor_wifi_task(self) -> None:
        try:
            while True:
                for service in await get_remote_pairing_tunnel_services():
                    if service.hostname in self.tunnel_tasks:
                        # skip tunnel if already exists for this ip
                        await service.close()
                        continue
                    if self.tunnel_exists_for_udid(service.remote_identifier):
                        # skip tunnel if already exists for this udid
                        await service.close()
                        continue
                    self.tunnel_tasks[service.hostname] = TunnelTask(
                        task=asyncio.create_task(self.start_tunnel_task(service.hostname, service),
                                                 name=f'start-tunnel-task-wifi-{service.hostname}'),
                        udid=service.remote_identifier
                    )
                await asyncio.sleep(REMOTEPAIRING_INTERVAL)
        except asyncio.CancelledError:
            pass

    @asyncio_print_traceback
    async def monitor_usbmux_task(self) -> None:
        try:
            while True:
                try:
                    for mux_device in usbmux.list_devices():
                        task_identifier = f'usbmux-{mux_device.serial}-{mux_device.connection_type}'
                        if self.tunnel_exists_for_udid(mux_device.serial):
                            continue
                        try:
                            service = CoreDeviceTunnelProxy(create_using_usbmux(mux_device.serial))
                        except (MuxException, InvalidServiceError, GetProhibitedError, construct.core.StreamError,
                                ConnectionAbortedError, DeviceNotFoundError, LockdownError):
                            continue
                        self.tunnel_tasks[task_identifier] = TunnelTask(
                            udid=mux_device.serial,
                            task=asyncio.create_task(
                                self.start_tunnel_task(task_identifier,
                                                       service,
                                                       protocol=TunnelProtocol.TCP),
                                name=f'start-tunnel-task-{task_identifier}'))
                except ConnectionFailedToUsbmuxdError:
                    # This is exception is expected to occur repeatedly on linux running usbmuxd
                    # as long as there isn't any physical iDevice connected
                    logger.debug('failed to connect to usbmux. waiting for it to restart')
                finally:
                    await asyncio.sleep(USBMUX_INTERVAL)
        except asyncio.CancelledError:
            pass

    @asyncio_print_traceback
    async def monitor_mobdev2_task(self) -> None:
        try:
            while True:
                async for ip, lockdown in get_mobdev2_lockdowns(only_paired=True):
                    if self.tunnel_exists_for_udid(lockdown.udid):
                        # skip tunnel if already exists for this udid
                        continue
                    task_identifier = f'mobdev2-{lockdown.udid}-{ip}'
                    try:
                        tunnel_service = CoreDeviceTunnelProxy(lockdown)
                    except InvalidServiceError:
                        logger.warning(f'[{task_identifier}] failed to start CoreDeviceTunnelProxy - skipping')
                        continue
                    self.tunnel_tasks[task_identifier] = TunnelTask(
                        task=asyncio.create_task(self.start_tunnel_task(task_identifier, tunnel_service),
                                                 name=f'start-tunnel-task-{task_identifier}'),
                        udid=lockdown.udid
                    )
                await asyncio.sleep(MOVDEV2_INTERVAL)
        except asyncio.CancelledError:
            pass

    @asyncio_print_traceback
    async def start_tunnel_task(
            self, task_identifier: str, protocol_handler: Union[RemotePairingProtocol, CoreDeviceTunnelProxy],
            queue: Optional[asyncio.Queue] = None, protocol: Optional[TunnelProtocol] = None) -> None:
        if protocol is None:
            protocol = self.protocol
        if isinstance(protocol_handler, CoreDeviceTunnelProxy):
            protocol = TunnelProtocol.TCP
        tun = None
        bailed_out = False
        try:
            if self.tunnel_exists_for_udid(protocol_handler.remote_identifier):
                # cancel current tunnel creation
                raise asyncio.CancelledError()

            async with start_tunnel(protocol_handler, protocol=protocol) as tun:
                if not self.tunnel_exists_for_udid(protocol_handler.remote_identifier):
                    self.tunnel_tasks[task_identifier].tunnel = tun
                    self.tunnel_tasks[task_identifier].udid = protocol_handler.remote_identifier
                    if queue is not None:
                        queue.put_nowait(tun)
                        # avoid sending another message if succeeded
                        queue = None
                    logger.info(f'[{asyncio.current_task().get_name()}] Created tunnel --rsd {tun.address} {tun.port}')
                    await tun.client.wait_closed()
                else:
                    bailed_out = True
                    logger.debug(
                        f'not establishing tunnel from {asyncio.current_task().get_name()} '
                        f'since there is already an active one for same udid')
        except construct.StreamError as e:
            logger.error(f"StreamError details: {str(e)}")
            logger.error(f"StreamError occurred at: {traceback.format_exc()}")
        except asyncio.CancelledError:
            pass
        except (asyncio.exceptions.IncompleteReadError, TimeoutError, OSError, ConnectionResetError, StreamError,
                InvalidServiceError) as e:
            if tun is None:
                logger.debug(f'got {e.__class__.__name__} from {asyncio.current_task().get_name()}')
            else:
                logger.debug(f'got {e.__class__.__name__} from tunnel --rsd {tun.address} {tun.port}')
        except Exception:
            logger.error(f'got exception from {asyncio.current_task().get_name()}: {traceback.format_exc()}')
        finally:
            if queue is not None:
                # notify something went wrong
                queue.put_nowait(None)

            if tun is not None and not bailed_out:
                logger.info(f'disconnected from tunnel --rsd {tun.address} {tun.port}')
                await tun.client.stop_tunnel()

            if protocol_handler is not None:
                try:
                    await protocol_handler.close()
                except OSError:
                    pass

            if task_identifier in self.tunnel_tasks:
                # in case the tunnel was removed just now
                self.tunnel_tasks.pop(task_identifier)

    @asyncio_print_traceback
    async def handle_new_potential_usb_cdc_ncm_interface_task(self, ip: str) -> None:
        rsd = None
        try:
            answers = None
            for i in range(REATTEMPT_COUNT):
                answers = await browse(REMOTED_SERVICE_NAMES, [ip])
                if answers:
                    break
                logger.debug(f'No addresses found for: {ip}')
                await asyncio.sleep(REATTEMPT_INTERVAL)

            if not answers:
                raise asyncio.CancelledError()

            peer_address = answers[0].ips[0]

            # establish an untrusted RSD handshake
            rsd = RemoteServiceDiscoveryService((peer_address, RSD_PORT))

            with stop_remoted():
                try:
                    await rsd.connect()
                except (ConnectionRefusedError, TimeoutError):
                    raise asyncio.CancelledError()

            if (self.protocol == TunnelProtocol.QUIC) and (Version(rsd.product_version) < Version('17.0.0')):
                await rsd.close()
                raise asyncio.CancelledError()

            await asyncio.create_task(
                self.start_tunnel_task(ip, await create_core_device_tunnel_service_using_rsd(rsd)),
                name=f'start-tunnel-task-usb-{ip}')
        except asyncio.CancelledError:
            pass
        except PairingError as e:
            logger.error(f'Failed to pair with {ip} with error: {e}')
        except RuntimeError:
            logger.debug(f'Got RuntimeError from: {asyncio.current_task().get_name()}')
        except Exception:
            logger.error(f'Error raised from: {asyncio.current_task().get_name()}: {traceback.format_exc()}')
        finally:
            if rsd is not None:
                try:
                    await rsd.close()
                except OSError:
                    pass

            if ip in self.tunnel_tasks:
                # in case the tunnel was removed just now
                self.tunnel_tasks.pop(ip)

    async def close(self) -> None:
        """ close all tasks """
        for task in self.tasks + [tunnel_task.task for tunnel_task in self.tunnel_tasks.values()]:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    def get_tunnels_ips(self) -> dict:
        """ Retrieve the available tunnel tasks and format them as {UDID: [IP]} """
        tunnels_ips = {}
        for ip, active_tunnel in self.tunnel_tasks.items():
            if (active_tunnel.udid is None) or (active_tunnel.tunnel is None):
                continue
            if active_tunnel.udid not in tunnels_ips:
                tunnels_ips[active_tunnel.udid] = [ip]
            else:
                tunnels_ips[active_tunnel.udid].append(ip)
        return tunnels_ips

    def cancel(self, udid: str) -> None:
        """ Cancel active tunnels """
        for tunnel_ip in self.get_tunnels_ips().get(udid, []):
            self.tunnel_tasks.pop(tunnel_ip).task.cancel()
            logger.info(f'canceling tunnel {tunnel_ip}')

    def clear(self) -> None:
        """ Clear active tunnels """
        for udid, tunnel in self.tunnel_tasks.items():
            logger.info(f'Removing tunnel {tunnel}')
            tunnel.task.cancel()
        self.tunnel_tasks = {}

def heartbeat_service_runner(udid: str, ip: Optional[str], pair_record_data: Optional[bytes]):
            try:
                logger.info(f"Starting heartbeat service runner for UDID: {udid}")
                
                
                logger.debug(f"Creating ld_service_provider for UDID: {udid}")
                ld_service_provider = create_using_tcp(identifier=udid, hostname=ip, pair_record=pair_record_data)
                
                logger.debug("Creating HeartbeatService")
                hbs = HeartbeatService(ld_service_provider)
                
                logger.info("Starting HeartbeatService")
                hbs.start()  # This is your infinite loop
            except Exception as e:
                logger.error(f"Error in heartbeat service runner: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
class TunneldRunner:
    """ TunneldRunner orchestrate between the webserver and TunneldCore """
    def run_in_subprocess(self, target_method, *args):
        process = multiprocessing.Process(target=target_method, args=args)
        process.start()
        return process
    def get_or_create_heartbeat_process(self, udid: str, ip: Optional[str], pair_record_unwrapped: Optional[dict]) -> multiprocessing.Process:
        if udid in self._heartbeat_processes:
            process = self._heartbeat_processes[udid]
            if process.is_alive():
                logger.info(f"Existing heartbeat process found for UDID: {udid}")
                return process
            else:
                logger.info(f"Existing heartbeat process for UDID: {udid} is not alive. Creating a new one.")
                del self._heartbeat_processes[udid]

        logger.info(f"Creating new heartbeat process for UDID: {udid}")
        process = self.run_in_subprocess(heartbeat_service_runner, udid, ip, pair_record_unwrapped)
        self._heartbeat_processes[udid] = process
        return process
    @classmethod
    def create(cls, host: str, port: int, protocol: TunnelProtocol = TunnelProtocol.QUIC, usb_monitor: bool = True,
               wifi_monitor: bool = True, usbmux_monitor: bool = True, mobdev2_monitor: bool = True) -> None:
        cls(host, port, protocol=protocol, usb_monitor=usb_monitor, wifi_monitor=wifi_monitor,
            usbmux_monitor=usbmux_monitor, mobdev2_monitor=mobdev2_monitor)._run_app()

    def __init__(self, host: str, port: int, protocol: TunnelProtocol = TunnelProtocol.QUIC, usb_monitor: bool = True,
                 wifi_monitor: bool = True, usbmux_monitor: bool = True, mobdev2_monitor: bool = True):
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            logging.getLogger('zeroconf').disabled = True
            self._tunneld_core.start()
            yield
            logger.info('Closing tunneld tasks...')
            await self._tunneld_core.close()

        self.host = host
        self.port = port
        self.protocol = protocol
        self._app = FastAPI(lifespan=lifespan)
        self._tunneld_core = TunneldCore(protocol=protocol, wifi_monitor=wifi_monitor, usb_monitor=usb_monitor,
                                         usbmux_monitor=usbmux_monitor, mobdev2_monitor=mobdev2_monitor)
        self._heartbeat_processes: Dict[str, multiprocessing.Process] = {}
    
        @self._app.get('/')
        async def list_tunnels() -> dict[str, list[dict]]:
            """ Retrieve the available tunnels and format them as {UUID: TUNNEL_ADDRESS} """
            tunnels = {}
            for ip, active_tunnel in self._tunneld_core.tunnel_tasks.items():
                if (active_tunnel.udid is None) or (active_tunnel.tunnel is None):
                    continue
                if active_tunnel.udid not in tunnels:
                    tunnels[active_tunnel.udid] = []
                tunnels[active_tunnel.udid].append({
                    'tunnel-address': active_tunnel.tunnel.address,
                    'tunnel-port': active_tunnel.tunnel.port,
                    'interface': ip})
            return tunnels

        @self._app.get('/shutdown')
        async def shutdown() -> fastapi.Response:
            """ Shutdown Tunneld """
            os.kill(os.getpid(), signal.SIGINT)
            data = {'operation': 'shutdown', 'data': True, 'message': 'Server shutting down...'}
            return generate_http_response(data)

        @self._app.get('/clear_tunnels')
        async def clear_tunnels() -> fastapi.Response:
            self._tunneld_core.clear()
            for heartbeat_process in self._heartbeat_processes.values():
                heartbeat_process.terminate()
            self._heartbeat_processes.clear()
            
            data = {'operation': 'clear_tunnels', 'data': True, 'message': 'Cleared tunnels...'}
            return generate_http_response(data)


        @self._app.get('/cancel')
        async def cancel_tunnel(udid: str) -> fastapi.Response:
            self._tunneld_core.cancel(udid=udid)
            data = {'operation': 'cancel', 'udid': udid, 'data': True, 'message': f'tunnel {udid} Canceled ...'}
            return generate_http_response(data)

        @self._app.get('/hello')
        async def hello() -> fastapi.Response:
            data = {'message': 'Hello, I\'m alive'}
            return generate_http_response(data)

        def generate_http_response(
                data: dict, status_code: int = 200, media_type: str = "application/json") -> fastapi.Response:
            return fastapi.Response(
                status_code=status_code,
                media_type=media_type,
                content=json.dumps(data))

        def run_in_subprocess( target_method, *args):
            process = multiprocessing.Process(target=target_method, args=args)
            process.start()
            return process
        

        @self._app.get('/start-tunnel')
        async def start_tunnel(
                udid: str, ip: Optional[str] = None, connection_type: Optional[str] = None, pair_record: Optional[str] = None) -> fastapi.Response:
            logger.info(f"Starting tunnel for UDID: {udid}, IP: {ip}, Connection Type: {connection_type}")
            udid_tunnels = [t.tunnel for t in self._tunneld_core.tunnel_tasks.values() if t.udid == udid]
            if len(udid_tunnels) > 0:

                logger.info(f"Existing tunnel found for UDID: {udid}")


                data = {
                    'interface': udid_tunnels[0].interface,
                    'port': udid_tunnels[0].port,
                    'address': udid_tunnels[0].address
                }
                return generate_http_response(data)

            queue = asyncio.Queue()
            created_task = False

            try:
                if not created_task and connection_type in ('usbmux-tcp', None):
                    task_identifier = f'usbmux-tcp-{udid}'
                    pair_record_unwrapped = None
                    try:
                        logger.debug(f"Attempting usbmux-tcp connection for UDID: {udid}")
                        if pair_record is not None:
                            logger.debug("Decoding provided pair record")
                            pair_record = base64.b64decode(pair_record)
                            pair_record_unwrapped = plistlib.loads(pair_record)
                        else:
                            logger.debug("Fetching local pairing record")
                            pair_record_unwrapped = pair_records.get_local_pairing_record(udid, pairing_records_cache_folder=common.get_home_folder())

                        logger.debug(f"Creating CoreDeviceTunnelProxy for UDID: {udid}")
                        ld_service_provider = create_using_tcp(identifier=udid, hostname=ip, pair_record=pair_record_unwrapped)
                        background_process = self.get_or_create_heartbeat_process(udid, ip, pair_record_unwrapped)
                        time.sleep(2)
                        service = CoreDeviceTunnelProxy(ld_service_provider)
                        logger.info(f"CoreDeviceTunnelProxy created: {service}")
                        task = asyncio.create_task(
                            self._tunneld_core.start_tunnel_task(task_identifier, service, protocol=TunnelProtocol.TCP,
                                                                 queue=queue),
                            name=f'start-tunnel-task-{task_identifier}')
                        self._tunneld_core.tunnel_tasks[task_identifier] = TunnelTask(task=task, udid=udid)
                        created_task = True
                        logger.info(f"usbmux-tcp task created for UDID: {udid}")
                    except (ConnectionFailedError, InvalidServiceError, MuxException, Exception) as e:
                        logger.error(f"Error in usbmux-tcp connection: {str(e)}")
                        logger.debug(f"Full traceback: {traceback.format_exc()}")
                if not created_task and connection_type in ('usbmux', None):
                    task_identifier = f'usbmux-{udid}'
                    try:
                        logger.debug(f"Attempting usbmux connection for UDID: {udid}")
                        service = CoreDeviceTunnelProxy(create_using_usbmux(udid))
                        task = asyncio.create_task(
                            self._tunneld_core.start_tunnel_task(task_identifier, service, protocol=TunnelProtocol.TCP,
                                                                 queue=queue),
                            name=f'start-tunnel-task-{task_identifier}')
                        self._tunneld_core.tunnel_tasks[task_identifier] = TunnelTask(task=task, udid=udid)
                        created_task = True
                        logger.info(f"usbmux task created for UDID: {udid}")
                    except (ConnectionFailedError, InvalidServiceError, MuxException) as e:
                        logger.error(f"Error in usbmux connection: {str(e)}")
                        logger.debug(f"Full traceback: {traceback.format_exc()}")
                if connection_type in ('usb', None):
                    logger.debug(f"Attempting USB connection for UDID: {udid}")
                    for rsd in await get_rsds(udid=udid):
                        rsd_ip = rsd.service.address[0]
                        if ip is not None and rsd_ip != ip:
                            await rsd.close()
                            logger.debug(f"Skipping RSD with IP {rsd_ip} as it doesn't match requested IP {ip}")
                            continue
                        task = asyncio.create_task(
                            self._tunneld_core.start_tunnel_task(rsd_ip,
                                                                 await create_core_device_tunnel_service_using_rsd(rsd),
                                                                 queue=queue),
                            name=f'start-tunnel-usb-{rsd_ip}')
                        self._tunneld_core.tunnel_tasks[rsd_ip] = TunnelTask(task=task, udid=rsd.udid)
                        created_task = True
                        logger.info(f"USB task created for UDID: {udid}, IP: {rsd_ip}")
                if not created_task and connection_type in ('wifi', None):
                    logger.debug(f"Attempting WiFi connection for UDID: {udid}")
                    for remotepairing in await get_remote_pairing_tunnel_services(udid=udid):
                        remotepairing_ip = remotepairing.hostname
                        if ip is not None and remotepairing_ip != ip:
                            await remotepairing.close()
                            logger.debug(f"Skipping remote pairing with IP {remotepairing_ip} as it doesn't match requested IP {ip}")
                            continue
                        task = asyncio.create_task(
                            self._tunneld_core.start_tunnel_task(remotepairing_ip, remotepairing, queue=queue),
                            name=f'start-tunnel-wifi-{remotepairing_ip}')
                        self._tunneld_core.tunnel_tasks[remotepairing_ip] = TunnelTask(
                            task=task, udid=remotepairing.remote_identifier)
                        created_task = True
                        logger.info(f"WiFi task created for UDID: {udid}, IP: {remotepairing_ip}")
            except Exception as e:
                logger.error(f"Unexpected error in start_tunnel: {str(e)}")
                logger.debug(f"Full traceback: {traceback.format_exc()}")
                return fastapi.Response(status_code=501,
                                        content=json.dumps({'error': {
                                            'exception': e.__class__.__name__,
                                            'traceback': traceback.format_exc(),
                                        }}))

            if not created_task:
                logger.warning(f"No task created for UDID: {udid}")
                return fastapi.Response(status_code=501, content=json.dumps({'error': 'task not created'}))

            logger.info(f"Waiting for tunnel creation for UDID: {udid}")
            tunnel: Optional[TunnelResult] = await queue.get()
            if tunnel is not None:
                logger.info(f"Tunnel created successfully for UDID: {udid}")
                data = {
                    'interface': tunnel.interface,
                    'port': tunnel.port,
                    'address': tunnel.address
                }
                return generate_http_response(data)

            else:
                logger.error(f"Tunnel creation failed for UDID: {udid}")
                return fastapi.Response(status_code=404,
                                        content=json.dumps({'error': 'something went wrong during tunnel creation'}))

    def _run_app(self) -> None:
        uvicorn.run(self._app, host=self.host, port=self.port, loop='asyncio')
