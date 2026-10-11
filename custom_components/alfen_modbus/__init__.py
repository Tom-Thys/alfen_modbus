"""The Alfen Modbus Integration."""
import asyncio
import logging
import operator
from datetime import datetime, timedelta

import homeassistant.helpers.config_validation as cv
import voluptuous as vol
from dateutil.tz import tzoffset
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_NAME, CONF_PORT, CONF_SCAN_INTERVAL
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.event import async_track_time_interval
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException

from .const import (
    CONF_MODBUS_ADDRESS,
    CONF_POLL_CONNECTED_INTERVAL,
    CONF_POLL_IDLE_INTERVAL,
    CONF_POLL_STATISTICS_INTERVAL,
    CONF_READ_SCN,
    CONF_READ_SOCKET2,
    DEFAULT_MODBUS_ADDRESS,
    DEFAULT_NAME,
    DEFAULT_POLL_CONNECTED_INTERVAL,
    DEFAULT_POLL_IDLE_INTERVAL,
    DEFAULT_POLL_STATISTICS_INTERVAL,
    DEFAULT_READ_SCN,
    DEFAULT_READ_SOCKET2,
    DEFAULT_SCAN_INTERVAL,
    DOMAIN,
    MAX_CURRENT_S,
    PHASE_SWITCH_OPTIONS,
    SCN_MAX_CURRENT_L,
    SCN_MAX_CURRENT_VALID_TIME_L,
    VALID_TIME_S,
)

# SCN_MAX_CURRENT_VALID_TIME_L keys the "SCN max current valid time" sensors below;
# unlike per-socket max current, SCN max current is not auto-renewed (see number.py).
from .repairs import async_check_firmware, async_clear_firmware_issue

_LOGGER = logging.getLogger(__name__)

ALFEN_MODBUS_SCHEMA = vol.Schema(
    {
        vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
        vol.Required(CONF_HOST): cv.string,
        vol.Required(CONF_PORT): cv.positive_int,
        vol.Optional(
            CONF_MODBUS_ADDRESS, default=DEFAULT_MODBUS_ADDRESS
        ): cv.positive_int,
        vol.Optional(CONF_READ_SCN, default=DEFAULT_READ_SCN): cv.boolean,
        vol.Optional(CONF_READ_SOCKET2, default=DEFAULT_READ_SOCKET2): cv.boolean,
        vol.Optional(
            CONF_SCAN_INTERVAL, default=DEFAULT_SCAN_INTERVAL
        ): cv.positive_int,
        vol.Optional(
            CONF_POLL_IDLE_INTERVAL, default=DEFAULT_POLL_IDLE_INTERVAL
        ): cv.positive_int,
        vol.Optional(
            CONF_POLL_CONNECTED_INTERVAL, default=DEFAULT_POLL_CONNECTED_INTERVAL
        ): cv.positive_int,
        vol.Optional(
            CONF_POLL_STATISTICS_INTERVAL, default=DEFAULT_POLL_STATISTICS_INTERVAL
        ): cv.positive_int,
    }
)

CONFIG_SCHEMA = vol.Schema(
    {DOMAIN: vol.Schema({cv.slug: ALFEN_MODBUS_SCHEMA})}, extra=vol.ALLOW_EXTRA
)

PLATFORMS = ["binary_sensor", "number", "select", "sensor", "switch"]

type AlfenConfigEntry = ConfigEntry[AlfenModbusHub]


async def async_setup(hass, config):
    """Set up the Alfen modbus component."""
    return True


async def async_setup_entry(hass: HomeAssistant, entry: AlfenConfigEntry):
    """Set up a alfen mobus."""
    host = entry.data[CONF_HOST]
    name = entry.data[CONF_NAME]
    port = entry.data[CONF_PORT]
    address = entry.data.get(CONF_MODBUS_ADDRESS, 1)
    scan_interval = entry.data[CONF_SCAN_INTERVAL]
    poll_idle_interval = entry.data.get(
        CONF_POLL_IDLE_INTERVAL, DEFAULT_POLL_IDLE_INTERVAL
    )
    poll_connected_interval = entry.data.get(
        CONF_POLL_CONNECTED_INTERVAL, DEFAULT_POLL_CONNECTED_INTERVAL
    )
    poll_statistics_interval = entry.data.get(
        CONF_POLL_STATISTICS_INTERVAL, DEFAULT_POLL_STATISTICS_INTERVAL
    )
    read_scn = entry.data.get(CONF_READ_SCN, False)
    read_socket2 = entry.data.get(CONF_READ_SOCKET2, False)

    _LOGGER.info("Setup %s.%s (integration v0.2.1, split socket reads, fixes issue #40)", DOMAIN, name)

    hub = AlfenModbusHub(
        hass,
        name,
        host,
        port,
        address,
        scan_interval,
        poll_idle_interval,
        poll_connected_interval,
        poll_statistics_interval,
        read_scn,
        read_socket2
    )
    """Register the hub."""
    entry.runtime_data = hub

    # Read device info before setting up platforms so device_info is available.
    # Connecting happens lazily inside read_modbus_data (off the event loop).
    # Only treat connection-level failures as "not ready yet" (HA will retry
    # setup automatically); anything else is a real bug and should surface
    # as a normal traceback instead of retrying forever.
    try:
        await hub.read_modbus_data()
    except (ConnectionError, OSError, ModbusException) as err:
        _LOGGER.exception("Unable to connect to %s:%s", host, port)
        raise ConfigEntryNotReady(f"Unable to connect to {host}:{port}") from err

    async_check_firmware(
        hass,
        entry.entry_id,
        hub.data.get("platformType", ""),
        hub.data.get("firmwareVersion", ""),
    )

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass, entry):
    """Unload Alfen mobus entry."""
    unload_ok = all(
        await asyncio.gather(
            *[
                hass.config_entries.async_forward_entry_unload(entry, component)
                for component in PLATFORMS
            ]
        )
    )
    if unload_ok:
        async_clear_firmware_issue(hass, entry.entry_id)
    return unload_ok


def validate(value, comparison, against):
    ops = {
        ">": operator.gt,
        "<": operator.lt,
        ">=": operator.ge,
        "<=": operator.le,
        "==": operator.eq,
        "!=": operator.ne,
    }
    if not ops[comparison](value, against):
        raise ValueError(f"Value {value} failed validation ({comparison}{against})")
    return value


class AlfenModbusHub:
    """Async-safe wrapper class for pymodbus."""

    def __init__(
        self,
        hass,
        name,
        host,
        port,
        address,
        scan_interval,
        poll_idle_interval,
        poll_connected_interval,
        poll_statistics_interval,
        read_scn=False,
        read_socket_2=False
    ):
        """Initialize the Modbus hub."""
        self._hass = hass
        self._client = ModbusTcpClient(host=host, port=port)
        self._lock = asyncio.Lock()
        self._name = name
        self._address = address
        self.read_scn = read_scn
        self.read_socket_2 = read_socket_2
        self._refreshInterval = scan_interval
        self._poll_idle_interval = poll_idle_interval
        self._poll_connected_interval = poll_connected_interval
        self._poll_statistics_interval = poll_statistics_interval
        self._scan_interval = timedelta(
            seconds=min(
                self._poll_idle_interval,
                self._poll_connected_interval,
                self._poll_statistics_interval,
            )
        )
        self._unsub_interval_method = None
        self._sensors = []
        self._inputs = []
        self.data = {}
        self._closing = False
        self._polling = False
        self._product_data_loaded = False
        self._station_data_loaded = False
        self._last_status_poll = 0.0
        self._last_statistics_poll = 0.0

    @callback
    def async_add_alfen_sensor(self, update_callback, refresh_callback = None):
        """Listen for data updates."""
        # This is the first sensor, set up interval.
        if not self._sensors:
            self._closing = False
            self._unsub_interval_method = async_track_time_interval(
                self._hass, self.async_refresh_modbus_data, self._scan_interval
            )
            # Schedule initial data read as a task (non-blocking); pymodbus's
            # sync client connects on demand from within that executor job.
            self._hass.async_create_task(self.read_modbus_data())

        self._sensors.append(update_callback)
        if refresh_callback is not None:
           self._inputs.append(refresh_callback)

    @callback
    def async_remove_alfen_sensor(self, update_callback, refresh_callback = None):
        """Remove data update."""
        self._sensors.remove(update_callback)
        if refresh_callback is not None:
           self._inputs.remove(refresh_callback)
        if not self._sensors:
            """stop the interval timer upon removal of last sensor"""
            self._unsub_interval_method()
            self._unsub_interval_method = None
            # Block any read still in flight from silently reopening the
            # connection after we close it below (pymodbus auto-connects).
            self._closing = True
            # Close under the lock, off the event loop, so this can't race
            # an in-flight executor read/write on the same socket.
            self._hass.async_create_task(self._async_close())

    async def _async_close(self):
        async with self._lock:
            try:
                await self._hass.async_add_executor_job(self._client.close)
            except Exception as err:  # noqa: BLE001 - teardown must never raise
                _LOGGER.debug("Error closing Modbus client: %s", err)



    async def async_refresh_modbus_data(self, _now: int | None = None) -> None:
        """Time to update."""
        if not self._sensors or self._polling:
            return

        self._polling = True
        try:
            update_result = await self.read_modbus_data()
        except Exception:
            _LOGGER.exception("Error reading modbus data")
            update_result = False
        finally:
            self._polling = False

        if update_result:
            for update_callback in self._sensors:
                update_callback()
            self.refresh_max_current()

    @property
    def name(self):
        """Return the name of this hub."""
        return self._name

    @property
    def has_socket_2(self):
        """Return true if a meter is available"""
        return self.read_socket_2

    @property
    def has_scn(self):
        """Return true if a battery is available"""
        return self.read_scn

    async def read_holding_registers(self, unit, address, count):
        """Read holding registers."""
        def _do_read():
            # Runs in the executor thread, off the event loop: pymodbus's
            # sync client connects on demand inside read_holding_registers.
            return self._client.read_holding_registers(address=address, count=count, device_id=unit)

        try:
            async with self._lock:
                if self._closing:
                    # Lost the race with teardown: the socket may already be
                    # closed. Don't let pymodbus silently reopen it here, and
                    # don't go through the reconnect-retry path below for it.
                    return None
                return await self._hass.async_add_executor_job(_do_read)
        except (BrokenPipeError, ConnectionError, OSError, ModbusException) as e:
            if self._closing:
                return None
            _LOGGER.warning("Connection error during read, attempting reconnect: %s", e)
            # Try to reconnect once
            def _do_reconnect_and_read():
                try:
                    self._client.close()
                except (OSError, ModbusException) as close_err:
                    _LOGGER.debug("Error closing Modbus client before reconnect: %s", close_err)
                self._client.connect()
                return self._client.read_holding_registers(address=address, count=count, device_id=unit)

            try:
                async with self._lock:
                    return await self._hass.async_add_executor_job(_do_reconnect_and_read)
            except Exception as retry_error:
                _LOGGER.error("Failed to reconnect and retry read: %s", retry_error)
                raise

    async def read_holding_registers_split(self, unit, chunks):
        """Read holding registers in multiple chunks and concatenate them.

        Needed because the socket measurement block (300-425) is 126 registers,
        which exceeds the 125-register FC3 limit. Chunks must align with value
        boundaries: firmware rejects reads that partially cover a defined value.
        Returns the concatenated register list, or None if any chunk fails.
        """
        registers = []
        for address, count in chunks:
            result = await self.read_holding_registers(unit, address, count)
            if result is None or result.isError():
                return None
            registers.extend(result.registers)
        return registers

    async def write_registers(self, unit, address, payload):
        """Write registers."""
        def _do_write():
            # Runs in the executor thread, off the event loop: pymodbus's
            # sync client connects on demand inside write_registers.
            return self._client.write_registers(address=address, values=payload, device_id=unit)

        try:
            async with self._lock:
                if self._closing:
                    return None
                return await self._hass.async_add_executor_job(_do_write)
        except (BrokenPipeError, ConnectionError, OSError, ModbusException) as e:
            if self._closing:
                return None
            _LOGGER.warning("Connection error during write, attempting reconnect: %s", e)
            # Try to reconnect once
            def _do_reconnect_and_write():
                try:
                    self._client.close()
                except (OSError, ModbusException) as close_err:
                    _LOGGER.debug("Error closing Modbus client before reconnect: %s", close_err)
                self._client.connect()
                return self._client.write_registers(address=address, values=payload, device_id=unit)

            try:
                async with self._lock:
                    return await self._hass.async_add_executor_job(_do_reconnect_and_write)
            except Exception as retry_error:
                _LOGGER.error("Failed to reconnect and retry write: %s", retry_error)
                raise
            
    def refresh_max_current(self):
        # Guard against KeyError if data hasn't been populated yet
        key1 = VALID_TIME_S + "1"
        key2 = VALID_TIME_S + "2"
        if key1 not in self.data:
            return
        refresh_interval = self._poll_connected_interval
        if int(self.data[key1]) < refresh_interval + 10 or (
            self.has_socket_2
            and key2 in self.data
            and int(self.data[key2]) < refresh_interval + 10
        ):
            for update_callback in self._inputs:
                # Schedule async callbacks as tasks
                result = update_callback()
                if asyncio.iscoroutine(result):
                    self._hass.async_create_task(result)
            
            

    async def read_modbus_data(self):
        if self._closing:
            return False

        if not self._product_data_loaded:
            if not await self.read_modbus_data_product():
                return False
            self._product_data_loaded = True

        if not self._station_data_loaded:
            if not await self.read_modbus_data_station():
                return False
            self._station_data_loaded = True

        self._update_station_time()

        now = asyncio.get_running_loop().time()
        status_interval = (
            self._poll_connected_interval
            if self._any_car_connected()
            else self._poll_idle_interval
        )
        status_due = (
            self._last_status_poll == 0.0
            or now - self._last_status_poll >= status_interval
        )
        statistics_due = (
            self._last_statistics_poll == 0.0
            or now - self._last_statistics_poll >= self._poll_statistics_interval
        )

        if not status_due and not statistics_due:
            return False

        if status_due:
            if not await self.read_modbus_data_socket_status(1):
                return False
            if not await self.read_modbus_data_socket_status(2):
                return False
            self._last_status_poll = now

            if self._any_car_connected():
                if not await self.read_modbus_data_socket_fast(1):
                    return False
                if not await self.read_modbus_data_socket_fast(2):
                    return False
            elif not await self.read_modbus_data_station():
                return False

        if statistics_due:
            if not await self.read_modbus_data_socket_statistics(1):
                return False
            if not await self.read_modbus_data_socket_statistics(2):
                return False
            if not await self.read_modbus_data_scn():
                return False
            self._last_statistics_poll = now

        return True

    def decode_string(self, decoder,length):
        s = decoder.decode_string(length*2)  # get 32 char string
        s = s.partition(b"\0")[0]  # omit NULL terminators
        s = s.decode("utf-8")  # decode UTF-8
        return str(s)

    def _socket_enabled(self, socket):
        if socket == 1:
            return True
        if socket != 2 or not self.has_socket_2:
            return False
        return self.data.get("numberOfSockets", 2) >= 2

    def _update_station_time(self):
        if "stationTime" in self.data:
            tzinfo = self.data["stationTime"].tzinfo
            self.data["stationTime"] = datetime.now(tz=tzinfo).replace(microsecond=0)

    def _any_car_connected(self):
        sockets = [1, 2] if self.has_socket_2 else [1]
        return any(self.data.get(f"socket_{socket}_carconnected") == 1 for socket in sockets)

    def _update_session_data(self, socket):
        prefix = f"socket_{socket}_"
        if self.data.get(prefix + "carconnected") == 0:
            self.data.pop(prefix + "chargingStartWh", None)
            self.data.pop(prefix + "chargingStart", None)
            self.data.pop(prefix + "currentSession", None)
            self.data.pop(prefix + "currentSessionDuration", None)
            return

        if (
            prefix + "chargingStartWh" not in self.data
            and prefix + "realEnergyDeliveredSum" in self.data
        ):
            self.data[prefix + "chargingStartWh"] = self.data[prefix + "realEnergyDeliveredSum"]
            if "stationTime" in self.data:
                self.data[prefix + "chargingStart"] = self.data["stationTime"]

        if (
            prefix + "chargingStartWh" in self.data
            and prefix + "realEnergyDeliveredSum" in self.data
        ):
            self.data[prefix + "currentSession"] = (
                self.data[prefix + "realEnergyDeliveredSum"]
                - self.data[prefix + "chargingStartWh"]
            )

        if (
            prefix + "chargingStart" in self.data
            and "stationTime" in self.data
        ):
            self.data[prefix + "currentSessionDuration"] = (
                self.data["stationTime"] - self.data[prefix + "chargingStart"]
            )

    def decode_from_registers(self, registers, offset, count, data_type):
        return self._client.convert_from_registers(registers[offset:offset+count], data_type=data_type, word_order='big')

    async def read_modbus_data_station(self):
        status_data = await self.read_holding_registers(self._address,1100,6)
        if status_data is None or status_data.isError():
            return False
    
        self.data["actualMaxCurrent"] =  round(self.decode_from_registers(status_data.registers,0,2,self._client.DATATYPE.FLOAT32),2)
        self.data["boardTemperature"] =  round(self.decode_from_registers(status_data.registers,2,2,self._client.DATATYPE.FLOAT32),3)
        self.data["backofficeConnected"] = self.data["backoffice"] = self.decode_from_registers(status_data.registers,4,1,self._client.DATATYPE.UINT16)
        self.data["numberOfSockets"] = self.decode_from_registers(status_data.registers,5,1,self._client.DATATYPE.UINT16)
        return True
        
    async def read_modbus_data_scn(self):
        if(self.has_scn):
            status_data = await self.read_holding_registers(self._address,1400,32)
            if status_data is None or status_data.isError():
                return False

            self.data["scnName"] = self.decode_from_registers(status_data.registers,0,4,self._client.DATATYPE.STRING).strip('\x00')
            self.data["scnSockets"] =  self.decode_from_registers(status_data.registers,4,1,self._client.DATATYPE.UINT16)

            self.data["scnTotalConsumptionL1"] = round(self.decode_from_registers(status_data.registers,5,2,self._client.DATATYPE.FLOAT32),2)
            self.data["scnTotalConsumptionL2"] = round(self.decode_from_registers(status_data.registers,7,2,self._client.DATATYPE.FLOAT32),2)
            self.data["scnTotalConsumptionL3"] = round(self.decode_from_registers(status_data.registers,9,2,self._client.DATATYPE.FLOAT32),2)

            self.data["scnActualMaxCurrentL1"] = round(self.decode_from_registers(status_data.registers,11,2,self._client.DATATYPE.FLOAT32),2)
            self.data["scnActualMaxCurrentL2"] = round(self.decode_from_registers(status_data.registers,13,2,self._client.DATATYPE.FLOAT32),2)
            self.data["scnActualMaxCurrentL3"] = round(self.decode_from_registers(status_data.registers,15,2,self._client.DATATYPE.FLOAT32),2)

            self.data[SCN_MAX_CURRENT_L+"L1"] = round(self.decode_from_registers(status_data.registers,17,2,self._client.DATATYPE.FLOAT32),2)
            self.data[SCN_MAX_CURRENT_L+"L2"] = round(self.decode_from_registers(status_data.registers,19,2,self._client.DATATYPE.FLOAT32),2)
            self.data[SCN_MAX_CURRENT_L+"L3"] = round(self.decode_from_registers(status_data.registers,21,2,self._client.DATATYPE.FLOAT32),2)

            self.data[SCN_MAX_CURRENT_VALID_TIME_L+"L1"] = self.decode_from_registers(status_data.registers,23,2,self._client.DATATYPE.UINT32)
            self.data[SCN_MAX_CURRENT_VALID_TIME_L+"L2"] = self.decode_from_registers(status_data.registers,25,2,self._client.DATATYPE.UINT32)
            self.data[SCN_MAX_CURRENT_VALID_TIME_L+"L3"] = self.decode_from_registers(status_data.registers,27,2,self._client.DATATYPE.UINT32)

            self.data["scnSafeCurrent"] = round(self.decode_from_registers(status_data.registers,29,2,self._client.DATATYPE.FLOAT32),2)
            self.data["scnMaxCurrentEnabled"] = self.decode_from_registers(status_data.registers,31,1,self._client.DATATYPE.UINT16)
        return True
        
    async def read_modbus_data_socket_status(self, socket):
        if not self._socket_enabled(socket):
            return True

        status_data = await self.read_holding_registers(socket, 1200, 16)
        if status_data is None or status_data.isError():
            return False

        key_prefix = f"socket_{socket}_"
        self.data[key_prefix + "available"] = self.decode_from_registers(
            status_data.registers, 0, 1, self._client.DATATYPE.UINT16
        )
        mode3 = (
            self.decode_from_registers(
                status_data.registers, 1, 5, self._client.DATATYPE.STRING
            )
            .strip("\x00")
            .strip()
            .upper()
        )
        self.data[key_prefix + "mode3state"] = mode3
        self.data[key_prefix + "actualMaxCurrent"] = round(
            self.decode_from_registers(
                status_data.registers, 6, 2, self._client.DATATYPE.FLOAT32
            ),
            2,
        )
        self.data[VALID_TIME_S + str(socket)] = self.decode_from_registers(
            status_data.registers, 8, 2, self._client.DATATYPE.UINT32
        )
        self.data[MAX_CURRENT_S + str(socket)] = round(
            self.decode_from_registers(
                status_data.registers, 10, 2, self._client.DATATYPE.FLOAT32
            ),
            2,
        )
        self.data[key_prefix + "saveCurrent"] = round(
            self.decode_from_registers(
                status_data.registers, 12, 2, self._client.DATATYPE.FLOAT32
            ),
            2,
        )
        self.data[key_prefix + "setpointAccounted"] = self.decode_from_registers(
            status_data.registers, 14, 1, self._client.DATATYPE.UINT16
        )
        self.data[key_prefix + "chargephases"] = self.decode_from_registers(
            status_data.registers, 15, 1, self._client.DATATYPE.UINT16
        )

        self.data[key_prefix + "carconnected"] = (
            0 if not mode3 or mode3.startswith(("A", "E", "F")) else 1
        )
        self.data[key_prefix + "carcharging"] = (
            1 if mode3.startswith(("C", "D")) and mode3.endswith("2") else 0
        )
        self.data[key_prefix + "chargerenabled"] = (
            1 if self.data[MAX_CURRENT_S + str(socket)] > 0 else 0
        )

        charge_phases = self.data[key_prefix + "chargephases"]
        if charge_phases in PHASE_SWITCH_OPTIONS:
            self.data["usephases_S" + str(socket)] = PHASE_SWITCH_OPTIONS[charge_phases]

        self._update_session_data(socket)
        return True

    async def read_modbus_data_socket_fast(self, socket):
        if not self._socket_enabled(socket):
            return True

        fast_data = await self.read_holding_registers(socket, 320, 26)
        if fast_data is None or fast_data.isError():
            current_l1_data = await self.read_holding_registers(socket, 320, 2)
            power_sum_data = await self.read_holding_registers(socket, 344, 2)
            if (
                current_l1_data is None
                or current_l1_data.isError()
                or power_sum_data is None
                or power_sum_data.isError()
            ):
                return False
            current_l1_registers = current_l1_data.registers
            power_sum_registers = power_sum_data.registers
            current_l1 = round(
                self.decode_from_registers(
                    current_l1_registers, 0, 2, self._client.DATATYPE.FLOAT32
                ),
                2,
            )
            real_power_sum = round(
                self.decode_from_registers(
                    power_sum_registers, 0, 2, self._client.DATATYPE.FLOAT32
                ),
                2,
            )
        else:
            current_l1 = round(
                self.decode_from_registers(
                    fast_data.registers, 0, 2, self._client.DATATYPE.FLOAT32
                ),
                2,
            )
            real_power_sum = round(
                self.decode_from_registers(
                    fast_data.registers, 24, 2, self._client.DATATYPE.FLOAT32
                ),
                2,
            )

        key_prefix = f"socket_{socket}_"
        self.data[key_prefix + "currentL1"] = current_l1
        self.data[key_prefix + "realPowerSum"] = real_power_sum
        return True

    async def read_modbus_data_socket_statistics(self, socket):
        if not self._socket_enabled(socket):
            return True

        statistics_data = await self.read_holding_registers(socket, 374, 4)
        if statistics_data is None or statistics_data.isError():
            return False

        key_prefix = f"socket_{socket}_"
        self.data[key_prefix + "realEnergyDeliveredSum"] = round(
            self.decode_from_registers(
                statistics_data.registers, 0, 4, self._client.DATATYPE.FLOAT64
            ),
            2,
        )
        self._update_session_data(socket)
        return True
        
        
    async def read_modbus_data_product(self):
        identification_data = await self.read_holding_registers(self._address, 100, 79)
        if identification_data is None or identification_data.isError():
            return False

        self.data["name"] = self.decode_from_registers(identification_data.registers, 0, 17, self._client.DATATYPE.STRING).strip('\x00')
        self.data["manufacturer"] = self.decode_from_registers(identification_data.registers, 17, 5, self._client.DATATYPE.STRING).strip('\x00')
        self.data["modbustableVersion"] = self.decode_from_registers(identification_data.registers, 22, 1,self._client.DATATYPE.INT16)
        self.data["firmwareVersion"] = self.decode_from_registers(identification_data.registers, 23, 17, self._client.DATATYPE.STRING).strip('\x00')
        self.data["platformType"] = self.decode_from_registers(identification_data.registers, 40, 17, self._client.DATATYPE.STRING).strip('\x00')
        self.data["serial"] = self.decode_from_registers(identification_data.registers, 57, 11, self._client.DATATYPE.STRING).strip('\x00')

        year    = self.decode_from_registers(identification_data.registers, 68, 1,self._client.DATATYPE.INT16)
        month   = self.decode_from_registers(identification_data.registers, 69, 1,self._client.DATATYPE.INT16)
        day     = self.decode_from_registers(identification_data.registers, 70, 1,self._client.DATATYPE.INT16)
        hour    = self.decode_from_registers(identification_data.registers, 71, 1,self._client.DATATYPE.INT16)
        minute  = self.decode_from_registers(identification_data.registers, 72, 1,self._client.DATATYPE.INT16)
        second  = self.decode_from_registers(identification_data.registers, 73, 1,self._client.DATATYPE.INT16)
        uptime  = self.decode_from_registers(identification_data.registers, 74, 4,self._client.DATATYPE.UINT64)
        utcoffset = self.decode_from_registers(identification_data.registers, 78, 1,self._client.DATATYPE.INT16)

        # Tijdconversie
        self.data["stationTime"] = datetime(
            year, month, day, hour, minute, second,
            tzinfo=tzoffset("", utcoffset * 60)
        )

        last_boot = self.data["stationTime"] - timedelta(milliseconds=uptime)
        self.data["lastBoot"] = last_boot.replace(microsecond=0)

        return True
