#!/usr/bin/env python3
"""
Sensor component for Cryptoinfo Advanced
Author: TheHoliestRoger
"""

import aiohttp
import asyncio
import async_timeout
import json
import time
import traceback
from datetime import datetime, timedelta, timezone
from dateutil import parser as dtparser

from .const.const import (
    _LOGGER,
    CONF_EXTRA_SENSOR_PROPERTY,
    SENSOR_PREFIX,
    ATTR_LAST_UPDATE,
    ATTR_24H_VOLUME,
    ATTR_BASE_PRICE,
    ATTR_1H_CHANGE,
    ATTR_24H_CHANGE,
    ATTR_7D_CHANGE,
    ATTR_30D_CHANGE,
    ATTR_MARKET_CAP,
    ATTR_CIRCULATING_SUPPLY,
    ATTR_TOTAL_SUPPLY,
    ATTR_ALL_TIME_HIGH,
    ATTR_ALL_TIME_HIGH_DATE,
    ATTR_ALL_TIME_HIGH_DAYS,
    ATTR_ALL_TIME_HIGH_DISTANCE,
    ATTR_ALL_TIME_LOW,
    ATTR_ALL_TIME_LOW_DATE,
    ATTR_ALL_TIME_LOW_DAYS,
    ATTR_24H_LOW,
    ATTR_24H_HIGH,
    ATTR_IMAGE_URL,
    ATTR_DIFFICULTY,
    ATTR_DIFFICULTY_CALC,
    ATTR_HASHRATE,
    ATTR_HASHRATE_CALC,
    ATTR_POOL_CONTROL_1000B,
    ATTR_POOL_CONTROL_1000B_PERC,
    ATTR_BLOCK_HEIGHT,
    ATTR_DIFFICULTY_BLOCK_PROGRESS,
    ATTR_DIFFICULTY_RETARGET_HEIGHT,
    ATTR_DIFFICULTY_RETARGET_SECONDS,
    ATTR_DIFFICULTY_RETARGET_PERCENT_CHANGE,
    ATTR_DIFFICULTY_RETARGET_ESTIMATED_DIFF,
    ATTR_HALVING_BLOCK_PROGRESS,
    ATTR_HALVING_BLOCKS_REMAINING,
    ATTR_NEXT_HALVING_HEIGHT,
    ATTR_TOTAL_HALVINGS_TO_DATE,
    ATTR_WORKER_COUNT,
    ATTR_LAST_BLOCK,
    ATTR_BLOCKS_PENDING,
    ATTR_BLOCKS_CONFIRMED,
    ATTR_BLOCKS_ORPHANED,
    ATTR_BLOCK_TIME_IN_SECONDS,
    ATTR_MEMPOOL_FEES_FASTEST,
    ATTR_MEMPOOL_FEES_30MIN,
    ATTR_MEMPOOL_FEES_60MIN,
    ATTR_MEMPOOL_FEES_ECO,
    ATTR_MEMPOOL_FEES_MINIMUM,
    ATTR_MEMPOOL_NEXT_BLOCK_SIZE,
    ATTR_MEMPOOL_NEXT_BLOCK_SIZE_CALC,
    ATTR_MEMPOOL_NEXT_BLOCK_TX_COUNT,
    ATTR_MEMPOOL_NEXT_BLOCK_TOTAL_FEE,
    ATTR_MEMPOOL_NEXT_BLOCK_TOTAL_FEE_CALC,
    ATTR_MEMPOOL_NEXT_BLOCK_MEDIAN_FEE,
    ATTR_MEMPOOL_NEXT_BLOCK_FEE_RANGE_COMBINED,
    ATTR_MEMPOOL_NEXT_BLOCK_FEE_RANGE_MIN,
    ATTR_MEMPOOL_NEXT_BLOCK_FEE_RANGE_MAX,
    ATTR_MEMPOOL_TX_COUNT,
    ATTR_MEMPOOL_TOTAL_FEE,
    ATTR_MEMPOOL_TOTAL_FEE_CALC,
    ATTR_MEMPOOL_SIZE_CALC,
    ATTR_MEMPOOL_AVERAGE_FEE_PER_TX,
    API_BASE_URL_COINGECKO,
    API_BASE_URL_CRYPTOID,
    API_BASE_URL_MEMPOOLSPACE,
    API_ENDPOINT_PRICE_MAIN,
    API_ENDPOINT_PRICE_ALT,
    API_ENDPOINT_DOMINANCE,
    API_ENDPOINT_CHAIN_SUMMARY,
    API_ENDPOINT_CHAIN_CONTROL,
    API_ENDPOINT_CHAIN_ORPHANS,
    API_ENDPOINT_CHAIN_BLOCK_TIME,
    API_ENDPOINT_NOMP_POOL_STATS,
    API_ENDPOINT_MEMPOOL_FEES,
    API_ENDPOINT_MEMPOOL_NEXT_BLOCKS,
    API_ENDPOINT_MEMPOOL_STATS,
    CONF_DIFF_MULTIPLIER,
    CONF_BLOCK_TIME_MINUTES,
    CONF_DIFFICULTY_WINDOW,
    CONF_HALVING_WINDOW,
    DEFAULT_CHAIN_DIFFICULTY_WINDOW,
    DEFAULT_CHAIN_DIFF_MULTIPLIER,
    DEFAULT_CHAIN_BLOCK_TIME_MINS,
    DEFAULT_CHAIN_HALVING_WINDOW,
    DEFAULT_MAX_FETCH_FAILURES,
    DAY_SECONDS,
    PROPERTY_POOL_CONTROL_REMAINING,
)

from .manager import CryptoInfoAdvEntityManager, CryptoInfoAdvDataFetchType
from .utils import unit_to_multiplier, currency_to_multiplier

from homeassistant.components.sensor import (
    CONF_STATE_CLASS,
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import (
    CONF_ID,
    CONF_UNIQUE_ID,
    CONF_UNIT_OF_MEASUREMENT,
)
from homeassistant.exceptions import TemplateError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.template import Template
from homeassistant.util import Throttle


class CryptoinfoAdvSensor(SensorEntity):
    def __init__(
        self,
        hass,
        cryptocurrency_name,
        currency_name,
        unit_of_measurement,
        multiplier,
        update_frequency,
        id_name,
        unique_id=None,
        state_class=None,
        api_mode="",
        pool_prefix=[""],
        fetch_args="",
        extra_sensors="",
        api_domain_name="",
        pool_name="",
        diff_multiplier="",
        block_time_minutes="",
        difficulty_window="",
        halving_window="",
        max_fetch_failures=None,
        is_child_sensor=False,
    ):
        # Internal Properties
        self.hass = hass
        self._session = async_get_clientsession(hass) if hass is not None else None
        self.data = None
        self.cryptocurrency_name = cryptocurrency_name
        self.currency_name = currency_name
        self.pool_prefixes = pool_prefix if isinstance(pool_prefix, list) else [pool_prefix]
        self.multiplier = multiplier
        self._diff_multiplier = int(diff_multiplier) if diff_multiplier.isdigit() else DEFAULT_CHAIN_DIFF_MULTIPLIER
        self._block_time_minutes = float(block_time_minutes) if block_time_minutes.replace(
            ".", "", 1).isdigit() else DEFAULT_CHAIN_BLOCK_TIME_MINS
        self._difficulty_window = int(difficulty_window) if difficulty_window.isdigit() else DEFAULT_CHAIN_DIFFICULTY_WINDOW
        self._halving_window = int(halving_window) if halving_window.isdigit() else DEFAULT_CHAIN_HALVING_WINDOW
        self._max_fetch_failures = int(max_fetch_failures) if max_fetch_failures is not None else DEFAULT_MAX_FETCH_FAILURES
        self._internal_id_name = id_name if id_name is not None else ""
        self._fetch_type = CryptoInfoAdvEntityManager.instance().get_fetch_type_from_str(api_mode)
        self._fetch_args = fetch_args if fetch_args and len(fetch_args) else None
        self._api_domain_name = api_domain_name if api_domain_name and len(api_domain_name) else None
        self._pool_name = pool_name if pool_name and len(pool_name) else None
        self._update_frequency = update_frequency if isinstance(update_frequency, timedelta) else timedelta(minutes=1)
        self._is_child_sensor = is_child_sensor
        self._child_sensors = list()
        self._child_sensor_config = extra_sensors
        self._fetch_failure_count = 0

        # HASS Attributes
        self.async_update = Throttle(update_frequency)(self._async_update)
        self._attr_unique_id = unique_id if unique_id is not None and len(unique_id) else self._build_unique_id()
        self._name = self._build_name()
        self._state = None
        self._last_update = None
        self._icon = "mdi:bitcoin"
        self._attr_device_class = self._build_device_class()
        self._attr_state_class = state_class or SensorStateClass.MEASUREMENT
        self._attr_available = True
        self._unit_of_measurement = unit_of_measurement

        # Sensor Attributes
        self._base_price = None
        self._24h_volume = None
        self._1h_change = None
        self._24h_change = None
        self._7d_change = None
        self._30d_change = None
        self._market_cap = None
        self._circulating_supply = None
        self._total_supply = None
        self._all_time_high = None
        self._all_time_low = None
        self._24h_low = None
        self._24h_high = None
        self._image_url = None
        self._ath_date = None
        self._atl_date = None
        self._difficulty = None
        self._hashrate = None
        self._pool_control_1000b = None
        self._block_height = None
        self._worker_count = None
        self._last_block = None
        self._blocks_pending = None
        self._blocks_confirmed = None
        self._blocks_orphaned = None
        self._mempool_tx_count = None
        self._mempool_total_fee = None
        self._mempool_fees_fastest = None
        self._mempool_fees_30min = None
        self._mempool_fees_60min = None
        self._mempool_fees_eco = None
        self._mempool_fees_minimum = None
        self._mempool_next_block_size = None
        self._mempool_next_block_tx_count = None
        self._mempool_next_block_total_fee = None
        self._mempool_next_block_median_fee = None
        self._mempool_next_block_fee_range_min = None
        self._mempool_next_block_fee_range_max = None

    @property
    def is_child_sensor(self):
        return self._is_child_sensor

    @property
    def cryptocurrency_friendly_name(self):
        return (
            self.cryptocurrency_name.upper()
            if len(self.cryptocurrency_name) <= 6
            else self.cryptocurrency_name.title()
        )

    @property
    def update_frequency(self):
        return self._update_frequency

    @property
    def fetch_type(self):
        return self._fetch_type

    @property
    def hashrate(self):
        return self._hashrate

    @property
    def name(self):
        return self._name

    @property
    def unique_id(self):
        return self._attr_unique_id

    @property
    def available(self):
        return self._attr_available

    @property
    def icon(self):
        return self._icon

    @property
    def state_class(self):
        return self._attr_state_class

    @property
    def unit_of_measurement(self):
        return self._unit_of_measurement

    @property
    def state(self):
        return self._state

    @property
    def block_time_in_seconds(self):
        if self._difficulty is None:
            return None

        best_hashrate = CryptoInfoAdvEntityManager.instance().get_best_hashrate(self.cryptocurrency_name)
        return (self._difficulty * self._diff_multiplier) / best_hashrate

    @property
    def difficulty_block_progress(self):
        if self.state is None:
            return None

        return int(self.state % self._difficulty_window)

    @property
    def difficulty_retarget_height(self):
        if self.state is None:
            return None

        return int(self.state + (self._difficulty_window - self.difficulty_block_progress))

    @property
    def difficulty_previous_target_height(self):
        if self.difficulty_retarget_height is None:
            return None

        return int(self.difficulty_retarget_height - 2016)

    @property
    def difficulty_retarget_seconds(self):
        if self.difficulty_retarget_height is None or self.block_time_in_seconds is None:
            return None

        return int((self.difficulty_retarget_height - self.state) * self.block_time_in_seconds)

    @property
    def difficulty_retarget_percent_change(self):
        if self.difficulty_retarget_seconds is None:
            return None

        last_diff_timestamp = CryptoInfoAdvEntityManager.instance().get_block_time(self.cryptocurrency_name)
        if last_diff_timestamp is None:
            return None

        time_window_expected = (self._block_time_minutes * 60) * self._difficulty_window
        time_next_diff = int(int(time.time()) + self.difficulty_retarget_seconds)
        time_window_current_diff = time_next_diff - last_diff_timestamp
        actual_percent_change = (((time_window_expected - time_window_current_diff) / time_window_current_diff) * 100)
        calc_percent_change = max([
            min([actual_percent_change, 300]),
            -75
        ])
        return round(calc_percent_change, 2)

    @property
    def difficulty_retarget_estimated_diff(self):
        if self.difficulty_retarget_percent_change is None:
            return None
        return round((self._difficulty * (1 + (self.difficulty_retarget_percent_change / 100))), 2)

    @property
    def halving_block_progress(self):
        if self.state is None:
            return None

        return int(self.state % self._halving_window)

    @property
    def halving_blocks_remaining(self):
        if self.halving_block_progress is None:
            return None

        return int(self._halving_window - self.halving_block_progress)

    @property
    def next_halving_height(self):
        if self.state is None or self.halving_block_progress is None:
            return None

        return int(self.state + (self._halving_window - self.halving_block_progress))

    @property
    def total_halvings_to_date(self):
        if self.state is None:
            return None

        return int(self.state // self._halving_window)

    def hashrate_calc(self, unit_of_measurement):
        if self._hashrate is None:
            return None

        return round(float(self._hashrate) / unit_to_multiplier(unit_of_measurement), 4)

    def difficulty_calc(self, unit_of_measurement):
        if self._difficulty is None:
            return None

        return round(float(self._difficulty) / unit_to_multiplier(unit_of_measurement), 4)

    def mempool_size_calc(self, unit_of_measurement):
        if self._state is None:
            return None

        return round(float(self._state) / unit_to_multiplier(unit_of_measurement), 4)

    @property
    def mempool_average_fee_per_tx(self):
        if self._mempool_total_fee is None or self._mempool_tx_count is None:
            return None

        return int(self._mempool_total_fee / self._mempool_tx_count)

    def mempool_total_fee_calc(self, unit_of_measurement):
        if self._mempool_total_fee is None:
            return None

        return round(float(self._mempool_total_fee) / currency_to_multiplier(unit_of_measurement), 4)

    def mempool_next_block_size_calc(self, unit_of_measurement):
        if self._mempool_next_block_size is None:
            return None

        return round(float(self._mempool_next_block_size) / unit_to_multiplier(unit_of_measurement), 4)

    def mempool_next_block_total_fee_calc(self, unit_of_measurement):
        if self._mempool_next_block_total_fee is None:
            return None

        return round(float(self._mempool_next_block_total_fee) / currency_to_multiplier(unit_of_measurement), 4)

    def mempool_next_block_fee_range_combined(self, unit_of_measurement):
        if self._mempool_next_block_fee_range_min is None or self._mempool_next_block_fee_range_max is None:
            return None

        return f"{self._mempool_next_block_fee_range_min:.0f} - {self._mempool_next_block_fee_range_max:.0f}"

    @property
    def all_time_high_distance(self):
        if self._all_time_high is None or self.state is None:
            return None

        return round(float(self._all_time_high) - self.state, 2)

    @property
    def all_time_high_days(self):
        if self._ath_date is None:
            return None

        date_diff = datetime.now(timezone.utc) - self._ath_date

        return date_diff.days

    @property
    def all_time_low_days(self):
        if self._atl_date is None:
            return None

        date_diff = datetime.now(timezone.utc) - self._atl_date

        return date_diff.days

    @property
    def pool_control_1000b_perc(self):
        if self._pool_control_1000b is None:
            return None

        return round(((float(self._pool_control_1000b) / 1000.0) * 100.0), 4)

    def get_extra_state_attrs(self, full_attr_force=False):
        output_attrs = {
            ATTR_LAST_UPDATE: self._last_update,
        }

        if full_attr_force or self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_price_types:
            output_attrs[ATTR_BASE_PRICE] = self._base_price
            output_attrs[ATTR_24H_VOLUME] = self._24h_volume
            output_attrs[ATTR_24H_CHANGE] = self._24h_change

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.PRICE_MAIN:
            output_attrs[ATTR_1H_CHANGE] = self._1h_change
            output_attrs[ATTR_7D_CHANGE] = self._7d_change
            output_attrs[ATTR_30D_CHANGE] = self._30d_change
            output_attrs[ATTR_CIRCULATING_SUPPLY] = self._circulating_supply
            output_attrs[ATTR_TOTAL_SUPPLY] = self._total_supply
            output_attrs[ATTR_ALL_TIME_HIGH] = self._all_time_high
            output_attrs[ATTR_ALL_TIME_LOW] = self._all_time_low
            output_attrs[ATTR_24H_LOW] = self._24h_low
            output_attrs[ATTR_24H_HIGH] = self._24h_high
            output_attrs[ATTR_IMAGE_URL] = self._image_url
            output_attrs[ATTR_ALL_TIME_HIGH_DATE] = self._ath_date
            output_attrs[ATTR_ALL_TIME_LOW_DATE] = self._atl_date

        if full_attr_force or self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_supply_types:
            output_attrs[ATTR_CIRCULATING_SUPPLY] = self._circulating_supply

        if full_attr_force or self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_market_cap_types:
            output_attrs[ATTR_MARKET_CAP] = self._market_cap

        if full_attr_force or self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_block_height_types:
            output_attrs[ATTR_BLOCK_HEIGHT] = self._block_height

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_SUMMARY:
            output_attrs[ATTR_DIFFICULTY] = self._difficulty
            output_attrs[ATTR_HASHRATE] = self._hashrate
            output_attrs[CONF_DIFF_MULTIPLIER] = self._diff_multiplier
            output_attrs[CONF_BLOCK_TIME_MINUTES] = self._block_time_minutes
            output_attrs[CONF_DIFFICULTY_WINDOW] = self._difficulty_window
            output_attrs[CONF_HALVING_WINDOW] = self._halving_window

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_CONTROL:
            output_attrs[ATTR_POOL_CONTROL_1000B] = self._pool_control_1000b

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.NOMP_POOL_STATS:
            output_attrs[ATTR_WORKER_COUNT] = self._worker_count
            output_attrs[ATTR_LAST_BLOCK] = self._last_block
            output_attrs[ATTR_BLOCKS_PENDING] = self._blocks_pending
            output_attrs[ATTR_BLOCKS_CONFIRMED] = self._blocks_confirmed
            output_attrs[ATTR_BLOCKS_ORPHANED] = self._blocks_orphaned

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_STATS:
            output_attrs[ATTR_MEMPOOL_TX_COUNT] = self._mempool_tx_count
            output_attrs[ATTR_MEMPOOL_TOTAL_FEE] = self._mempool_total_fee

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_FEES:
            output_attrs[ATTR_MEMPOOL_FEES_FASTEST] = self._mempool_fees_fastest
            output_attrs[ATTR_MEMPOOL_FEES_30MIN] = self._mempool_fees_30min
            output_attrs[ATTR_MEMPOOL_FEES_60MIN] = self._mempool_fees_60min
            output_attrs[ATTR_MEMPOOL_FEES_ECO] = self._mempool_fees_eco
            output_attrs[ATTR_MEMPOOL_FEES_MINIMUM] = self._mempool_fees_minimum

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_NEXT_BLOCK:
            output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_SIZE] = self._mempool_next_block_size
            output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_TX_COUNT] = self._mempool_next_block_tx_count
            output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_TOTAL_FEE] = self._mempool_next_block_total_fee
            output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_MEDIAN_FEE] = self._mempool_next_block_median_fee
            output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_FEE_RANGE_MIN] = self._mempool_next_block_fee_range_min
            output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_FEE_RANGE_MAX] = self._mempool_next_block_fee_range_max

        return output_attrs

    @property
    def extra_state_attributes(self):
        return self.get_extra_state_attrs()

    def get_extra_sensor_attrs(self, full_attr_force=False, child_sensor=None):
        output_attrs = self.get_extra_state_attrs(full_attr_force=full_attr_force)

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_SUMMARY:

            if child_sensor is None or child_sensor.attribute_key == ATTR_BLOCK_TIME_IN_SECONDS:
                output_attrs[ATTR_BLOCK_TIME_IN_SECONDS] = self.block_time_in_seconds

            if child_sensor is None or child_sensor.attribute_key == ATTR_DIFFICULTY_BLOCK_PROGRESS:
                output_attrs[ATTR_DIFFICULTY_BLOCK_PROGRESS] = self.difficulty_block_progress

            if child_sensor is None or child_sensor.attribute_key == ATTR_DIFFICULTY_RETARGET_HEIGHT:
                output_attrs[ATTR_DIFFICULTY_RETARGET_HEIGHT] = self.difficulty_retarget_height

            if child_sensor is None or child_sensor.attribute_key == ATTR_DIFFICULTY_RETARGET_SECONDS:
                output_attrs[ATTR_DIFFICULTY_RETARGET_SECONDS] = self.difficulty_retarget_seconds

            if child_sensor is None or child_sensor.attribute_key == ATTR_DIFFICULTY_RETARGET_PERCENT_CHANGE:
                output_attrs[ATTR_DIFFICULTY_RETARGET_PERCENT_CHANGE] = self.difficulty_retarget_percent_change

            if child_sensor is None or child_sensor.attribute_key == ATTR_DIFFICULTY_RETARGET_ESTIMATED_DIFF:
                output_attrs[ATTR_DIFFICULTY_RETARGET_ESTIMATED_DIFF] = self.difficulty_retarget_estimated_diff

            if child_sensor is None or child_sensor.attribute_key == ATTR_DIFFICULTY_CALC:
                output_attrs[ATTR_DIFFICULTY_CALC] = self.difficulty_calc(
                    child_sensor.unit_of_measurement if child_sensor is not None else None
                )

            if child_sensor is None or child_sensor.attribute_key == ATTR_HALVING_BLOCK_PROGRESS:
                output_attrs[ATTR_HALVING_BLOCK_PROGRESS] = self.halving_block_progress

            if child_sensor is None or child_sensor.attribute_key == ATTR_HALVING_BLOCKS_REMAINING:
                output_attrs[ATTR_HALVING_BLOCKS_REMAINING] = self.halving_blocks_remaining

            if child_sensor is None or child_sensor.attribute_key == ATTR_NEXT_HALVING_HEIGHT:
                output_attrs[ATTR_NEXT_HALVING_HEIGHT] = self.next_halving_height

            if child_sensor is None or child_sensor.attribute_key == ATTR_TOTAL_HALVINGS_TO_DATE:
                output_attrs[ATTR_TOTAL_HALVINGS_TO_DATE] = self.total_halvings_to_date

        if full_attr_force or self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_hashrate_types:

            if child_sensor is None or child_sensor.attribute_key == ATTR_HASHRATE_CALC:
                output_attrs[ATTR_HASHRATE_CALC] = self.hashrate_calc(
                    child_sensor.unit_of_measurement if child_sensor is not None else None
                )

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_STATS:

            if child_sensor is None or child_sensor.attribute_key == ATTR_MEMPOOL_SIZE_CALC:
                output_attrs[ATTR_MEMPOOL_SIZE_CALC] = self.mempool_size_calc(
                    child_sensor.unit_of_measurement if child_sensor is not None else None
                )

            if child_sensor is None or child_sensor.attribute_key == ATTR_MEMPOOL_TOTAL_FEE_CALC:
                output_attrs[ATTR_MEMPOOL_TOTAL_FEE_CALC] = self.mempool_total_fee_calc(
                    child_sensor.unit_of_measurement if child_sensor is not None else None
                )

            if child_sensor is None or child_sensor.attribute_key == ATTR_MEMPOOL_AVERAGE_FEE_PER_TX:
                output_attrs[ATTR_MEMPOOL_AVERAGE_FEE_PER_TX] = self.mempool_average_fee_per_tx

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_NEXT_BLOCK:

            if child_sensor is None or child_sensor.attribute_key == ATTR_MEMPOOL_NEXT_BLOCK_SIZE_CALC:
                output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_SIZE_CALC] = self.mempool_next_block_size_calc(
                    child_sensor.unit_of_measurement if child_sensor is not None else None
                )

            if child_sensor is None or child_sensor.attribute_key == ATTR_MEMPOOL_NEXT_BLOCK_TOTAL_FEE_CALC:
                output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_TOTAL_FEE_CALC] = self.mempool_next_block_total_fee_calc(
                    child_sensor.unit_of_measurement if child_sensor is not None else None
                )

            if child_sensor is None or child_sensor.attribute_key == ATTR_MEMPOOL_NEXT_BLOCK_FEE_RANGE_COMBINED:
                output_attrs[ATTR_MEMPOOL_NEXT_BLOCK_FEE_RANGE_COMBINED] = self.mempool_next_block_fee_range_combined(
                    child_sensor.unit_of_measurement if child_sensor is not None else None
                )

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.PRICE_MAIN:

            if child_sensor is None or child_sensor.attribute_key == ATTR_ALL_TIME_HIGH_DISTANCE:
                output_attrs[ATTR_ALL_TIME_HIGH_DISTANCE] = self.all_time_high_distance

            if child_sensor is None or child_sensor.attribute_key == ATTR_ALL_TIME_HIGH_DAYS:
                output_attrs[ATTR_ALL_TIME_HIGH_DAYS] = self.all_time_high_days

            if child_sensor is None or child_sensor.attribute_key == ATTR_ALL_TIME_LOW_DAYS:
                output_attrs[ATTR_ALL_TIME_LOW_DAYS] = self.all_time_low_days

        if full_attr_force or self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_CONTROL:

            if child_sensor is None or child_sensor.attribute_key == ATTR_POOL_CONTROL_1000B_PERC:
                output_attrs[ATTR_POOL_CONTROL_1000B_PERC] = self.pool_control_1000b_perc

        return output_attrs

    @property
    def all_extra_sensor_keys(self):
        return self.get_extra_sensor_attrs(full_attr_force=True).keys()

    @classmethod
    def get_valid_extra_sensor_keys(cls):
        empty_sensor = CryptoinfoAdvSensor(None, *["" for x in range(6)])
        keys = list(empty_sensor.all_extra_sensor_keys)
        del empty_sensor

        return keys

    @property
    def extra_sensor_attributes(self):
        return self.get_extra_sensor_attrs()

    @property
    def valid_attribute_keys(self):
        base_keys = list(self.extra_sensor_attributes.keys())

        return base_keys[:]

    def _build_name(self):
        if self._fetch_type not in CryptoInfoAdvEntityManager.instance().fetch_price_types:
            return (
                SENSOR_PREFIX
                + (self._internal_id_name if len(self._internal_id_name) > 0 else (
                    "{0} {1}".format(self.cryptocurrency_friendly_name, self._fetch_type.name)
                ))
            )

        else:
            return (
                SENSOR_PREFIX
                + (self._internal_id_name if len(self._internal_id_name) > 0 else (
                    "{0} Price {1}".format(self.cryptocurrency_name.title(), self.currency_name.upper())
                ))
            )

    @property
    def pool_prefix_id(self):
        return "".join(self.pool_prefixes)

    def _build_unique_id(self):
        if self._fetch_type not in CryptoInfoAdvEntityManager.instance().fetch_price_types:
            if self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_CONTROL:
                id_slug = f"{self._fetch_type.id_slug}_{self.pool_prefix_id}"
            else:
                id_slug = f"{self._fetch_type.id_slug}"
            return "{0}{1}{2}_{3}".format(
                self.cryptocurrency_name, self.multiplier, self._update_frequency.seconds, id_slug,
            )

        else:
            return "{0}{1}{2}{3}".format(
                self.cryptocurrency_name, self.currency_name, self.multiplier, self._update_frequency.seconds
            )

    def _build_device_class(self):
        if self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_price_types:
            return SensorDeviceClass.MONETARY

        elif self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_time_types:
            return SensorDeviceClass.DURATION

        else:
            return None

    def check_valid_config(self, raise_error=True):
        if self._fetch_type == CryptoInfoAdvDataFetchType.NOMP_POOL_STATS:

            if self._api_domain_name is None:
                _LOGGER.error(f"No API domain name supplied for sensor {self.name}")

                if raise_error:
                    raise ValueError()

                return False

            if self._pool_name is None:
                _LOGGER.error(f"No pool name supplied for sensor {self.name}")

                if raise_error:
                    raise ValueError()

                return False

        if self._fetch_type in CryptoInfoAdvEntityManager.instance().fetch_mempool_types:

            if self.cryptocurrency_name.lower() not in ['btc', 'bitcoin']:
                _LOGGER.error(f"Sensor {self.name} is not BTC, mempool is only supported for BTC.")

                if raise_error:
                    raise ValueError()

                return False

        return True

    def _log_api_error(self, error, tb):
        _LOGGER.error(
            f"CryptoinfoAdvanced error fetching update for {self.name}: "
            + f"{type(error).__name__}: {error}"
        )
        _LOGGER.error(tb)

    async def _async_api_fetch(self, api_data, url, extract_data, extract_primary, encoding="utf-8"):
        try:
            if api_data is None:
                async with async_timeout.timeout(30):
                    response = await self._session.get(url)
                    if response.status == 200:
                        resp_text = await response.text(encoding=encoding)
                        api_data = extract_data(json.loads(resp_text))
            primary_data = extract_primary(api_data)
            self.data = api_data
        except asyncio.TimeoutError:
            _LOGGER.error("Timeout fetching update for %s", self.name)
            primary_data, api_data = None, None
        except aiohttp.ClientError as err:
            _LOGGER.error(
                "Error fetching update for %s: %r", self.name, err
            )
            primary_data, api_data = None, None
        except Exception as error:
            tb = traceback.format_exc()
            self._log_api_error(error, tb)
            primary_data, api_data = None, None

        return primary_data, api_data

    def _extract_data_price_main_primary(self, api_data):
        if api_data is not None:
            return api_data["current_price"] * float(self.multiplier)
        else:
            raise ValueError()

    def _extract_data_price_main_full(self, json_data):
        return json_data[0]

    def _extract_data_price_simple_primary(self, api_data):
        if api_data is not None:
            return api_data[self.currency_name] * float(self.multiplier)
        else:
            raise ValueError()

    def _extract_data_price_simple_full(self, json_data):
        return json_data[self.cryptocurrency_name]

    def _extract_data_dominance_primary(self, api_data):
        return float(api_data["market_cap_percentage"][self.cryptocurrency_name])

    def _extract_data_dominance_full(self, json_data):
        return json_data["data"]

    def _extract_data_chain_summary_primary(self, api_data):
        return api_data[self.cryptocurrency_name]["height"]

    def _extract_data_chain_summary_full(self, json_data):
        return json_data

    def _extract_data_chain_control_primary(self, api_data):
        return True

    def _extract_data_chain_control_special(self, json_data, ignore_not_found=True):
        pool_data = None
        data_100_blk = 0
        data_1000_blk = 0
        for pool_prefix in self.pool_prefixes:
            if pool_prefix is not None and len(pool_prefix):
                for pool in json_data["pools"]:
                    if pool["name"].lower().startswith(pool_prefix.lower()):
                        pool_data = pool
                        data_100_blk += pool["nb100"]
                        data_1000_blk += pool["nb1000"]

        if ignore_not_found or pool_data is not None:
            return {
                **(pool_data if pool_data is not None else {}),
                "nb100": data_100_blk,
                "nb1000": data_1000_blk,
            }

        return None

    def _extract_data_chain_control_full(self, json_data):
        if self._extract_data_chain_control_special(json_data, ignore_not_found=False) is None:
            _LOGGER.debug(f"Pool Prefixes {self.pool_prefixes} not found")

        return json_data

    def _extract_data_chain_orphans_primary(self, api_data):
        orphans_start_timestamp = api_data["d"] * DAY_SECONDS
        last_orphan_timestamp = (len(api_data["n"]) * DAY_SECONDS) + orphans_start_timestamp
        last_orphan_date = datetime.fromtimestamp(last_orphan_timestamp).date()
        today_date = datetime.now().date()
        orphans_today = api_data["n"][-1] if today_date == last_orphan_date else 0
        return orphans_today

    def _extract_data_chain_orphans_full(self, json_data):
        return json_data

    def _extract_data_chain_block_time_primary(self, api_data):
        return int(api_data)

    def _extract_data_chain_block_time_full(self, json_data):
        return json_data

    def _extract_data_nomp_pool_stats_full(self, json_data):
        pool_data = {
            **json_data["pools"][self._pool_name],
            **json_data["pools"][self._pool_name]["poolStats"],
            "blocks_pending": json_data["pools"][self._pool_name]["blocks"]["pending"],
            "blocks_confirmed": json_data["pools"][self._pool_name]["blocks"]["confirmed"],
            "blocks_orphaned": json_data["pools"][self._pool_name]["blocks"]["orphaned"],
        }

        for k in ["blocks", "workers", "poolFees", "poolStats"]:
            del pool_data[k]

        return pool_data

    def _extract_data_nomp_pool_stats_primary(self, api_data):
        return float(api_data["hashrate"])

    def _extract_data_mempool_stats_full(self, json_data):
        return json_data

    def _extract_data_mempool_stats_primary(self, api_data):
        return int(api_data["vsize"])

    def _extract_data_mempool_fees_full(self, json_data):
        return json_data

    def _extract_data_mempool_fees_primary(self, api_data):
        return int(api_data["fastestFee"])

    def _extract_data_mempool_next_block_full(self, json_data):
        return json_data

    def _extract_data_mempool_next_block_primary(self, api_data):
        return int(api_data[0]["nTx"])

    def _extract_data_mempool_next_block_special(self, json_data):
        if isinstance(json_data, list) and len(json_data) >= 2:
            return list([json_data[0]["feeRange"][0], json_data[0]["feeRange"][-1]])

        return None

    async def _fetch_price_data_main(self, api_data=None):
        if not self._fetch_type == CryptoInfoAdvDataFetchType.PRICE_MAIN:
            raise ValueError()

        price_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_PRICE_MAIN.format(API_BASE_URL_COINGECKO, self.cryptocurrency_name, self.currency_name),
            self._extract_data_price_main_full, self._extract_data_price_main_primary
        )

        if price_data is not None:
            self._update_all_properties(
                state=float(price_data),
                base_price=api_data["current_price"],
                volume_24h=api_data["total_volume"],
                change_1h=api_data["price_change_percentage_1h_in_currency"],
                change_24h=api_data["price_change_percentage_24h_in_currency"],
                change_7d=api_data["price_change_percentage_7d_in_currency"],
                change_30d=api_data["price_change_percentage_30d_in_currency"],
                market_cap=api_data["market_cap"],
                circulating_supply=api_data["circulating_supply"],
                total_supply=api_data["total_supply"],
                all_time_high=api_data["ath"],
                all_time_low=api_data["atl"],
                low_24h=api_data["low_24h"],
                high_24h=api_data["high_24h"],
                image_url=api_data["image"],
                ath_date=api_data.get("ath_date"),
                atl_date=api_data.get("atl_date"),
            )
        else:
            raise ValueError()

        return self.data

    async def _fetch_price_data_alternate(self, api_data=None):
        if self._fetch_type not in CryptoInfoAdvEntityManager.instance().fetch_price_types:
            raise ValueError()

        price_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_PRICE_ALT.format(API_BASE_URL_COINGECKO, self.cryptocurrency_name, self.currency_name),
            self._extract_data_price_simple_full, self._extract_data_price_simple_primary
        )

        if price_data is not None:
            self._update_all_properties(
                state=float(price_data),
                base_price=api_data[self.currency_name],
                volume_24h=api_data[self.currency_name + "_24h_vol"],
                change_24h=api_data[self.currency_name + "_24h_change"],
                market_cap=api_data[self.currency_name + "_market_cap"]
            )
        else:
            raise ValueError()

        return self.data

    async def _fetch_dominance(self, api_data=None):
        dominance_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_DOMINANCE.format(API_BASE_URL_COINGECKO),
            self._extract_data_dominance_full,
            self._extract_data_dominance_primary
        )

        if dominance_data is not None:
            self._update_all_properties(
                state=round(dominance_data, 2),
                market_cap=api_data["total_market_cap"][self.cryptocurrency_name]
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_chain_summary(self, api_data=None):
        summary_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_CHAIN_SUMMARY.format(API_BASE_URL_CRYPTOID),
            self._extract_data_chain_summary_full,
            self._extract_data_chain_summary_primary,
            encoding="latin-1"
        )

        if summary_data is not None:
            self._update_all_properties(
                state=int(summary_data),
                difficulty=api_data[self.cryptocurrency_name]["diff"],
                circulating_supply=api_data[self.cryptocurrency_name]["supply"],
                hashrate=api_data[self.cryptocurrency_name]["hashrate"],
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_chain_control(self, api_data=None):
        if len(self.pool_prefixes) == 1 and PROPERTY_POOL_CONTROL_REMAINING in self.pool_prefixes:
            (remaining_control_100, remaining_control_1000) = CryptoInfoAdvEntityManager.instance(
            ).get_remaining_hash_control(self.cryptocurrency_name)

            self._update_all_properties(
                state=remaining_control_100,
                pool_control_1000b=remaining_control_1000,
            )

            return self.data

        control_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_CHAIN_CONTROL.format(API_BASE_URL_CRYPTOID, self.cryptocurrency_name),
            self._extract_data_chain_control_full,
            self._extract_data_chain_control_primary,
            encoding="latin-1"
        )

        if control_data is not None and api_data is not None:
            pool_data = self._extract_data_chain_control_special(api_data)

            if pool_data is None:
                raise ValueError()

            self._update_all_properties(
                state=int(pool_data["nb100"]),
                pool_control_1000b=pool_data["nb1000"],
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_chain_orphans(self, api_data=None):
        orphans_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_CHAIN_ORPHANS.format(API_BASE_URL_CRYPTOID, self.cryptocurrency_name),
            self._extract_data_chain_orphans_full,
            self._extract_data_chain_orphans_primary,
            encoding="latin-1"
        )

        if orphans_data is not None:
            self._update_all_properties(
                state=int(orphans_data),
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_chain_block_time(self, api_data=None):
        (block_height_arg, ) = self._get_fetch_args()

        try:
            block_height = int(block_height_arg)
        except Exception:
            block_height = CryptoInfoAdvEntityManager.instance().get_last_diff(self.cryptocurrency_name)

            if block_height_arg is not None:
                _LOGGER.warning("Error fetching " + self.name + " - Invalid block height arg supplied.")
            elif block_height is None:
                _LOGGER.warning("Error fetching " + self.name + " - No data from Chain Summary sensor.")

        if block_height is None:
            raise ValueError()

        if self._state is not None and self._state > 0 and self._block_height == block_height:
            api_data = self._state

        block_time_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_CHAIN_BLOCK_TIME.format(API_BASE_URL_CRYPTOID, self.cryptocurrency_name, block_height),
            self._extract_data_chain_block_time_full,
            self._extract_data_chain_block_time_primary,
            encoding="latin-1"
        )

        if block_time_data is not None:
            self._update_all_properties(
                state=int(block_time_data),
                block_height=block_height,
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_nomp_pool_stats(self, api_data=None):
        self.check_valid_config()

        hashrate_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_NOMP_POOL_STATS.format(self._api_domain_name),
            self._extract_data_nomp_pool_stats_full,
            self._extract_data_nomp_pool_stats_primary
        )

        if hashrate_data is not None:
            self._update_all_properties(
                state=float(hashrate_data),
                hashrate=float(hashrate_data),
                block_height=int(api_data["height"]),
                worker_count=int(api_data["workerCount"]),
                last_block=int(api_data["lastBlock"]),
                blocks_pending=int(api_data["blocks_pending"]),
                blocks_confirmed=int(api_data["blocks_confirmed"]),
                blocks_orphaned=int(api_data["blocks_orphaned"]),
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_mempool_stats(self, api_data=None):
        self.check_valid_config()

        mempool_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_MEMPOOL_STATS.format(API_BASE_URL_MEMPOOLSPACE),
            self._extract_data_mempool_stats_full,
            self._extract_data_mempool_stats_primary
        )

        if mempool_data is not None:
            self._update_all_properties(
                state=int(mempool_data),
                mempool_tx_count=int(api_data["count"]),
                mempool_total_fee=int(api_data["total_fee"]),
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_mempool_fees(self, api_data=None):
        self.check_valid_config()

        mempool_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_MEMPOOL_FEES.format(API_BASE_URL_MEMPOOLSPACE),
            self._extract_data_mempool_fees_full,
            self._extract_data_mempool_fees_primary
        )

        if mempool_data is not None:
            self._update_all_properties(
                state=int(mempool_data),
                mempool_fees_fastest=int(api_data["fastestFee"]),
                mempool_fees_30min=int(api_data["halfHourFee"]),
                mempool_fees_60min=int(api_data["hourFee"]),
                mempool_fees_eco=int(api_data["economyFee"]),
                mempool_fees_minimum=int(api_data["minimumFee"]),
            )

        else:
            raise ValueError()

        return self.data

    async def _fetch_mempool_next_block(self, api_data=None):
        self.check_valid_config()

        mempool_data, api_data = await self._async_api_fetch(
            api_data,
            API_ENDPOINT_MEMPOOL_NEXT_BLOCKS.format(API_BASE_URL_MEMPOOLSPACE),
            self._extract_data_mempool_next_block_full,
            self._extract_data_mempool_next_block_primary
        )

        if mempool_data is not None:
            fee_ranges = self._extract_data_mempool_next_block_special(api_data)

            if fee_ranges is None:
                raise ValueError()

            self._update_all_properties(
                state=int(mempool_data),
                mempool_next_block_size=int(api_data[0]["blockSize"]),
                mempool_next_block_tx_count=int(api_data[0]["nTx"]),
                mempool_next_block_total_fee=int(api_data[0]["totalFees"]),
                mempool_next_block_median_fee=int(api_data[0]["medianFee"]),
                mempool_next_block_fee_range_min=int(fee_ranges[0]),
                mempool_next_block_fee_range_max=int(fee_ranges[1]),
            )

        else:
            raise ValueError()

        return self.data

    def _render_fetch_args(self):
        if self._fetch_args is None:
            return None

        args = self._fetch_args

        if "{" not in args:
            return args

        else:
            args_compiled = Template(args, self.hass)

        if args_compiled:
            try:
                args_to_render = {"arguments": args}
                rendered_args = args_compiled.async_render(args_to_render)
            except TemplateError as ex:
                _LOGGER.exception("Error rendering args template: %s", ex)
                return

        else:
            rendered_args = None

        if rendered_args == args:
            # No template used. default behavior
            pass

        else:
            # Template used. Construct the string used in the shell
            args = f"{rendered_args}"

        return args

    def _get_fetch_args(self, min_length=1, expected_length=1, default_value=None):
        rendered_args = self._render_fetch_args()

        if rendered_args is None or not len(rendered_args):
            return (None for x in range(expected_length))

        split_args = rendered_args.split(" ")
        args_len = len(split_args)

        if not args_len >= min_length:
            return (None for x in range(expected_length))

        if args_len < expected_length:
            split_args.extend([default_value for x in range(expected_length - args_len)])

        return (arg.strip() for arg in split_args[:expected_length])

    def _update_all_properties(
        self,
        state=None,
        base_price=None,
        volume_24h=None,
        change_1h=None,
        change_24h=None,
        change_7d=None,
        change_30d=None,
        market_cap=None,
        circulating_supply=None,
        total_supply=None,
        all_time_high=None,
        all_time_low=None,
        low_24h=None,
        high_24h=None,
        image_url=None,
        ath_date=None,
        atl_date=None,
        difficulty=None,
        hashrate=None,
        pool_control_1000b=None,
        block_height=None,
        worker_count=None,
        last_block=None,
        blocks_pending=None,
        blocks_confirmed=None,
        blocks_orphaned=None,
        mempool_tx_count=None,
        mempool_total_fee=None,
        mempool_fees_fastest=None,
        mempool_fees_30min=None,
        mempool_fees_60min=None,
        mempool_fees_eco=None,
        mempool_fees_minimum=None,
        mempool_next_block_size=None,
        mempool_next_block_tx_count=None,
        mempool_next_block_total_fee=None,
        mempool_next_block_median_fee=None,
        mempool_next_block_fee_range_min=None,
        mempool_next_block_fee_range_max=None,
        available=True,
    ):
        if available:
            self._fetch_failure_count = 0
        self._state = state
        self._last_update = datetime.today().strftime("%d-%m-%Y %H:%M")
        self._base_price = base_price
        self._24h_volume = volume_24h
        self._1h_change = change_1h
        self._24h_change = change_24h
        self._7d_change = change_7d
        self._30d_change = change_30d
        self._market_cap = market_cap
        self._circulating_supply = circulating_supply
        self._total_supply = total_supply
        self._all_time_high = all_time_high
        self._all_time_low = all_time_low
        self._24h_low = low_24h
        self._24h_high = high_24h
        self._image_url = image_url
        self._difficulty = difficulty
        self._hashrate = hashrate
        self._pool_control_1000b = pool_control_1000b
        self._block_height = block_height
        self._worker_count = worker_count
        self._last_block = last_block
        self._blocks_pending = blocks_pending
        self._blocks_confirmed = blocks_confirmed
        self._blocks_orphaned = blocks_orphaned
        self._mempool_tx_count = mempool_tx_count
        self._mempool_total_fee = mempool_total_fee
        self._mempool_fees_fastest = mempool_fees_fastest
        self._mempool_fees_30min = mempool_fees_30min
        self._mempool_fees_60min = mempool_fees_60min
        self._mempool_fees_eco = mempool_fees_eco
        self._mempool_fees_minimum = mempool_fees_minimum
        self._mempool_next_block_size = mempool_next_block_size
        self._mempool_next_block_tx_count = mempool_next_block_tx_count
        self._mempool_next_block_total_fee = mempool_next_block_total_fee
        self._mempool_next_block_median_fee = mempool_next_block_median_fee
        self._mempool_next_block_fee_range_min = mempool_next_block_fee_range_min
        self._mempool_next_block_fee_range_max = mempool_next_block_fee_range_max
        self._attr_available = available
        if ath_date and len(ath_date) > 0:
            try:
                self._ath_date = dtparser.parse(ath_date)
            except Exception:
                self._ath_date = None
        if atl_date and len(atl_date) > 0:
            try:
                self._atl_date = dtparser.parse(atl_date)
            except Exception:
                self._atl_date = None

        self._update_child_sensors()

    def get_child_data(self, child_sensor):
        child_data = self.get_extra_sensor_attrs(
            child_sensor=child_sensor
        )

        return child_data.get(child_sensor.attribute_key)

    def _update_child_sensors(self):
        if not len(self._child_sensors) > 0:
            return

        for sensor in self._child_sensors:
            sensor._update()

    def init_child_sensors(self):
        child_sensors = list()

        if self._child_sensor_config is None or not len(self._child_sensor_config):
            return child_sensors

        valid_child_conf = list([
            conf for conf in self._child_sensor_config
            if conf[CONF_EXTRA_SENSOR_PROPERTY] in self.valid_attribute_keys
        ])

        for conf in valid_child_conf:
            id_name = conf.get(CONF_ID)
            unique_id = conf.get(CONF_UNIQUE_ID)
            state_class = conf.get(CONF_STATE_CLASS)
            attribute_key = conf.get(CONF_EXTRA_SENSOR_PROPERTY)
            unit_of_measurement = conf.get(CONF_UNIT_OF_MEASUREMENT)
            child_sensors.append(
                CryptoinfoAdvChildSensor(
                    self,
                    id_name,
                    unique_id,
                    state_class,
                    attribute_key,
                    unit_of_measurement,
                )
            )

        self._child_sensors = child_sensors

        return child_sensors

    def _process_failed_fetch(self):
        self._fetch_failure_count = self._fetch_failure_count + 1

        if self._fetch_failure_count >= self._max_fetch_failures:
            self._update_all_properties(available=False)

    async def _async_update(self):
        api_data = None

        if not CryptoInfoAdvEntityManager.instance().should_fetch_entity(self):
            api_data = CryptoInfoAdvEntityManager.instance().fetch_cached_entity_data(self)

        try:
            if self._fetch_type == CryptoInfoAdvDataFetchType.DOMINANCE:
                api_data = await self._fetch_dominance(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_SUMMARY:
                api_data = await self._fetch_chain_summary(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_CONTROL:
                api_data = await self._fetch_chain_control(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_ORPHANS:
                api_data = await self._fetch_chain_orphans(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.CHAIN_BLOCK_TIME:
                api_data = await self._fetch_chain_block_time(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.NOMP_POOL_STATS:
                api_data = await self._fetch_nomp_pool_stats(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_STATS:
                api_data = await self._fetch_mempool_stats(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_FEES:
                api_data = await self._fetch_mempool_fees(api_data)

            elif self._fetch_type == CryptoInfoAdvDataFetchType.MEMPOOL_NEXT_BLOCK:
                api_data = await self._fetch_mempool_next_block(api_data)

            else:
                api_data = await self._fetch_price_data_main(api_data)

        except ValueError:
            try:
                api_data = await self._fetch_price_data_alternate(api_data)
            except ValueError:
                self._process_failed_fetch()
                return

        CryptoInfoAdvEntityManager.instance().set_cached_entity_data(self, api_data)


class CryptoinfoAdvChildSensor(CryptoinfoAdvSensor):
    def __init__(
        self,
        parent_sensor,
        id_name,
        unique_id,
        state_class,
        attribute_key,
        unit_of_measurement,
        *args,
        **kwargs
    ):
        super().__init__(
            hass=parent_sensor.hass,
            cryptocurrency_name=parent_sensor.cryptocurrency_name,
            currency_name=parent_sensor.currency_name,
            unit_of_measurement=unit_of_measurement,
            multiplier=parent_sensor.multiplier,
            update_frequency=parent_sensor._update_frequency,
            id_name=id_name,
            unique_id=unique_id,
            state_class=state_class,
            api_mode=CryptoInfoAdvEntityManager.instance().get_extra_sensor_fetch_type_from_str(parent_sensor, attribute_key),
            pool_prefix=parent_sensor.pool_prefixes,
            fetch_args=parent_sensor._fetch_args,
            extra_sensors="",
            api_domain_name="",
            pool_name=parent_sensor._pool_name,
            max_fetch_failures=parent_sensor._max_fetch_failures,
            is_child_sensor=True,
        )

        self._parent_sensor = parent_sensor
        self._attribute_key = attribute_key

    @property
    def attribute_key(self):
        return self._attribute_key

    async def _async_update(self):
        self._update()

    def _update(self):
        new_state = self._parent_sensor.get_child_data(self)

        if new_state is not None and new_state != self._state:
            self._update_all_properties(state=new_state)

        elif new_state is None:
            self._process_failed_fetch()
