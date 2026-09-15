"""`config.enable_presence_heartbeat` on the asyncio subscription manager.

The flag is False by default. `NativeSubscriptionManager.reconnect` checks it
before registering the presence heartbeat timer;
`AsyncioSubscriptionManager.reconnect` must do the same, otherwise every
`PubNubAsyncio` application issues presence transactions it did not ask for and
receives their failures as error statuses.

Both clients are driven through stubbed endpoints so no network traffic is
involved.
"""

import asyncio

import pytest
import pytest_asyncio

import pubnub.pubnub as pubnub_native
import pubnub.pubnub_asyncio as pubnub_asyncio
from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNHeartbeatNotificationOptions, PNStatusCategory
from pubnub.errors import PNERR_CLIENT_TIMEOUT, PNERR_REQUEST_CANCELLED
from pubnub.exceptions import PubNubException
from pubnub.models.consumer.common import PNStatus
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub
from pubnub.pubnub_asyncio import (
    AsyncioEnvelope,
    PubNubAsyncio,
    PubNubAsyncioException,
)

CHANNEL = "test-channel"

# Presence timeout and heartbeat interval, shortened from the defaults of 300
# and 280 so a test can observe several fires.
PRESENCE_TIMEOUT = 20
HEARTBEAT_INTERVAL = 0.1

# Long enough for at least one heartbeat fire at the interval above.
HEARTBEAT_WINDOW = 0.35

# Time for the subscribe loop to get as far as its first request.
SETTLE = 0.05


def make_config(enable_presence_heartbeat=False):
    config = PNConfiguration()
    config.subscribe_key = "test-sub-key"
    config.publish_key = "test-pub-key"
    config.uuid = "test-uuid"
    config.enable_presence_heartbeat = enable_presence_heartbeat
    config.set_presence_timeout_with_custom_interval(
        PRESENCE_TIMEOUT, HEARTBEAT_INTERVAL
    )
    return config


class StatusRecorder(SubscribeCallback):
    def __init__(self):
        self.categories = []

    def status(self, pubnub, status):
        self.categories.append(status.category)

    def message(self, pubnub, message):
        pass

    def presence(self, pubnub, presence):
        pass


class ParkedSubscribeStub:
    """A subscribe long poll that never answers.

    Keeps the subscribe loop out of the way: the subscription state is
    populated by `adapt_subscribe_builder`, which is all the heartbeat needs.
    """

    def __init__(self, pubnub):
        self._pubnub = pubnub

    def channels(self, channels):
        return self

    def channel_groups(self, groups):
        return self

    def timetoken(self, timetoken):
        return self

    def region(self, region):
        return self

    def filter_expression(self, expression):
        return self

    async def future(self):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            status = PNStatus()
            status.error = True
            status.category = PNStatusCategory.PNCancelledCategory
            status.error_data = PubNubException(pn_error=PNERR_REQUEST_CANCELLED)
            return PubNubAsyncioException(result=None, status=status)

    def pn_async(self, callback):
        return ParkedNativeCall()


class ParkedNativeCall:
    def __init__(self):
        self.is_executed = False
        self.is_canceled = False

    def cancel(self):
        self.is_canceled = True


def heartbeat_success():
    status = PNStatus()
    status.error = False
    status.category = PNStatusCategory.PNAcknowledgmentCategory
    return AsyncioEnvelope(result=None, status=status)


def heartbeat_timeout():
    status = PNStatus()
    status.error = True
    status.category = PNStatusCategory.PNTimeoutCategory
    status.error_data = PubNubException(pn_error=PNERR_CLIENT_TIMEOUT)
    return PubNubAsyncioException(result=None, status=status)


class HeartbeatStub:
    """Counts presence heartbeat requests instead of sending them."""

    calls = 0
    response = staticmethod(heartbeat_success)

    def __init__(self, pubnub):
        self._pubnub = pubnub

    def channels(self, channels):
        return self

    def channel_groups(self, groups):
        return self

    def state(self, state):
        return self

    def cancellation_event(self, event):
        return self

    async def future(self):
        type(self).calls += 1
        return type(self).response()

    def pn_async(self, callback):
        type(self).calls += 1
        envelope = type(self).response()
        callback(envelope.result, envelope.status)
        return ParkedNativeCall()


@pytest.fixture
def stub_endpoints(monkeypatch):
    HeartbeatStub.calls = 0
    HeartbeatStub.response = staticmethod(heartbeat_success)
    monkeypatch.setattr(pubnub_asyncio, "Subscribe", ParkedSubscribeStub)
    monkeypatch.setattr(pubnub_asyncio, "Heartbeat", HeartbeatStub)
    monkeypatch.setattr(pubnub_native, "Subscribe", ParkedSubscribeStub)
    monkeypatch.setattr(pubnub_native, "Heartbeat", HeartbeatStub)
    return HeartbeatStub


@pytest_asyncio.fixture
async def async_client_factory(stub_endpoints):
    clients = []

    def build(enable_presence_heartbeat=False):
        pubnub = PubNubAsyncio(make_config(enable_presence_heartbeat))
        recorder = StatusRecorder()
        pubnub.add_listener(recorder)
        clients.append(pubnub)
        return pubnub, recorder

    try:
        yield build
    finally:
        for pubnub in clients:
            await pubnub.stop()


# --- the timer is only registered when the flag is set ------------------


@pytest.mark.asyncio
async def test_subscribe_registers_no_heartbeat_timer_by_default(
    async_client_factory,
):
    pubnub, _ = async_client_factory()
    assert pubnub.config.enable_presence_heartbeat is False

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(SETTLE)

    assert pubnub._subscription_manager._heartbeat_periodic_callback is None


@pytest.mark.asyncio
async def test_subscribe_registers_heartbeat_timer_when_enabled(
    async_client_factory,
):
    pubnub, _ = async_client_factory(enable_presence_heartbeat=True)

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(SETTLE)

    callback = pubnub._subscription_manager._heartbeat_periodic_callback
    assert callback is not None
    assert callback._running is True


@pytest.mark.asyncio
async def test_reconnect_registers_no_heartbeat_timer_by_default(
    async_client_factory,
):
    """`reconnect()` is reached from unsubscribe and set-state too."""
    pubnub, _ = async_client_factory()

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(SETTLE)
    pubnub.reconnect()
    await asyncio.sleep(SETTLE)

    assert pubnub._subscription_manager._heartbeat_periodic_callback is None


# --- no presence transactions are issued --------------------------------


@pytest.mark.asyncio
async def test_no_heartbeat_requests_sent_by_default(async_client_factory):
    pubnub, _ = async_client_factory()

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(HEARTBEAT_WINDOW)

    assert HeartbeatStub.calls == 0


@pytest.mark.asyncio
async def test_heartbeat_requests_sent_when_enabled(async_client_factory):
    """The flag still turns the heartbeat on."""
    pubnub, _ = async_client_factory(enable_presence_heartbeat=True)

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(HEARTBEAT_WINDOW)

    assert HeartbeatStub.calls >= 1


def test_native_client_sends_no_heartbeat_requests_by_default(stub_endpoints):
    """The behaviour the asyncio manager is being brought in line with."""
    pubnub = PubNub(make_config())
    try:
        pubnub.subscribe().channels([CHANNEL]).execute()

        assert pubnub._subscription_manager._heartbeat_periodic_callback is None
        assert HeartbeatStub.calls == 0
    finally:
        pubnub._subscription_manager.stop()


# --- no error statuses from a disabled subsystem ------------------------


@pytest.mark.asyncio
async def test_heartbeat_failure_not_announced_by_default(async_client_factory):
    """A heartbeat timeout reached listeners as `PNTimeoutCategory`.

    With `heartbeat_notification_options` at its default of FAILURES, a failed
    heartbeat is announced verbatim. In a steady-state subscribed session the
    heartbeat is the only thing on the asyncio path that announces
    `PNTimeoutCategory`: the subscribe loop restarts silently on its own long
    poll timeouts. An application reading that as a lost connection acts on a
    failure that never happened.
    """
    HeartbeatStub.response = staticmethod(heartbeat_timeout)
    pubnub, recorder = async_client_factory()

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(HEARTBEAT_WINDOW)

    assert recorder.categories == []


@pytest.mark.asyncio
async def test_heartbeat_failure_announced_when_enabled(async_client_factory):
    """Announcement is unchanged for an application that opted in."""
    HeartbeatStub.response = staticmethod(heartbeat_timeout)
    pubnub, recorder = async_client_factory(enable_presence_heartbeat=True)
    assert (
        pubnub.config.heartbeat_notification_options
        is PNHeartbeatNotificationOptions.FAILURES
    )

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(HEARTBEAT_WINDOW)

    assert PNStatusCategory.PNTimeoutCategory in recorder.categories


@pytest.mark.asyncio
async def test_heartbeat_success_not_announced_when_enabled(async_client_factory):
    """FAILURES verbosity stays silent on success, so one timeout looks like one event."""
    pubnub, recorder = async_client_factory(enable_presence_heartbeat=True)

    pubnub.subscribe().channels([CHANNEL]).execute()
    await asyncio.sleep(HEARTBEAT_WINDOW)

    assert HeartbeatStub.calls >= 1
    assert recorder.categories == []
