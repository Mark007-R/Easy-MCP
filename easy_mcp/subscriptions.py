"""Change notifications: who is told that a list or a resource changed, and the listen frames.

Internal: users never import it.  A change to a list (registering or
removing a tool, a resource, a template or a prompt) is announced to two
kinds of recipient, each a *sink*:

* a handshake-era session, told with an untagged
  ``notifications/tools/list_changed`` (or the prompts or resources one)
  once its ``initialize`` result has been sent: on stdout over stdio, on the
  legacy ``/sse`` stream, on a Streamable HTTP session's ``GET /mcp``
  stream;
* one ``subscriptions/listen`` stream (2026-07-28), told with notifications
  tagged with the listen request's id, after its acknowledgment.

Each sink debounces on its own: the first change opens a window of
:data:`LIST_CHANGED_DEBOUNCE_SECONDS`, later changes ride along, and the
window never grows.  When it closes, the sink compares a digest of the list
its client may see with the digest of what it was last told, so a client
hears about a change only when *its* list changed: a tool it cannot see,
or one added and removed within the window, sends nothing.

A resource update (``notifications/resources/updated``) is the second topic.
It goes to the sessions subscribed to the URI (``resources/subscribe``) and
to the listen streams whose acknowledgment honored it
(``resourceSubscriptions``), matched by the exact URI.  It is not debounced,
only coalesced: an update still waiting to be written is not queued again.
An update for a session that has no sink yet (no ``GET /mcp`` stream open,
say) waits for one, when the session is kept in this process.

Changes may come from any thread.  Only :meth:`_Sink.request_flush` and
:meth:`_Sink.request_update` run off the event loop, and they touch nothing
but the pending sets, under a lock; the flushes run on the sink's own loop.
"""

from __future__ import annotations

import asyncio
import collections
import logging
import math
import threading
import uuid
import weakref
from collections.abc import Callable, Hashable, Iterable
from typing import TYPE_CHECKING, Any

from .exceptions import INVALID_PARAMS, INVALID_REQUEST, ProtocolError, SubscriptionLimitError
from .logging import audit
from .protocol import ACKNOWLEDGED_METHOD, LIST_KINDS, META_SUBSCRIPTION_ID

if TYPE_CHECKING:
    from .security.auth import ClientIdentity

# Read when a window opens, so tests can shorten it.
LIST_CHANGED_DEBOUNCE_SECONDS = 0.1

# Listen streams one client may hold open in one process.
MAX_SUBSCRIPTIONS_PER_CLIENT = 8

# Resource URIs one listen stream, or one handshake-era session, may watch.
MAX_RESOURCE_SUBSCRIPTIONS = 1000

RESOURCE_UPDATED_METHOD = "notifications/resources/updated"

# The text of the notifications/cancelled that ends a subscription the
# server tore down, by why it did.
_END_REASONS = {
    "shutdown": "server shutting down",
    "session_closed": "session closed",
    "token_expired": "access token expired",
}

logger = logging.getLogger("easy_mcp.subscriptions")

Push = Callable[[dict[str, Any]], None]
Digest = Callable[[str, "ClientIdentity | None"], str]


def valid_subscription_id(value: object) -> bool:
    """Whether *value* can name a subscription: a JSON string or a finite number."""
    if isinstance(value, bool):
        return False
    if isinstance(value, str | int):
        return True
    return isinstance(value, float) and math.isfinite(value)


def _subscription_key(value: Any) -> tuple[str, Any]:
    # By type as well as value, so 1, 1.0 and "1" name three subscriptions.
    return type(value).__name__, value


def parse_filter(value: object) -> tuple[frozenset[str], tuple[str, ...] | None]:
    """The list kinds a ``notifications`` filter asks for, and the resource URIs it names.

    The URIs are ``None`` when the filter has no ``resourceSubscriptions``,
    and are not counted here: a server without resources ignores them, and
    one with resources refuses more than :data:`MAX_RESOURCE_SUBSCRIPTIONS`
    as a limit (``-32007``).  Unknown fields are ignored (a later revision
    may add some).

    Raises:
        ProtocolError: ``-32602`` for a filter that is not an object, a flag
            that is not a boolean, or ``resourceSubscriptions`` that is not
            an array of strings.
    """
    if not isinstance(value, dict):
        raise ProtocolError(
            "Invalid params: subscriptions/listen requires a 'notifications' object",
            code=INVALID_PARAMS,
        )
    kinds: set[str] = set()
    for kind, (_, field) in LIST_KINDS.items():
        if field not in value:
            continue
        flag = value[field]
        if not isinstance(flag, bool):
            raise ProtocolError(
                f"Invalid params: notifications.{field} must be a boolean", code=INVALID_PARAMS
            )
        if flag:
            kinds.add(kind)
    if "resourceSubscriptions" not in value:
        return frozenset(kinds), None
    resources = value["resourceSubscriptions"]
    if not (isinstance(resources, list) and all(isinstance(uri, str) for uri in resources)):
        raise ProtocolError(
            "Invalid params: notifications.resourceSubscriptions must be an array of strings",
            code=INVALID_PARAMS,
        )
    return frozenset(kinds), tuple(resources)


def _digest_failed(kind: str) -> None:
    error_id = uuid.uuid4().hex[:12]
    logger.error(
        "could not compute the %s list for a change notification error_id=%s",
        kind,
        error_id,
        exc_info=True,
    )


def _tag(subscription_id: Any) -> dict[str, Any]:
    return {META_SUBSCRIPTION_ID: subscription_id}


def ack_message(
    subscription_id: Any, kinds: Iterable[str], resources: Iterable[str] | None = None
) -> dict[str, Any]:
    """``notifications/subscriptions/acknowledged`` naming what will be honored.

    *resources* are the resource URIs honored; ``None`` leaves
    ``resourceSubscriptions`` out.
    """
    honored: dict[str, Any] = {LIST_KINDS[kind][1]: True for kind in sorted(kinds)}
    if resources is not None:
        honored["resourceSubscriptions"] = list(resources)
    return {
        "jsonrpc": "2.0",
        "method": ACKNOWLEDGED_METHOD,
        "params": {"_meta": _tag(subscription_id), "notifications": honored},
    }


def resource_updated_message(uri: str, subscription_id: Any = None) -> dict[str, Any]:
    """``notifications/resources/updated`` for *uri*: tagged on a listen stream, bare otherwise."""
    params: dict[str, Any] = {"uri": uri}
    if subscription_id is not None:
        params = {"_meta": _tag(subscription_id), "uri": uri}
    return {"jsonrpc": "2.0", "method": RESOURCE_UPDATED_METHOD, "params": params}


def list_changed_message(kind: str, subscription_id: Any = None) -> dict[str, Any]:
    """The list-change notification for *kind*: tagged on a listen stream, bare otherwise."""
    message: dict[str, Any] = {"jsonrpc": "2.0", "method": LIST_KINDS[kind][0]}
    if subscription_id is not None:
        message["params"] = {"_meta": _tag(subscription_id)}
    return message


def cancelled_message(subscription_id: Any, reason: str) -> dict[str, Any]:
    """The ``notifications/cancelled`` that ends a subscription on a multiplexed channel."""
    return {
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {
            "requestId": subscription_id,
            "reason": _END_REASONS.get(reason, "subscription closed by the server"),
            "_meta": _tag(subscription_id),
        },
    }


class _Sink:
    """One recipient of list changes: a session, or one ``subscriptions/listen`` stream.

    It holds no reference to the context it serves.  Its state is touched on
    its own loop, except the pending set, which :meth:`request_flush`
    updates from any thread under the lock.
    """

    __slots__ = (
        "_finalizer",
        "_lock",
        "_notifier",
        "_pending",
        "_scheduled",
        "_timer",
        "_updates",
        "_updates_scheduled",
        "baselines",
        "channel",
        "client_id",
        "closed",
        "ended",
        "identity",
        "key",
        "kinds",
        "listen",
        "loop",
        "multiplexed",
        "push",
        "settled",
        "subscription_id",
        "uris",
    )

    def __init__(
        self,
        notifier: ChangeNotifier,
        *,
        key: Any,
        channel: Hashable,
        listen: bool,
        client_id: str,
        identity: ClientIdentity | None,
        kinds: frozenset[str],
        subscription_id: Any,
        push: Push,
        multiplexed: bool,
        loop: asyncio.AbstractEventLoop,
        baselines: dict[str, str],
        uris: frozenset[str] = frozenset(),
    ) -> None:
        self._notifier = notifier
        self.key = key
        self.channel = channel
        self.listen = listen
        self.client_id = client_id
        self.identity = identity
        self.kinds = kinds
        self.subscription_id = subscription_id
        self.push = push
        self.multiplexed = multiplexed
        self.loop = loop
        # kind -> digest of the list this recipient was last told about.
        self.baselines = baselines
        # Set when a listen stream ends, whoever ended it.
        self.ended: asyncio.Event | None = asyncio.Event() if listen else None
        # Set once a listen request needs no other answer: the server sent
        # its completion result, or its client cancelled it.
        self.settled = False
        self.closed = False
        # The resource URIs a listen stream was acknowledged for; a session's
        # are kept by the notifier, since they outlive its sinks.
        self.uris = uris
        self._finalizer: Any = None
        self._pending: set[str] = set()
        self._scheduled = False
        self._timer: asyncio.TimerHandle | None = None
        # Resource updates waiting for a flush, in order (a dict as an ordered set).
        self._updates: dict[str, None] = {}
        self._updates_scheduled = False
        self._lock = threading.Lock()

    def request_flush(self, kind: str) -> None:
        """Note that *kind* changed; any thread."""
        with self._lock:
            if self.closed:
                return
            self._pending.add(kind)
            if self._scheduled:
                return  # a window is open already: the change rides along
            self._scheduled = True
        try:
            self.loop.call_soon_threadsafe(self._arm)
        except RuntimeError:
            # Its loop has closed: nobody is left to tell.
            self._notifier.drop(self, "undeliverable")

    def _arm(self) -> None:
        if not self.closed:
            self._timer = self.loop.call_later(LIST_CHANGED_DEBOUNCE_SECONDS, self._flush)

    def _flush(self) -> None:
        with self._lock:
            kinds = sorted(self._pending)
            self._pending.clear()
            self._scheduled = False
        self._timer = None
        self.deliver(kinds)

    def deliver(self, kinds: Iterable[str]) -> None:
        """Tell the client about each of *kinds* whose list changed since it was last told.

        On the loop.  A recipient that cannot be told never fails the change
        that caused it: one whose channel is gone is dropped; one whose list
        cannot be computed any more is dropped too, except a listen stream,
        which the server ends with its final frames.
        """
        for kind in kinds:
            if self.closed:
                return
            try:
                digest = self._notifier._digest(kind, self.identity)
            except Exception:
                _digest_failed(kind)
                if self.listen:
                    self._notifier.end(self, "undeliverable")
                else:
                    self._notifier.drop(self, "undeliverable")
                return
            if self.baselines.get(kind) == digest:
                continue
            try:
                self.push(list_changed_message(kind, self.subscription_id))
            except Exception:
                # Not told: a stream that takes over from it (whose start the
                # baselines are kept for) must still announce this change.
                self._notifier.drop(self, "undeliverable")
                return
            self.baselines[kind] = digest
            logger.debug("told client %s that the %s list changed", self.client_id, kind)

    def request_update(self, uri: str) -> bool:
        """Note that the resource *uri* changed; any thread.  Whether it will be told.

        Not debounced: the update is sent on the sink's loop as soon as it
        runs.  An update for *uri* still waiting is not queued again.
        """
        with self._lock:
            if self.closed:
                return False
            if uri in self._updates:
                return True
            self._updates[uri] = None
            if self._updates_scheduled:
                return True
            self._updates_scheduled = True
        try:
            self.loop.call_soon_threadsafe(self._flush_updates)
        except RuntimeError:
            # Its loop has closed: nobody is left to tell.
            self._notifier.drop(self, "undeliverable")
            return False
        return True

    def _flush_updates(self) -> None:
        with self._lock:
            uris = list(self._updates)
            self._updates.clear()
            self._updates_scheduled = False
        for uri in uris:
            if self.closed:
                return
            if not self._notifier._watches(self, uri):
                continue  # unsubscribed meanwhile
            try:
                self.push(resource_updated_message(uri, self.subscription_id))
            except Exception:
                self._notifier.drop(self, "undeliverable")
                return
            logger.debug("told client %s that a resource changed", self.client_id)

    def _close(self) -> list[str]:
        """Stop for good: no flush runs for it from now on.  On its loop, or once it has closed.

        Returns the resource updates that were still waiting here, taken in
        the same step, so none can be queued between: a sink that replaces
        it, or the session's next one, sends them instead.
        """
        with self._lock:
            self.closed = True
            self._pending.clear()
            waiting = list(self._updates)
            self._updates.clear()
        timer, self._timer = self._timer, None
        if timer is not None:
            timer.cancel()
        finalizer, self._finalizer = self._finalizer, None
        if finalizer is not None:
            finalizer.detach()
        return waiting

    def _set_ended(self) -> None:
        ended = self.ended
        if ended is None:
            return
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if running is self.loop:
            ended.set()
            return
        try:
            self.loop.call_soon_threadsafe(ended.set)
        except RuntimeError:
            pass  # its loop has closed: nothing waits on it any more

    async def wait_ended(self) -> None:
        """Wait until the stream ends, whoever ends it."""
        assert self.ended is not None
        await self.ended.wait()


class _Subscriptions:
    """The resources one session is subscribed to, and the updates waiting for its sink.

    *hold*: updates made while the session has no sink wait for one.  Only
    for a session kept in this process: with a shared store its next
    stream may open on another worker, and the entry lives only as long as
    a sink here.
    """

    __slots__ = ("held", "hold", "uris")

    def __init__(self, hold: bool) -> None:
        self.uris: set[str] = set()
        self.held: dict[str, None] = {}
        self.hold = hold


class ChangeNotifier:
    """Every recipient of list changes in this process, and the changes they are told about.

    Args:
        digest: ``digest(kind, identity)``: a digest of the *kind* list the
            identity may see.  Called on the sinks' loops.
        view: ``view(identity)``: what decides which lists the identity
            sees; two identities with the same view see the same lists.
        final: ``final(subscription_id)``: the completion result a listen
            stream the server ends receives.
    """

    def __init__(
        self,
        digest: Digest,
        *,
        view: Callable[[ClientIdentity | None], Hashable],
        final: Callable[[Any], dict[str, Any]],
    ) -> None:
        self._digest = digest
        self._view = view
        self._final = final
        self._lock = threading.Lock()
        # Session sinks by session; listen sinks by channel, then by id.
        self._sessions: dict[Hashable, _Sink] = {}
        self._channels: dict[Hashable, dict[tuple[str, Any], _Sink]] = {}
        self._per_client: collections.Counter[str] = collections.Counter()
        self._listens = 0
        # The resource URIs each handshake-era session is subscribed to, by
        # session.  They outlive the session's sinks (a GET /mcp stream may
        # close and another open), and end with the session.
        self._subscribed: dict[Hashable, _Subscriptions] = {}

    def baselines(self, kinds: Iterable[str], identity: ClientIdentity | None) -> dict[str, str]:
        """The digest of each of *kinds* as *identity* sees it now.

        A kind whose list cannot be computed is left out, and logged: its
        recipient is not told about that list, rather than the handshake,
        stream or listen that asked failing.
        """
        digests: dict[str, str] = {}
        for kind in kinds:
            try:
                digests[kind] = self._digest(kind, identity)
            except Exception:
                _digest_failed(kind)
        return digests

    # ------------------------------------------------------------ sessions

    def watch_session(
        self,
        key: Hashable,
        *,
        push: Push,
        identity: ClientIdentity | None,
        client_id: str,
        kinds: Iterable[str],
        baselines: dict[str, str],
        multiplexed: bool = False,
        anchor: object | None = None,
        subscriptions: Iterable[str] | None = None,
        hold: bool = True,
    ) -> _Sink:
        """Tell the session *key* about list changes from now on, through *push*.

        On the loop that delivers to it.  *baselines* is what its client was
        last told the lists hold.  A sink the session had is replaced.
        With an *anchor*, the sink goes (on its loop) once the anchor is
        collected, so a transport that forgets to end the session leaks
        nothing.  It is told about updates of the resources the session is
        subscribed to as well: *subscriptions* when given (with *hold*, as
        :meth:`set_subscriptions` takes them), and first about those updated
        while the session had no sink.
        """
        kinds = frozenset(kinds)
        sink = _Sink(
            self,
            key=key,
            channel=key,
            listen=False,
            client_id=client_id,
            identity=identity,
            kinds=kinds,
            subscription_id=None,
            push=push,
            multiplexed=multiplexed,
            loop=asyncio.get_running_loop(),
            baselines={kind: digest for kind, digest in baselines.items() if kind in kinds},
        )
        if anchor is not None:
            sink._finalizer = weakref.finalize(anchor, self._anchor_collected, key, sink)
        with self._lock:
            replaced = self._sessions.get(key)
            self._sessions[key] = sink
            if subscriptions is not None:
                self._set_subscriptions(key, subscriptions, hold)
            entry = self._subscribed.get(key)
            held = list(entry.held) if entry is not None else []
            if entry is not None:
                entry.held.clear()
        # An update a publisher hands the replaced sink from now on is
        # refused there, and sent here instead (publish_resource_updated).
        carried = replaced._close() if replaced is not None else []
        for uri in dict.fromkeys([*carried, *held]):
            sink.request_update(uri)
        return sink

    def session(self, key: Hashable) -> _Sink | None:
        """The sink of the session *key*, if it has one."""
        with self._lock:
            return self._sessions.get(key)

    def refresh_identity(self, key: Hashable, identity: ClientIdentity | None) -> None:
        """Judge what the session *key* may see by *identity* from now on (its latest request's).

        When that changes which lists it sees, what its client holds is no
        longer known (it may have listed with either credential): it is
        told once to list again, at the next flush.
        """
        with self._lock:
            sink = self._sessions.get(key)
            if sink is None:
                return
            previous, sink.identity = sink.identity, identity
        if self._view(previous) == self._view(identity):
            return
        sink.baselines.clear()
        for kind in sink.kinds:
            sink.request_flush(kind)

    def _anchor_collected(self, key: Hashable, sink: _Sink) -> None:
        # Run by the collector, which may run on any thread, inside any of
        # this notifier's (or the sink's) locks held there: it takes none,
        # and leaves ending the session to the sink's loop.
        try:
            sink.loop.call_soon_threadsafe(self._session_collected, key, sink)
        except RuntimeError:
            pass  # its loop has closed: the next change drops it (request_flush)

    def _session_collected(self, key: Hashable, sink: _Sink) -> None:
        # The session's context is gone, so the session is: its
        # subscriptions go too, unless another sink took over meanwhile.
        if self.end_session(key, sink):
            self.forget_session(key)

    # ------------------------------------------------- resource subscriptions

    def _sink_gone(self, key: Hashable) -> None:
        """The session *key* has no sink here any more; with the lock held."""
        entry = self._subscribed.get(key)
        if entry is not None and not entry.hold:
            # Kept only while a sink here delivers (a shared store).
            del self._subscribed[key]

    def _set_subscriptions(self, key: Hashable, uris: Iterable[str], hold: bool) -> None:
        """:meth:`set_subscriptions`, with the lock held."""
        wanted = set(uris)
        if not wanted or (not hold and key not in self._sessions):
            self._subscribed.pop(key, None)
            return
        entry = self._subscribed.get(key)
        if entry is None:
            entry = self._subscribed[key] = _Subscriptions(hold)
        entry.hold = hold
        entry.uris = wanted
        entry.held = {uri: None for uri in entry.held if uri in wanted}

    def set_subscriptions(self, key: Hashable, uris: Iterable[str], *, hold: bool) -> None:
        """The session *key* is subscribed to exactly *uris* (as its store record says).

        With *hold*, updates made while it has no sink wait for one; without,
        nothing is kept for a session with no sink here.  Any thread.
        """
        with self._lock:
            self._set_subscriptions(key, uris, hold)

    def add_subscription(self, key: Hashable, uri: str, *, hold: bool) -> None:
        """The session *key*, whose store record keeps its subscriptions, watches *uri* too.

        As :meth:`set_subscriptions` takes the record's whole set (with
        *hold*), but for the one URI a request added: two requests answered
        out of order then never leave the older set in place.  Any thread.
        """
        with self._lock:
            if not hold and key not in self._sessions:
                return  # kept only while a sink here delivers (a shared store)
            entry = self._subscribed.get(key)
            if entry is None:
                entry = self._subscribed[key] = _Subscriptions(hold)
            entry.hold = hold
            entry.uris.add(uri)

    def subscribe(self, key: Hashable, uri: str, *, cap: int) -> bool:
        """Subscribe the session *key*, kept in this process, to *uri*; any thread.

        Returns ``False`` (and changes nothing) when it watches *cap* URIs
        already.  Idempotent.
        """
        with self._lock:
            entry = self._subscribed.get(key)
            if entry is None:
                entry = self._subscribed[key] = _Subscriptions(hold=True)
            if uri in entry.uris:
                return True
            if len(entry.uris) >= cap:
                return False
            entry.uris.add(uri)
            return True

    def unsubscribe(self, key: Hashable, uri: str) -> None:
        """Stop telling the session *key* about *uri*, even an update already waiting."""
        with self._lock:
            entry = self._subscribed.get(key)
            if entry is None:
                return
            entry.uris.discard(uri)
            entry.held.pop(uri, None)
            if not entry.uris:
                del self._subscribed[key]

    def subscriptions(self, key: Hashable) -> frozenset[str]:
        """The resource URIs the session *key* is subscribed to, as this process knows them."""
        with self._lock:
            entry = self._subscribed.get(key)
            return frozenset(entry.uris) if entry is not None else frozenset()

    def forget_session(self, key: Hashable) -> None:
        """The session *key* has ended: its subscriptions and waiting updates go."""
        with self._lock:
            self._subscribed.pop(key, None)

    def _watches(self, sink: _Sink, uri: str) -> bool:
        """Whether *sink* is still to be told about *uri*."""
        if sink.listen:
            return uri in sink.uris
        with self._lock:
            entry = self._subscribed.get(sink.key)
            return entry is not None and uri in entry.uris

    def publish_resource_updated(self, uri: str) -> int:
        """The resource *uri* changed: tell every recipient watching it.  Any thread.

        Returns how many subscriptions matched and had the update queued
        (an update for a session with no sink yet waits for one, when the
        session is kept in this process).  Delivery itself happens on each
        recipient's loop.  A session sink that closes between being picked
        here and being handed the update (its stream replaced or ended
        meanwhile, on its loop) refuses it, and it goes to the session's
        sink after it, or waits for one, instead.
        """
        count = 0
        sinks: list[_Sink] = []
        with self._lock:
            for key, entry in self._subscribed.items():
                if uri not in entry.uris:
                    continue
                sink = self._sessions.get(key)
                if sink is not None:
                    sinks.append(sink)
                elif entry.hold:
                    entry.held[uri] = None
                    count += 1
            for streams in self._channels.values():
                sinks.extend(sink for sink in streams.values() if uri in sink.uris)
        for sink in sinks:
            if sink.request_update(uri) or (not sink.listen and self._redeliver(sink.key, uri)):
                count += 1
        return count

    def end_session(self, key: Hashable, sink: _Sink | None = None) -> bool:
        """Stop telling the session *key* (or only its *sink*, if that is still its sink).

        Resource updates it had yet to write wait for the session's next
        sink, when the session is kept in this process.
        """
        with self._lock:
            current = self._sessions.get(key)
            if current is None or (sink is not None and current is not sink):
                return False
            del self._sessions[key]
            self._sink_gone(key)
        for uri in current._close():
            self._redeliver(key, uri)
        logger.debug("session sink %s ended", current.client_id)
        return True

    def _redeliver(self, key: Hashable, uri: str) -> bool:
        """Pass an update of *uri* that a closed sink of the session *key* never sent on.

        To the session's sink now, or else to the updates waiting for its
        next one (when the session is kept in this process); nowhere when
        the session no longer watches *uri*.  Whether it was queued or kept.
        Any thread.
        """
        while True:
            with self._lock:
                entry = self._subscribed.get(key)
                if entry is None or uri not in entry.uris:
                    return False
                sink = self._sessions.get(key)
                if sink is None:
                    if not entry.hold:
                        return False
                    entry.held[uri] = None
                    return True
            if sink.request_update(uri):
                return True
            # That sink closed meanwhile too, and has left _sessions: look again.

    # ------------------------------------------------------- listen streams

    def open(
        self,
        *,
        channel: Hashable,
        client_id: str,
        identity: ClientIdentity | None,
        subscription_id: Any,
        kinds: frozenset[str],
        push: Push,
        multiplexed: bool,
        max_total: int,
        uris: frozenset[str] = frozenset(),
    ) -> _Sink:
        """Register one listen stream; on its loop.

        The sink's ``kinds`` are *kinds* less any whose list cannot be
        computed (:meth:`baselines`): those it is never told about.  *uris*
        are the resources whose updates it is told about.

        Raises:
            ProtocolError: ``-32600``: *subscription_id* is open on *channel*
                already.
            SubscriptionLimitError: The client holds
                :data:`MAX_SUBSCRIPTIONS_PER_CLIENT` streams, or *max_total*
                are open in this process (audited ``subscription_refused``).
        """
        key = _subscription_key(subscription_id)
        # What the client is told about from now on is measured from here.
        baselines = self.baselines(kinds, identity)
        kinds = frozenset(baselines)
        sink = _Sink(
            self,
            key=key,
            channel=channel,
            listen=True,
            client_id=client_id,
            identity=identity,
            kinds=kinds,
            subscription_id=subscription_id,
            push=push,
            multiplexed=multiplexed,
            loop=asyncio.get_running_loop(),
            baselines=baselines,
            uris=uris,
        )
        refused: str | None = None
        with self._lock:
            streams = self._channels.get(channel)
            duplicate = streams is not None and key in streams
            if duplicate:
                pass
            elif self._per_client[client_id] >= MAX_SUBSCRIPTIONS_PER_CLIENT:
                refused = "client_limit"
            elif self._listens >= max_total:
                refused = "server_limit"
            else:
                self._channels.setdefault(channel, {})[key] = sink
                self._per_client[client_id] += 1
                self._listens += 1
        if duplicate:
            raise ProtocolError(
                f"Invalid request: subscription {subscription_id!r} is already open on this "
                "channel",
                code=INVALID_REQUEST,
            )
        if refused is not None:
            audit("subscription_refused", client_id=client_id, reason=refused)
            if refused == "client_limit":
                raise SubscriptionLimitError(
                    "Too many subscriptions/listen streams are open for this client "
                    f"(at most {MAX_SUBSCRIPTIONS_PER_CLIENT}); close one first"
                )
            raise SubscriptionLimitError(
                "Too many subscriptions/listen streams are open on this server; retry later"
            )
        return sink

    def cancel(self, channel: Hashable | None, request_id: object) -> bool:
        """End the listen stream *request_id* names on *channel*, as its client asked.

        Nothing more is written for it, not even a change already pending.
        Returns whether there was one.
        """
        if channel is None or not valid_subscription_id(request_id):
            return False
        with self._lock:
            streams = self._channels.get(channel)
            sink = streams.get(_subscription_key(request_id)) if streams is not None else None
        if sink is None or not self.drop(sink, "client_cancelled"):
            return False
        sink.settled = True  # a cancelled request gets no answer
        return True

    def drop(self, sink: _Sink, reason: str) -> bool:
        """Remove *sink* without a word to its client.  Idempotent; whether this call removed it."""
        if not self._unregister(sink):
            return False
        sink._close()
        if sink.listen:
            audit(
                "subscription_close",
                client_id=sink.client_id,
                subscription_id=sink.subscription_id,
                reason=reason,
            )
            sink._set_ended()
        else:
            logger.debug(
                "session of client %s stops receiving list changes (%s)", sink.client_id, reason
            )
        return True

    def end(self, sink: _Sink, reason: str) -> bool:
        """End a listen stream as the server: its completion result, then on a
        multiplexed channel ``notifications/cancelled``.  On its loop.

        Returns whether this call ended it.
        """
        if not self._unregister(sink):
            return False
        sink._close()
        sink.settled = True
        try:
            sink.push(self._final(sink.subscription_id))
            if sink.multiplexed:
                sink.push(cancelled_message(sink.subscription_id, reason))
        except Exception:
            logger.debug("could not send the end of subscription %r", sink.subscription_id)
        audit(
            "subscription_close",
            client_id=sink.client_id,
            subscription_id=sink.subscription_id,
            reason=reason,
        )
        sink._set_ended()
        return True

    def close(self, channel: Hashable | None, session: Hashable | None, *, reason: str) -> int:
        """End every listen stream of *channel*, and the session *session*'s notifications.

        On the loop.  Each listen stream gets its final frames (:meth:`end`)
        before this returns.  The session's resource subscriptions end too.
        Returns how many subscriptions ended.
        """
        with self._lock:
            streams = list(self._channels.get(channel, {}).values()) if channel is not None else []
        ended = sum(1 for sink in streams if self.end(sink, reason))
        if session is not None:
            if self.end_session(session):
                ended += 1
            self.forget_session(session)
        return ended

    def changed(self, kind: str) -> None:
        """The *kind* list changed: every recipient that may care checks it.  Any thread."""
        with self._lock:
            sinks = [sink for sink in self._sessions.values() if kind in sink.kinds]
            for streams in self._channels.values():
                sinks.extend(sink for sink in streams.values() if kind in sink.kinds)
        for sink in sinks:
            sink.request_flush(kind)

    def count(self) -> int:
        """How many listen streams are open."""
        with self._lock:
            return self._listens

    def _unregister(self, sink: _Sink) -> bool:
        with self._lock:
            if not sink.listen:
                if self._sessions.get(sink.key) is not sink:
                    return False
                del self._sessions[sink.key]
                self._sink_gone(sink.key)
                return True
            streams = self._channels.get(sink.channel)
            if streams is None or streams.get(sink.key) is not sink:
                return False
            del streams[sink.key]
            if not streams:
                del self._channels[sink.channel]
            self._per_client[sink.client_id] -= 1
            if self._per_client[sink.client_id] <= 0:
                del self._per_client[sink.client_id]
            self._listens -= 1
            return True
