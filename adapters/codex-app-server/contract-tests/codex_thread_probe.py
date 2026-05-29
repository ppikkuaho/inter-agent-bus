"""Deterministic probe doubles for the codex app-server adapter contract tests."""

from __future__ import annotations

from typing import Any


class BusPolicyError(RuntimeError):
    pass


class ProbeRecorder:
    def __init__(
        self,
        *,
        raise_policy_unknown: bool = False,
        raise_terminal_input: bool = True,
    ) -> None:
        self.raise_policy_unknown = raise_policy_unknown
        self.raise_terminal_input = raise_terminal_input
        self.calls: list[dict[str, Any]] = []
        self.terminal_calls: list[dict[str, Any]] = []
        self._session_threads: dict[str, dict[str, Any]] = {}
        self._session_terminals: dict[str, dict[str, Any]] = {}

    def set_session_thread(self, runtime_session_id: str, record: dict[str, Any] | None) -> None:
        if record is None:
            self._session_threads.pop(runtime_session_id, None)
            return
        self._session_threads[runtime_session_id] = dict(record)

    def set_session_terminal(self, runtime_session_id: str, record: dict[str, Any] | None) -> None:
        if record is None:
            self._session_terminals.pop(runtime_session_id, None)
            return
        self._session_terminals[runtime_session_id] = dict(record)

    def load_session_thread(self, runtime_session_id: str | None) -> dict[str, Any] | None:
        if runtime_session_id is None:
            return None
        record = self._session_threads.get(runtime_session_id)
        return dict(record) if isinstance(record, dict) else None

    def load_session_terminal(self, runtime_session_id: str | None) -> dict[str, Any] | None:
        if runtime_session_id is None:
            return None
        record = self._session_terminals.get(runtime_session_id)
        return dict(record) if isinstance(record, dict) else None

    def send_synthetic_turn_for_bus(
        self,
        *,
        thread_id: str,
        text: str,
        runtime_session_id: str,
        wait_timeout: float,
        source: str = "bus",
        wait_for_completion: bool = True,
    ) -> dict[str, Any]:
        self.calls.append({
            "thread_id": thread_id,
            "text": text,
            "runtime_session_id": runtime_session_id,
            "wait_timeout": wait_timeout,
            "source": source,
            "wait_for_completion": wait_for_completion,
        })
        if self.raise_policy_unknown:
            raise BusPolicyError("policy_unknown — stubbed")
        return {"status": "ok", "turn_id": "probe_stub"}

    def send_terminal_turn(
        self,
        *,
        text: str,
        source: str,
        runtime_session_id: str | None = None,
    ) -> dict[str, Any]:
        self.terminal_calls.append({
            "text": text,
            "source": source,
            "runtime_session_id": runtime_session_id,
        })
        if self.raise_terminal_input:
            raise RuntimeError("terminal_input_unavailable — stubbed")
        return {
            "status": "ok",
            "transport": "terminal-input",
            "runtime_session_id": runtime_session_id,
            "submitted_text": text,
        }


DEFAULT_PROBE = ProbeRecorder()


def load_session_thread(runtime_session_id: str | None) -> dict[str, Any] | None:
    return DEFAULT_PROBE.load_session_thread(runtime_session_id)


def load_session_terminal(runtime_session_id: str | None) -> dict[str, Any] | None:
    return DEFAULT_PROBE.load_session_terminal(runtime_session_id)


def send_synthetic_turn_for_bus(
    *,
    thread_id: str,
    text: str,
    runtime_session_id: str,
    wait_timeout: float,
    source: str = "bus",
    wait_for_completion: bool = True,
) -> dict[str, Any]:
    return DEFAULT_PROBE.send_synthetic_turn_for_bus(
        thread_id=thread_id,
        text=text,
        runtime_session_id=runtime_session_id,
        wait_timeout=wait_timeout,
        source=source,
        wait_for_completion=wait_for_completion,
    )


def send_terminal_turn(
    *,
    text: str,
    source: str,
    runtime_session_id: str | None = None,
) -> dict[str, Any]:
    return DEFAULT_PROBE.send_terminal_turn(
        text=text,
        source=source,
        runtime_session_id=runtime_session_id,
    )
