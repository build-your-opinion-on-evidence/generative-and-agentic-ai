
"""Direct-neighbor transport and ingress result delivery for local cold tests."""

import asyncio
import json
from dataclasses import asdict, replace
from time import monotonic
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import UUID

from deals_contracts import TaskEnvelope
from deals_core import TypeState

from .runtime import AgentRuntime, ROOT


class PeerError(RuntimeError):
    pass


class PeerMesh:
    def __init__(self, agent_id):
        agents = json.loads(
            (ROOT / "configs/agents/local.json").read_text()
        )["agents"]

        self.indices = {
            a["agent_id"]: i for i, a in enumerate(agents, 1)
        }
        self.urls = {
            a["agent_id"]: f"http://127.0.0.1:{a['port']}"
            for a in agents
        }
        self.graph = json.loads(
            (ROOT / "configs/routing/graph.json").read_text()
        )

        if set(self.graph) != set(self.urls):
            raise ValueError(
                "Graph must contain exactly the configured agents"
            )

        for owner, neighbors in self.graph.items():
            if (
                not isinstance(neighbors, list)
                or owner in neighbors
                or len(neighbors) != len(set(neighbors))
                or not set(neighbors) <= set(self.urls)
            ):
                raise ValueError("Invalid direct-neighbor graph")

        self.agent_id = agent_id
        self.neighbors = self.graph[agent_id]

    def _request(self, peer, path, payload=None):
        body = None if payload is None else json.dumps(payload).encode()
        request = Request(
            self.urls[peer] + path,
            data=body,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urlopen(request, timeout=5) as response:
                return json.load(response)
        except Exception as error:
            raise PeerError(
                f"{peer}: {type(error).__name__}"
            ) from None

    async def post(self, peer, path, payload):
        # Retry the same identity; receivers deduplicate transfers.
        for attempt in range(3):
            try:
                response = await asyncio.to_thread(
                    self._request, peer, path, payload
                )
                if (
                    not isinstance(response, dict)
                    or response.get("accepted") is not True
                ):
                    raise PeerError(
                        f"{peer}: missing acknowledgement"
                    )
                return response
            except PeerError:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.2 * (attempt + 1))

    async def states(self, task, hop_limit):
        if task.hop_count >= hop_limit:
            return []

        peers = [
            peer for peer in self.neighbors
            if peer != task.previous_agent
        ]
        replies = await asyncio.gather(*(
            asyncio.to_thread(
                self._request,
                peer,
                "/internal/state/" + quote(task.task_type, safe=""),
            )
            for peer in peers
        ))

        states = []
        for peer, reply in zip(peers, replies):
            state = TypeState(**reply["state"])
            if (
                reply["run_id"] != task.run_id
                or reply["phase"] != task.phase
                or state.agent_id != peer
                or state.agent_index != self.indices[peer]
                or state.task_type != task.task_type
            ):
                raise PeerError(f"{peer}: incompatible state")
            states.append(state)

        return states


class NetworkRuntime(AgentRuntime):
    mode = "network-cold-test"

    def __init__(self, agent_id):
        super().__init__(agent_id)
        self.peers = PeerMesh(agent_id)
        self.origins = {}

    def submit(self, prompt, task_type):
        task_id = super().submit(prompt, task_type)
        self.origins[task_id] = self.state.agent_id
        self.records[task_id]["ingress_agent_id"] = self.state.agent_id
        self.records[task_id]["current_agent_id"] = self.state.agent_id
        self._save(self.records[task_id])
        return task_id

    async def neighbor_states(self, task):
        return await self.peers.states(
            task, self.config["routing"]["hop_limit"]
        )

    async def forward(self, task, decision, record):
        destination = decision["destination"]
        forwarded = replace(
            task,
            previous_agent=self.state.agent_id,
            hop_count=task.hop_count + 1,
        )
        record.update(
            status="forwarded",
            current_agent_id=destination,
        )
        payload = {
            "sender_id": self.state.agent_id,
            "origin_agent": self.origins[task.task_id],
            "task": asdict(forwarded),
            "record": json.loads(json.dumps(record)),
        }
        self._save(record)

        try:
            await self.peers.post(
                destination, "/internal/tasks", payload
            )
        except PeerError as error:
            # A lost ACK may mean the task was already accepted.
            # Preserve it for inspection instead of executing it twice.
            if (
                record["status"] == "forwarded"
                and record["current_agent_id"] == destination
            ):
                record.update(
                    status="transfer_uncertain",
                    error=str(error),
                    pending_transfer={
                        "destination": destination,
                        "payload": payload,
                    },
                )

    def receive(self, payload):
        sender = payload["sender_id"]
        origin = payload["origin_agent"]
        data = dict(payload["task"])
        incoming = payload["record"]

        data["completed_results"] = tuple(data["completed_results"])
        data["executors"] = tuple(data["executors"])
        task = TaskEnvelope(**data)

        if str(UUID(task.task_id)) != task.task_id:
            raise ValueError("Noncanonical task ID")

        if (
            sender not in self.peers.graph
            or origin not in self.peers.urls
            or self.state.agent_id not in self.peers.graph[sender]
            or task.previous_agent != sender
            or not 1 <= task.hop_count <= self.config["routing"]["hop_limit"]
            or task.split_count > self.executor.split_limit
        ):
            raise ValueError("Invalid transfer")

        expected = {
            "task_id": task.task_id,
            "run_id": task.run_id,
            "task_type": task.task_type,
            "original_prompt": task.original_prompt,
            "ingress_agent_id": origin,
        }
        for key, value in expected.items():
            if incoming.get(key) != value:
                raise ValueError("Transfer metadata mismatch")

        if self.origins.get(task.task_id, origin) != origin:
            raise ValueError("Task origin changed")

        if task.task_id not in self.records and len(self.records) >= 1000:
            raise RuntimeError("Local diagnostic task limit reached")

        if not self.state.enqueue(task):
            return {"accepted": True, "duplicate": True}

        self.origins[task.task_id] = origin
        record = self.records.setdefault(task.task_id, {})
        record.clear()
        record.update(json.loads(json.dumps(incoming)))
        record.update(
            status="queued",
            current_agent_id=self.state.agent_id,
        )

        self.submitted_at.setdefault(task.task_id, monotonic())
        self.queued_at[self._key(task)] = monotonic()
        self._save(record)
        self._launch(self.state.dispatch())

        return {"accepted": True, "duplicate": False}

    async def publish(self, record):
        origin = self.origins[record["task_id"]]

        if origin == self.state.agent_id:
            if record.get("latency_scope") != "ingress_to_result_received":
                record["latency_seconds"] = (
                    monotonic() - self.submitted_at[record["task_id"]]
                )
                record["latency_scope"] = "ingress_to_result_received"
            return

        record["latency_seconds"] = None
        record["current_agent_id"] = self.state.agent_id
        payload = {
            "sender_id": self.state.agent_id,
            "record": json.loads(json.dumps(record)),
        }

        try:
            reply = await self.peers.post(
                origin, "/internal/results", payload
            )
            record["latency_seconds"] = reply["latency_seconds"]
            record["latency_scope"] = "ingress_to_result_received"
        except PeerError as error:
            record.update(
                terminal_status=record["status"],
                status="result_delivery_pending",
                delivery_error=str(error),
                pending_result=payload,
            )

    def accept_result(self, payload):
        sender = payload["sender_id"]
        incoming = payload["record"]
        task_id = incoming["task_id"]

        if (
            sender not in self.peers.urls
            or self.origins.get(task_id) != self.state.agent_id
            or incoming["status"] not in {"completed", "failed"}
        ):
            raise ValueError("Invalid terminal result")

        record = self.records[task_id]
        for key in (
            "run_id", "task_type", "original_prompt", "ingress_agent_id"
        ):
            if incoming.get(key) != record.get(key):
                raise ValueError("Result metadata mismatch")

        if record["status"] in {"completed", "failed"}:
            if (
                record["status"] != incoming["status"]
                or record.get("result") != incoming.get("result")
            ):
                raise ValueError("Conflicting terminal result")
        else:
            record.clear()
            record.update(json.loads(json.dumps(incoming)))
            record["current_agent_id"] = sender
            record["latency_seconds"] = (
                monotonic() - self.submitted_at[task_id]
            )
            record["latency_scope"] = "ingress_to_result_received"
            self._save(record)

        return {
            "accepted": True,
            "latency_seconds": record["latency_seconds"],
        }
