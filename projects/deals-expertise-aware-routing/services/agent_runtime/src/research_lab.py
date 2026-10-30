"""Concurrent batches, recorded routing explanations and benchmark experiments."""
import asyncio
import hashlib
import json
import math
import os
import random
import re
from collections import Counter, deque
from dataclasses import asdict, replace
from fractions import Fraction
from statistics import median
from time import monotonic, time
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from uuid import uuid4
from fastapi import APIRouter, HTTPException, Request as WebRequest
from fastapi.responses import FileResponse
from .network import NetworkRuntime
from .runtime import ROOT
from .warmup_cache import WarmupCache, fingerprint
import inspect
router = APIRouter(prefix='/lab-api')
CATALOG = {'math': (700, 140, 7), 'bbh': (627, 100, 20), 'mmlu_pro': (200, 100, 5)}
ROLES = [{'name': 'Mathematics', 'skills': ['algebra', 'geometry', 'number theory', 'calculation'], 'prompt': 'Specialize in mathematics. Check algebra, units, boundary cases and arithmetic before returning the final answer.'}, {'name': 'Reasoning and code', 'skills': ['logic', 'algorithms', 'computer science', 'BBH'], 'prompt': 'Specialize in logical reasoning and computer science. Track constraints, test counterexamples and check each inference.'}, {'name': 'Knowledge', 'skills': ['biology', 'history', 'law', 'MMLU-Pro'], 'prompt': 'Specialize in knowledge questions. Compare all options, distinguish facts from assumptions and check the answer format.'}]
TERMINAL = {'completed', 'failed', 'transfer_uncertain', 'result_delivery_pending'}

def read_json(path, default):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return default

def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    temporary.replace(path)

def fetch(url, payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(url, data=body, headers={'Content-Type': 'application/json', 'User-Agent': 'DEALS-local-lab'})
    with urlopen(request, timeout=45) as response:
        return json.load(response)

def boxed(text):
    text = str(text)
    matches = list(re.finditer('\\\\boxed\\s*\\{', text))
    if not matches:
        return None
    start = matches[-1].end()
    depth = 1
    for end in range(start, len(text)):
        depth += (text[end] == '{') - (text[end] == '}')
        if depth == 0:
            return text[start:end]
    return None

def normalize(text):
    text = boxed(str(text)) or str(text).strip()
    answers = re.findall('(?:FINAL|RESULT|Answer|The answer is)\\s*[:=]?\\s*(.+)', text, re.I)
    text = answers[-1] if answers else text
    text = text.strip().rstrip('.').strip('$ ')
    # Unwrap inline/display math delimiters such as \(...\) and \[...\].
    wrapped = re.fullmatch(r'\\\((.*)\\\)|\\\[(.*)\\\]', text, re.S)
    if wrapped:
        text = (wrapped[1] if wrapped[1] is not None else wrapped[2]).strip().rstrip('.').strip('$ ')
    text = re.sub('\\\\(?:dfrac|tfrac)', '\\\\frac', text)
    text = re.sub('\\\\(?:left|right|,|!|;|quad|qquad|displaystyle)', '', text)
    # Formatting-only differences: \text{..}, degrees and percent signs.
    text = re.sub(r'\\(?:text|textbf|mathrm|mbox)\s*\{([^{}]*)\}', r'\1', text)
    text = re.sub(r'\^\s*\{?\\circ\}?|\\circ|°', '', text)
    text = re.sub(r'\\?%', '', text).strip()
    # A plain number followed only by a unit word, e.g. "10 cups".
    unit = re.fullmatch(r'(-?\d+(?:\.\d+)?)\s+[A-Za-z]{2,}(?: [A-Za-z]+)*', text)
    if unit:
        text = unit[1]
    fraction = re.fullmatch('\\\\frac\\{(-?\\d+)\\}\\{(-?\\d+)\\}', text)
    try:
        if fraction:
            return str(Fraction(int(fraction[1]), int(fraction[2])))
        return str(Fraction(text))
    except (ValueError, ZeroDivisionError):
        return re.sub('\\s+', '', text).strip('(){}').casefold()

def grade(case, answer):
    return normalize(answer) == normalize(case['answer'])

def usage(records):
    calls = {call['call_id']: call for record in records for call in record.get('calls', [])}
    complete = bool(calls) and all((record.get('usage_complete') is True for record in records))
    for call in calls.values():
        measured = call.get('usage', {})
        complete &= measured.get('source') == 'provider' and all((type(measured.get(key)) is int and measured[key] >= 0 for key in ('input_tokens', 'output_tokens')))
    tokens = sum((call['usage']['input_tokens'] + call['usage']['output_tokens'] for call in calls.values())) if complete else None
    return (list(calls.values()), tokens)

def retrieve(memory, text):
    words = Counter(re.findall('\\w+', text.casefold()))
    norm = math.sqrt(sum((value * value for value in words.values()))) or 1
    ranked = []
    for item in memory:
        other = Counter(re.findall('\\w+', item['prompt'].casefold()))
        other_norm = math.sqrt(sum((value * value for value in other.values()))) or 1
        cosine = sum((value * other[word] for word, value in words.items())) / (norm * other_norm)
        score = cosine * (1 + 0.2 / (1 + item.get('seconds', 0) / 60))
        if score >= 0.7:
            ranked.append((score, item))
    ranked.sort(key=lambda item: item[0], reverse=True)
    return [item for _, item in ranked[:3]]

WARMUP_CACHE = WarmupCache(ROOT / '.runtime' / 'warmup_cache.sqlite3')

def run_sizes(payload):
    """Warmup/test case counts for a run; None means the whole split.

    New UI sends warmup_size/test_size ("all" or a number). Older payloads
    used size=mini|full with one shared limit.
    """
    def parse(value):
        if value in (None, 'all', 'full'):
            return None
        number = int(value)
        if number < 1:
            raise ValueError('Run sizes must be at least 1')
        return number
    if 'test_size' in payload or 'warmup_size' in payload:
        return {'warmup': parse(payload.get('warmup_size', 12)), 'test': parse(payload.get('test_size', 12))}
    shared = int(payload.get('limit', 12)) if payload.get('size', 'mini') == 'mini' else None
    return {'warmup': shared, 'test': shared}

def _source_digest(*functions):
    return hashlib.sha256(''.join(inspect.getsource(function) for function in functions).encode()).hexdigest()[:16]

class LabRuntime(NetworkRuntime):

    def __init__(self, agent_id):
        super().__init__(agent_id)
        self.context = {'experiment': 'demo', 'phase': 'warmup'}
        self.learned = {}
        self.feedback_seen = set()
        self.updates = deque(maxlen=300)
        self.history = deque(maxlen=600)
        self.peaks = {'queued': 0, 'in_flight': 0, 'backlog': 0}
        self.lab_jobs = {}
        self.background = set()
        self.lock = asyncio.Lock()
        self.shared_memory = []
        overrides = read_json(ROOT / 'configs/agents/skills.json', {})
        self.profiles = {aid: overrides.get(aid, ROLES[index % len(ROLES)]) for index, aid in enumerate(self.peers.urls)}
        self.role = self.profiles[agent_id]
        original_snapshot = self.state.snapshot
        self.raw_snapshot = original_snapshot

        def snapshot(task_type):
            base = original_snapshot(task_type)
            experiment = self.learned.get(self.context['experiment'], {})
            counts = experiment.get(task_type, {}).get('counts', [0, 0])
            return replace(base, successes=counts[0], failures=counts[1])
        self.state.snapshot = snapshot
        original_route = self.state.routing_decision

        def route(task, neighbors, **policy):
            record = self.records[task.task_id]
            decision = original_route(task, neighbors, **policy)
            local = asdict(self.state.snapshot(task.task_type))
            alpha = policy.get('alpha', 1)
            beta = policy.get('beta', 1)
            weight = policy['weight']
            margin = policy['margin']

            def quality(state):
                return (state['successes'] + alpha) / (state['successes'] + state['failures'] + alpha + beta)
            candidates = []
            for neighbor in neighbors:
                peer = asdict(neighbor)
                eligible = task.hop_count < policy['hop_limit'] and peer['agent_id'] != task.previous_agent
                load_term = local['queued'] + local['in_flight'] - peer['queued'] - peer['in_flight']
                quality_term = weight * (quality(peer) - quality(local))
                candidates.append(peer | {'load_term': load_term, 'quality_term': quality_term, 'score': load_term + quality_term, 'eligible': eligible})
            baseline = record.get('lab', {}).get('system') == 'agentic_rag'
            if baseline:
                decision = original_route(task, [], **policy)
            if baseline:
                reason = 'Central baseline: executor selected by planner'
            elif task.hop_count >= policy['hop_limit']:
                reason = 'Hop limit reached: local execution'
            elif decision['action'] == 'forward':
                reason = 'Best eligible R is strictly greater than margin'
            else:
                reason = 'No eligible neighbor has R strictly greater than margin'
            decision['xai'] = {'local': local, 'local_quality': quality(local), 'candidates': candidates, 'weight': weight, 'margin': margin, 'alpha': alpha, 'beta': beta, 'hop_count': task.hop_count, 'hop_limit': policy['hop_limit'], 'previous_agent': task.previous_agent, 'phase': self.context['phase'], 'reason': reason}
            self.capture()
            return decision
        self.state.routing_decision = route
        original_step = self.executor.step

        def step(task, demos):
            record = self.records[task.task_id]
            metadata = record.get('lab', {})
            memory = self.learned.get(self.context['experiment'], {}).get(task.task_type, {}).get('memory', [])
            selected = retrieve(memory, task.original_prompt) if metadata.get('system') == 'deals' else []
            examples = '\n'.join(('Example: ' + item['prompt'] + '\nVerified answer: ' + item['answer'] for item in selected))
            prefix = 'Agent role: ' + self.role['name'] + '. ' + self.role['prompt']
            if examples:
                prefix += '\nPrivate warmup examples:\n' + examples
            specialized = replace(task, remaining_work=prefix + '\n\nTask:\n' + task.remaining_work)
            record['steps'][-1]['retrieved_trajectories'] = selected
            record.setdefault('specializations', []).append({'agent': agent_id, 'role': self.role['name'], 'retrieved': len(selected)})
            return original_step(specialized, demos)
        self.executor.step = step
        self.spawn(self.sampler())

    def spawn(self, coroutine):
        job = asyncio.create_task(coroutine)
        self.background.add(job)
        job.add_done_callback(self.background.discard)
        return job

    def types(self):
        return {task_type: asdict(self.state.snapshot(task_type)) for task_type in self.state.task_types}

    def capture(self):
        states = self.types()
        queued = sum((state['queued'] for state in states.values()))
        active = sum((state['in_flight'] for state in states.values()))
        point = {'at': time(), 'queued': queued, 'in_flight': active, 'backlog': queued + active}
        for key in self.peaks:
            self.peaks[key] = max(self.peaks[key], point[key])
        self.history.append(point)

    def _save(self, record):
        super()._save(record)
        if hasattr(self, 'history'):
            self.capture()

    def prune(self):
        for task_id in list(self.records):
            if len(self.records) < 900:
                break
            if self.records[task_id]['status'] in {'completed', 'failed'}:
                self.records.pop(task_id)

    def receive(self, payload):
        self.prune()
        return super().receive(payload)

    async def sampler(self):
        while True:
            self.capture()
            await asyncio.sleep(0.3)

    async def rpc(self, agent, path, payload=None):
        return await asyncio.to_thread(fetch, self.peers.urls[agent] + '/lab-api' + path, payload)

    async def all_nodes(self):

        async def one(agent):
            try:
                return await self.rpc(agent, '/node')
            except Exception as error:
                return {'agent_id': agent, 'status': 'offline', 'error': type(error).__name__}
        return await asyncio.gather(*(one(agent) for agent in self.peers.urls))

    async def configure(self, experiment, phase):
        for _ in range(100):
            nodes = await self.all_nodes()
            if any((node['status'] != 'ok' for node in nodes)):
                raise RuntimeError('All configured agents must be online')
            if all((not node['history'] or node['history'][-1]['backlog'] == 0 for node in nodes)):
                break
            await asyncio.sleep(0.3)
        else:
            raise RuntimeError('Agents are busy; wait for outstanding tasks')
        await asyncio.gather(*(self.rpc(agent, '/configure', {'experiment': experiment, 'phase': phase}) for agent in self.peers.urls))

    def map_type(self, label):
        types = list(self.state.task_types)
        if label in types:
            return label

        def normalize_type(value):
            return re.sub('[^a-z0-9]', '', value.casefold())
        matches = [value for value in types if normalize_type(value) == normalize_type(label)]
        if matches:
            return matches[0]
        domain = label.split('.')[0]
        matches = [value for value in types if value.startswith(domain + '.')]
        return matches[0] if matches else types[0]

    async def submit_cases(self, cases, system, job, phase, entry=None, prompts=None):
        grouped = {}
        locations = {}
        agents = list(self.peers.urls)
        rng = random.Random(1 if phase == 'warmup' else 2)
        distributions = {}
        for case in cases:
            task_type = self.map_type(case['type'])
            if task_type not in distributions:
                distributions[task_type] = [rng.gammavariate(0.7, 1) for _ in agents]
            agent = entry or rng.choices(agents, weights=distributions[task_type])[0]
            item = {'prompt': (prompts or {}).get(case['id'], case['prompt']) + '\nReturn your final answer as FINAL: <answer>.', 'task_type': task_type, 'lab': {'job': job['id'], 'case_id': case['id'], 'system': system, 'phase': phase}}
            grouped.setdefault(agent, []).append(item)
        replies = await asyncio.gather(*(self.rpc(agent, '/admit', {'tasks': tasks}) for agent, tasks in grouped.items()))
        for (agent, _), reply in zip(grouped.items(), replies):
            for item in reply['tasks']:
                locations[item['case_id']] = (agent, item['task_id'])
        records = {}
        deadline = monotonic() + 900
        while len(records) < len(cases):
            if monotonic() > deadline:
                raise RuntimeError('Task deadline reached; inspect outstanding traces before another run')

            async def poll(case_id, location):
                agent, task_id = location
                try:
                    record = await self.rpc(agent, '/record/' + quote(task_id, safe=''))
                    return (case_id, record)
                except Exception:
                    return (case_id, None)
            pending = [(case_id, location) for case_id, location in locations.items() if case_id not in records]
            replies = await asyncio.gather(*(poll(case_id, location) for case_id, location in pending))
            for case_id, record in replies:
                if record and record['status'] in TERMINAL:
                    record['lab_completed_at'] = monotonic()
                    records[case_id] = record
                    if phase == 'warmup' and system == 'deals':
                        case = next((case for case in cases if case['id'] == case_id))
                        await self.feedback(case, record, job)
            job['progress'] = f'{system} · {phase} · {len(records)}/{len(cases)} tasks'
            if len(records) < len(cases):
                await asyncio.sleep(0.4)
        return records

    async def feedback(self, case, record, job):
        if record['status'] != 'completed':
            return
        answer = record['result']['final_answer']
        correct = grade(case, answer)
        for agent in set(record.get('executors', [])):
            await self.rpc(agent, '/feedback', {'key': job['id'] + ':' + case['id'], 'correct': correct, 'type': record['task_type'], 'prompt': case['prompt'], 'answer': answer, 'seconds': record.get('latency_seconds', 0) or 0})

    async def baseline(self, cases, job, phase):
        agents = list(self.peers.urls)
        descriptions = '; '.join((agent + ': ' + self.profiles[agent]['name'] for agent in agents))
        planner_prompts = {case['id']: 'Choose one executor. Return only its agent ID. Agents: ' + descriptions + '\nQuestion: ' + case['prompt'] for case in cases}
        plans = await self.submit_cases(cases, 'agentic_rag', job, phase, entry=agents[0], prompts=planner_prompts)
        chosen = {}
        for case in cases:
            answer = (plans[case['id']].get('result') or {}).get('final_answer', '')
            chosen[case['id']] = next((agent for agent in agents if agent in answer), agents[0])
        results = {}
        executor_jobs = []
        for agent in agents:
            subset = [case for case in cases if chosen[case['id']] == agent]
            if not subset:
                continue
            prompts = {}
            for case in subset:
                selected = retrieve(self.shared_memory, case['prompt'])
                demonstrations = '\n'.join(('Example: ' + item['prompt'] + '\nVerified answer: ' + item['answer'] for item in selected))
                prompts[case['id']] = 'Shared training retrieval:\n' + demonstrations + '\nQuestion:\n' + case['prompt']
            executor_jobs.append(self.submit_cases(subset, 'agentic_rag', job, phase, entry=agent, prompts=prompts))
        for group in await asyncio.gather(*executor_jobs):
            results.update(group)
        review_prompts = {case['id']: 'Check this candidate answer against the question. Return the corrected final answer.\nQuestion: ' + case['prompt'] + '\nCandidate: ' + str((results[case['id']].get('result') or {}).get('final_answer', 'Execution failed')) for case in cases}
        reviewed = await self.submit_cases(cases, 'agentic_rag', job, phase, entry=agents[0], prompts=review_prompts)
        combined = {}
        for case in cases:
            case_id = case['id']
            stages = [plans[case_id], results[case_id], reviewed[case_id]]
            final = dict(reviewed[case_id])
            final['calls'] = [call for record in stages for call in record.get('calls', [])]
            final['usage_complete'] = all((record.get('usage_complete') for record in stages))
            final['status'] = 'completed' if all((record['status'] == 'completed' for record in stages)) else 'failed'
            final['latency_seconds'] = final['lab_completed_at'] - job['phase_started']
            final['steps'] = [step for record in stages for step in record.get('steps', [])]
            combined[case_id] = final
            if phase == 'warmup' and final['status'] == 'completed' and grade(case, final['result']['final_answer']):
                self.shared_memory.append({'prompt': case['prompt'], 'answer': final['result']['final_answer'], 'seconds': final['latency_seconds']})
                self.shared_memory = self.shared_memory[-40:]
        return combined

    def warmup_fingerprint(self, dataset, system, cases):
        """Key covering everything that shapes learned warmup state."""
        agents = read_json(ROOT / 'configs/agents/local.json', {}).get('agents', [])
        context = {
            'models': {agent.get('agent_id'): agent.get('model') for agent in agents},
            'profiles': self.profiles,
            'routing': self.config['routing'],
            'execution': read_json(ROOT / 'configs/models/execution.json', {}),
            'code': _source_digest(boxed, normalize, grade, retrieve, LabRuntime.baseline, LabRuntime.feedback),
        }
        return fingerprint(dataset, system, [case['id'] for case in cases], context)

    async def capture_warmup(self, experiment, system):
        if system == 'agentic_rag':
            return {'shared_memory': list(self.shared_memory)}
        agents = list(self.peers.urls)
        replies = await asyncio.gather(*(self.rpc(agent, '/learned/export', {'experiment': experiment}) for agent in agents))
        return {'agents': {agent: reply.get('learned', {}) for agent, reply in zip(agents, replies)}}

    async def restore_warmup(self, experiment, system, stored):
        if system == 'agentic_rag':
            self.shared_memory = list(stored.get('shared_memory', []))
            return
        learned = stored.get('agents', {})
        missing = [agent for agent in self.peers.urls if agent not in learned]
        if missing:
            raise RuntimeError('Cached warmup has no state for ' + ', '.join(missing) + '; run a fresh warmup')
        await asyncio.gather(*(self.rpc(agent, '/learned/load', {'experiment': experiment, 'learned': learned[agent]}) for agent in self.peers.urls))

    def report(self, dataset, system, phase, cases, records, elapsed, job, provenance):
        tasks = []
        for case in cases:
            record = records[case['id']]
            calls, tokens = usage([record])
            tasks.append({'case_id': case['id'], 'correct': record['status'] == 'completed' and grade(case, (record.get('result') or {}).get('final_answer', '')), 'status': record['status'], 'usage_complete': tokens is not None, 'latency_seconds': record.get('latency_seconds', elapsed) or elapsed, 'calls': calls, 'trace': record, 'original_type': case['type'], 'runtime_type': record['task_type']})
        calls, tokens = usage(tasks)
        result = {'kind': 'benchmark', 'schema_version': 1, 'dataset': dataset, 'system': system, 'phase': phase, 'comparison_group': job['id'], 'token_scope': 'all attempted planner, execution, split, retrieval-prompt and review calls', 'elapsed_seconds': elapsed, 'tasks': tasks, 'provenance': provenance, 'case_set': hashlib.sha256(json.dumps(sorted((case['id'] for case in cases))).encode()).hexdigest(), 'protocol': {'grader': 'conservative normalized exact match', 'retriever': 'lexical cosine + execution-time factor; not BGE', 'profiles': self.profiles, 'routing': self.config['routing'], 'baseline': 'central planner, shared warmup retrieval, specialist executor, central reviewer'}, 'warmup': getattr(self, 'current_warmup', None), 'node_evidence': job.get('node_evidence', [])}
        result['metrics'] = {'accuracy': sum((task['correct'] for task in tasks)) / len(tasks), 'tokens': tokens, 'tokens_per_task': tokens / len(tasks) if tokens is not None else None, 'median_latency': median((task['latency_seconds'] for task in tasks)), 'throughput': len(tasks) / elapsed, 'failed': sum((task['status'] != 'completed' for task in tasks)), 'cases': len(tasks), 'forwards': sum((step.get('routing', {}).get('action') == 'forward' for record in records.values() for step in record.get('steps', [])))}
        directory = ROOT / 'outputs' / (job['id'] + '-' + dataset + '-' + system + '-' + phase)
        write_json(directory / 'results.json', result)
        summary = {key: value for key, value in result.items() if key != 'tasks'}
        write_json(directory / 'summary.json', summary)
        return summary

    async def run_job(self, job, payload):
        async with self.lock:
            job["status"] = "running"
            try:
                datasets = payload.get("datasets", list(CATALOG))
                if not isinstance(datasets, list) or not datasets:
                    raise ValueError("Choose at least one benchmark dataset")

                manifests = {}
                for dataset in datasets:
                    if dataset not in CATALOG:
                        raise ValueError("Unknown benchmark: " + str(dataset))

                    source = read_json(
                        ROOT / "data/lab" / (dataset + ".json"), None
                    )
                    if not isinstance(source, dict) or source.get("dataset") != dataset:
                        raise RuntimeError(
                            "Download or import " + dataset + " data first"
                        )

                    validate_manifest(source)
                    manifests[dataset] = source

                for dataset, source in manifests.items():
                    sizes = run_sizes(payload)
                    warmup_cases = source["warmup"][:sizes["warmup"]]
                    test_cases = source["test"][:sizes["test"]]
                    if not warmup_cases or not test_cases:
                        raise ValueError(dataset + " needs warmup and test cases")
                    job.setdefault("plan", {})[dataset] = {
                        "warmup_cases": len(warmup_cases),
                        "test_cases": len(test_cases),
                    }

                    for system in ("deals", "agentic_rag"):
                        experiment = job["id"] + "-" + dataset + "-" + system
                        key, details = self.warmup_fingerprint(
                            dataset, system, warmup_cases
                        )
                        cached = (
                            None if payload.get("fresh_warmup")
                            else WARMUP_CACHE.get(key)
                        )

                        if cached:
                            # Reuse a stored warmup: no warmup tasks are run.
                            await self.configure(experiment, "warmup")
                            job["progress"] = (
                                f"{system} · warmup · loading cached warmup"
                            )
                            await self.restore_warmup(
                                experiment, system, cached["payload"]
                            )
                            self.current_warmup = {
                                "source": "cache", "key": key,
                                "cases": len(warmup_cases),
                                "created_at": cached["created_at"],
                                "source_job": cached["source_job"],
                            }
                        else:
                            if system == "agentic_rag":
                                self.shared_memory = []
                            await self.configure(experiment, "warmup")
                            job["phase_started"] = monotonic()
                            if system == "agentic_rag":
                                records = await self.baseline(
                                    warmup_cases, job, "warmup"
                                )
                            else:
                                records = await self.submit_cases(
                                    warmup_cases, system, job, "warmup"
                                )
                            job["node_evidence"] = await self.all_nodes()
                            self.current_warmup = {
                                "source": "fresh", "key": key,
                                "cases": len(warmup_cases),
                                "created_at": time(),
                                "source_job": job["id"],
                            }
                            job["results"].append(self.report(
                                dataset, system, "warmup", warmup_cases,
                                records, monotonic() - job["phase_started"],
                                job, source["provenance"]
                            ))
                            WARMUP_CACHE.put(
                                key, details,
                                await self.capture_warmup(experiment, system),
                                job["id"],
                            )

                        job.setdefault("warmups", []).append({
                            "dataset": dataset, "system": system,
                            **self.current_warmup,
                        })

                        await self.configure(experiment, "test")
                        job["phase_started"] = monotonic()
                        job["progress"] = f"{system} · test · starting"
                        if system == "agentic_rag":
                            records = await self.baseline(test_cases, job, "test")
                        else:
                            records = await self.submit_cases(
                                test_cases, system, job, "test"
                            )
                        job["node_evidence"] = await self.all_nodes()
                        job["results"].append(self.report(
                            dataset, system, "test", test_cases, records,
                            monotonic() - job["phase_started"],
                            job, source["provenance"]
                        ))

                job.update(
                    status="completed",
                    progress="Reports saved; inspect paired test results"
                )
            except Exception as error:
                job.update(
                    status="failed",
                    error=str(error),
                    progress="Run stopped; inspect task traces"
                )
            finally:
                job.pop("phase_started", None)
                write_json(
                    ROOT / "outputs" / job["id"] / "lab-job.json", job
                )

    def new_job(self, payload):
        if any((job['status'] in {'running', 'queued'} for job in self.lab_jobs.values())):
            raise ValueError('An experiment is already active')
        job = {'id': 'lab-' + uuid4().hex[:10], 'status': 'queued', 'progress': 'Preparing agents', 'results': [], 'created_at': time()}
        self.lab_jobs[job['id']] = job
        self.spawn(self.run_job(job, payload))
        return job

    async def close(self):
        for task in self.background:
            task.cancel()
        await asyncio.gather(*tuple(self.background), return_exceptions=True)
        await super().close()

def validate_manifest(data):
    if data.get('dataset') not in CATALOG or not isinstance(data.get('provenance'), str):
        raise ValueError('Need dataset=math|bbh|mmlu_pro and provenance')
    all_ids = set()
    all_prompts = set()
    for phase in ('warmup', 'test'):
        cases = data.get(phase)
        if not isinstance(cases, list) or not cases or len(cases) > 2000:
            raise ValueError('Both warmup and test must contain 1-2000 cases')
        for case in cases:
            if not all((isinstance(case.get(key), str) and case[key] for key in ('id', 'prompt', 'answer', 'type'))) or len(case['prompt']) > 15000:
                raise ValueError('Every case requires string id, prompt, answer and type')
            digest = hashlib.sha256(case['prompt'].strip().encode()).hexdigest()
            if case['id'] in all_ids or digest in all_prompts:
                raise ValueError('Duplicate/leaked case across warmup and test')
            all_ids.add(case['id'])
            all_prompts.add(digest)
    return data

def download_data(dataset):
    rng = random.Random(17)
    result = {'dataset': dataset, 'warmup': [], 'test': [], 'provenance': "Public dataset samples, seed 17; not the paper's released case IDs"}

    def rows(repo, config, split, offset=0, length=100):
        url = 'https://datasets-server.huggingface.co/rows?' + urlencode({'dataset': repo, 'config': config, 'split': split, 'offset': offset, 'length': length})
        data = fetch(url)
        return [item['row'] for item in data['rows']]
    if dataset == 'math':
        configurations = ['algebra', 'counting_and_probability', 'geometry', 'intermediate_algebra', 'number_theory', 'prealgebra', 'precalculus']
        for configuration in configurations:
            for phase, split, count in (('warmup', 'train', 100), ('test', 'test', 20)):
                source = rows('EleutherAI/hendrycks_math', configuration, split)
                rng.shuffle(source)
                for item in source[:count]:
                    answer = boxed(item['solution'])
                    if answer is None:
                        raise ValueError('MATH source answer has no boxed result')
                    case_id = hashlib.sha256(item['problem'].encode()).hexdigest()[:16]
                    result[phase].append({'id': configuration + '-' + split + '-' + case_id, 'prompt': item['problem'], 'answer': answer, 'type': 'math.' + configuration})
    elif dataset == 'bbh':
        configurations = ['boolean_expressions', 'causal_judgement', 'date_understanding', 'disambiguation_qa', 'dyck_languages', 'formal_fallacies', 'geometric_shapes', 'hyperbaton', 'logical_deduction_three_objects', 'logical_deduction_five_objects', 'logical_deduction_seven_objects', 'movie_recommendation', 'multistep_arithmetic_two', 'navigate', 'object_counting', 'penguins_in_a_table', 'reasoning_about_colored_objects', 'ruin_names', 'salient_translation_error_detection', 'snarks']
        for index, configuration in enumerate(configurations):
            data = fetch('https://raw.githubusercontent.com/suzgunmirac/BIG-Bench-Hard/main/bbh/' + configuration + '.json')['examples']
            rng.shuffle(data)
            training_count = 31 + (index < 7)
            selections = (('test', data[:5]), ('warmup', data[5:5 + training_count]))
            for phase, selected in selections:
                for item in selected:
                    case_id = hashlib.sha256(item['input'].encode()).hexdigest()[:16]
                    result[phase].append({'id': configuration + '-' + case_id, 'prompt': item['input'], 'answer': item['target'], 'type': 'bbh.' + configuration})
    else:
        subjects = {'biology', 'computer science', 'history', 'law', 'math'}
        pools = {subject: [] for subject in subjects}
        for offset in range(0, 12100, 100):
            source = rows('TIGER-Lab/MMLU-Pro', 'default', 'test', offset)
            for item in source:
                if item['category'] in pools:
                    pools[item['category']].append(item)
            if all((len(pool) >= 60 for pool in pools.values())):
                break
        for subject, pool in pools.items():
            rng.shuffle(pool)
            if len(pool) < 60:
                raise ValueError('Insufficient MMLU-Pro subject data')
            for phase, selected in (('warmup', pool[:40]), ('test', pool[40:60])):
                for item in selected:
                    options = '\n'.join((f'({chr(65 + index)}) {value}' for index, value in enumerate(item['options'])))
                    result[phase].append({'id': 'mmlu-' + str(item['question_id']), 'prompt': item['question'] + '\n' + options + '\nReturn only the option letter.', 'answer': item['answer'], 'type': 'mmlu_pro.' + subject.replace(' ', '_')})
    for phase in ('warmup', 'test'):
        rng.shuffle(result[phase])
    validate_manifest(result)
    write_json(ROOT / 'data/lab' / (dataset + '.json'), result)
    return {'dataset': dataset, 'warmup': len(result['warmup']), 'test': len(result['test'])}

@router.get('/ui', include_in_schema=False)
async def frontend():
    return FileResponse(ROOT / 'apps/frontend/research-lab.html')

@router.get('/node')
async def node(request: WebRequest):
    runtime = request.app.state.runtime
    return {'agent_id': runtime.state.agent_id, 'status': 'ok', 'model': runtime.executor.model, 'role': runtime.role, 'capacity': runtime.state.capacity, 'types': runtime.types(), 'peaks': runtime.peaks, 'history': list(runtime.history)[-120:], 'context': runtime.context, 'updates': list(runtime.updates)[-40:]}

@router.post('/configure')
async def configure(request: WebRequest, data: dict):
    runtime = request.app.state.runtime
    if any((state['queued'] + state['in_flight'] for state in runtime.types().values())):
        raise HTTPException(409, 'Node has outstanding work')
    if data.get('phase') not in {'warmup', 'test'} or not isinstance(data.get('experiment'), str):
        raise HTTPException(422, 'Invalid context')
    runtime.context = {'experiment': data['experiment'], 'phase': data['phase']}
    runtime.history.clear()
    runtime.peaks = dict.fromkeys(runtime.peaks, 0)
    return {'ok': True}

@router.post('/admit')
async def admit(request: WebRequest, data: dict):
    runtime = request.app.state.runtime
    tasks = data.get('tasks', [])
    if not 1 <= len(tasks) <= 1000:
        raise HTTPException(422, 'Batch must contain 1-1000 tasks')
    if any((item.get('task_type') not in runtime.state.task_types or not isinstance(item.get('prompt'), str) or (not 0 < len(item['prompt']) <= 20000) for item in tasks)):
        raise HTTPException(422, 'Invalid task type or prompt')
    removable = [task_id for task_id, record in runtime.records.items() if record['status'] in {'completed', 'failed'}]
    while len(runtime.records) + len(tasks) > 950 and removable:
        runtime.records.pop(removable.pop(0), None)
    if len(runtime.records) + len(tasks) > 1000:
        raise HTTPException(409, 'Too many outstanding tasks')
    reply = []
    for item in tasks:
        task_id = runtime.submit(item['prompt'], item['task_type'])
        runtime.records[task_id]['lab'] = item['lab']
        runtime._save(runtime.records[task_id])
        reply.append({'task_id': task_id, 'case_id': item['lab']['case_id']})
    return {'tasks': reply}

@router.get('/record/{task_id}')
async def record(request: WebRequest, task_id: str):
    data = request.app.state.runtime.records.get(task_id)
    if data is None:
        raise HTTPException(404, 'Trace unavailable')
    return data

@router.get('/records')
async def records(request: WebRequest):
    return list(request.app.state.runtime.records.values())[-200:]

@router.post('/learned/export')
async def learned_export(request: WebRequest, data: dict):
    """This agent's learned warmup state for one experiment."""
    runtime = request.app.state.runtime
    experiment = data.get('experiment')
    if not isinstance(experiment, str):
        raise HTTPException(422, 'experiment is required')
    return {'learned': runtime.learned.get(experiment, {})}

@router.post('/learned/load')
async def learned_load(request: WebRequest, data: dict):
    """Install cached warmup state so a test can run without re-warming."""
    runtime = request.app.state.runtime
    experiment = data.get('experiment')
    learned = data.get('learned')
    if not isinstance(experiment, str) or not isinstance(learned, dict):
        raise HTTPException(422, 'experiment and learned are required')
    if any((state['queued'] + state['in_flight'] for state in runtime.types().values())):
        raise HTTPException(409, 'Node has outstanding work')
    for task_type, bucket in learned.items():
        counts = bucket.get('counts') if isinstance(bucket, dict) else None
        if task_type not in runtime.state.task_types or not (isinstance(counts, list) and len(counts) == 2 and all((type(value) is int and value >= 0 for value in counts))) or not isinstance(bucket.get('memory', []), list):
            raise HTTPException(422, 'Invalid learned state for ' + str(task_type))
    runtime.learned[experiment] = {task_type: {'counts': list(bucket['counts']), 'memory': list(bucket.get('memory', []))} for task_type, bucket in learned.items()}
    return {'loaded': len(learned)}

@router.get('/warmup-cache')
async def warmup_cache_entries():
    return {'entries': WARMUP_CACHE.entries()}

@router.post('/warmup-cache/clear')
async def warmup_cache_clear(request: WebRequest, data: dict):
    runtime = request.app.state.runtime
    if any((job.get('status') in {'queued', 'running', 'stopping'} for job in runtime.lab_jobs.values())):
        raise HTTPException(409, 'Wait for the current evaluation to finish')
    dataset = data.get('dataset')
    if dataset is not None and dataset not in CATALOG:
        raise HTTPException(422, 'Unknown dataset')
    return {'deleted': WARMUP_CACHE.clear(dataset)}

@router.post('/feedback')
async def feedback(request: WebRequest, data: dict):
    runtime = request.app.state.runtime
    if runtime.context['phase'] != 'warmup':
        raise HTTPException(409, 'Test phase freezes quality and memory')
    key = runtime.context['experiment'] + ':' + data['key']
    if key in runtime.feedback_seen:
        return {'duplicate': True}
    if type(data.get('correct')) is not bool or data.get('type') not in runtime.state.task_types:
        raise HTTPException(422, 'Invalid verified feedback')
    experiment = runtime.learned.setdefault(runtime.context['experiment'], {})
    bucket = experiment.setdefault(data['type'], {'counts': [0, 0], 'memory': []})
    before = list(bucket['counts'])
    bucket['counts'][0 if data['correct'] else 1] += 1
    if data['correct']:
        bucket['memory'].append({key: data[key] for key in ('prompt', 'answer', 'seconds')} | {'stored_at': time()})
        while sum((len(value['memory']) for value in experiment.values())) > 40:
            oldest = min((value for value in experiment.values() if value['memory']), key=lambda value: value['memory'][0]['stored_at'])
            oldest['memory'].pop(0)
    runtime.feedback_seen.add(key)
    runtime.updates.append({'at': time(), 'type': data['type'], 'before': before, 'after': list(bucket['counts']), 'correct': data['correct']})
    return {'accepted': True}

@router.get('/state')
async def state(request: WebRequest):
    runtime = request.app.state.runtime
    nodes = await runtime.all_nodes()

    async def get_records(agent):
        try:
            return await runtime.rpc(agent, '/records')
        except Exception:
            return []
    groups = await asyncio.gather(*(get_records(agent) for agent in runtime.peers.urls))
    selected = {}
    for group in groups:
        for record in group:
            old = selected.get(record['task_id'])

            def rank(value):
                return (value['status'] in {'completed', 'failed'}, len(value.get('steps', [])))
            if old is None or rank(record) > rank(old):
                selected[record['task_id']] = record
    ready = {dataset: {phase: len(read_json(ROOT / 'data/lab' / (dataset + '.json'), {}).get(phase, [])) for phase in ('warmup', 'test')} for dataset in CATALOG}
    reports = []
    paths = sorted((ROOT / 'outputs').glob('lab-*/summary.json'), key=lambda path: path.stat().st_mtime, reverse=True)
    for path in paths[:60]:
        data = read_json(path, {})
        if 'metrics' in data:
            reports.append({key: value for key, value in data.items() if key != 'tasks'})
    return {'nodes': nodes, 'graph': runtime.peers.graph, 'tasks': list(selected.values())[-200:], 'jobs': list(runtime.lab_jobs.values()), 'ready': ready, 'reports': reports}

@router.post('/jobs')
async def jobs(request: WebRequest, data: dict):
    runtime = request.app.state.runtime
    if runtime.state.agent_id != next(iter(runtime.peers.urls)):
        raise HTTPException(409, "Open the lab on the first agent's port")
    if data.get('mode') not in {'batch', 'benchmark'} or data.get('entry') not in (None, *runtime.peers.urls):
        raise HTTPException(422, 'Invalid mode or ingress')
    try:
        count = int(data.get('count', 18))
        limit = int(data.get('limit', 12))
    except (TypeError, ValueError):
        raise HTTPException(422, 'Invalid task count') from None
    if not 1 <= count <= 100 or not 1 <= limit <= 140:
        raise HTTPException(422, 'Invalid task count')
    if any((dataset not in CATALOG for dataset in data.get('datasets', CATALOG))) or data.get('size', 'mini') not in {'mini', 'full'}:
        raise HTTPException(422, 'Invalid dataset or size')
    try:
        return runtime.new_job(data)
    except ValueError as error:
        raise HTTPException(409, str(error)) from None

@router.post('/datasets')
async def datasets(request: WebRequest, data: dict):
    runtime = request.app.state.runtime
    dataset = data.get('dataset')
    if dataset not in CATALOG:
        raise HTTPException(422, 'Unknown dataset')
    if 'warmup' in data:
        try:
            validate_manifest(data)
        except ValueError as error:
            raise HTTPException(422, str(error)) from None
        write_json(ROOT / 'data/lab' / (dataset + '.json'), data)
        return {'status': 'imported'}
    job = {'id': 'data-' + uuid4().hex[:8], 'status': 'running', 'progress': 'Downloading ' + dataset, 'results': []}
    runtime.lab_jobs[job['id']] = job

    async def download():
        try:
            result = await asyncio.to_thread(download_data, dataset)
            job.update(status='completed', progress=str(result))
        except Exception as error:
            job.update(status='failed', error=str(error), progress='Dataset download failed')
    runtime.spawn(download())
    return job
