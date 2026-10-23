/* ==========================================================================
   Enterprise Agentic Orchestrator — Real-time Dashboard
   ========================================================================== */

let pollIntervalA = null;
let pollIntervalB = null;
let currentApprovalId = null;
let currentCidB = null;
let currentScenarioA_is_fraud = false;
let currentScenarioB_is_fraud = false;

function renderInitialState() {
  const dagContainer = document.getElementById('dag-master-timeline');
  dagContainer.innerHTML = '<div style="font-size: 0.85rem; color: var(--text-dim); font-style: italic; text-align: center; margin-top: 2rem;">Waiting for DAG...</div>';

  const reactContainer = document.getElementById('react-master-transcript');
  reactContainer.innerHTML = '<div style="font-size: 0.85rem; color: var(--text-dim); font-style: italic; text-align: center; margin-top: 2rem;">Awaiting Execution Command...</div>';

  document.getElementById('dag-final-outcome').classList.add('hidden');
  document.getElementById('react-final-outcome').classList.add('hidden');
}

async function runGrandMasterDemo(mode = 'random') {
  const statusTag = document.getElementById('master-status-tag');
  renderInitialState();

  if (pollIntervalA) clearInterval(pollIntervalA);
  if (pollIntervalB) clearInterval(pollIntervalB);
  currentApprovalId = null;
  currentCidB = null;
  lastSequenceB = 0;

  statusTag.innerText = "STARTING PIPELINE...";
  statusTag.style.color = "var(--accent-amber)";

  try {
    const resA = await fetch(`/api/scenarios/A?mode=${mode}`, { method: 'POST' });
    const dataA = await resA.json();
    currentScenarioA_is_fraud = !!dataA.is_fraud;
    
    const contextBoxA = document.querySelector('.panel-unfraud .event-context-box');
    if (currentScenarioA_is_fraud) {
        contextBoxA.innerHTML = `
          <div class="context-title">📥 Incoming Event: High-Value Transaction</div>
          <div class="context-details">Large single transaction attempt.</div>
          <div class="context-rule">Matched Router Rule: <code>R04-default-dag</code> &rarr; <code>known_fraud_containment_dag</code></div>
        `;
    } else {
        contextBoxA.innerHTML = `
          <div class="context-title">📥 Incoming Event: Known Travel Pattern</div>
          <div class="context-details">Safe transaction matching customer itinerary.</div>
          <div class="context-rule">Matched Router Rule: <code>R01-known-task-type-dag</code> &rarr; <code>travel_clearance_dag</code></div>
        `;
    }

    await new Promise(r => setTimeout(r, 600));

    const resB = await fetch(`/api/scenarios/B?mode=${mode}`, { method: 'POST' });
    const dataB = await resB.json();
    currentScenarioB_is_fraud = !!dataB.is_fraud;
    
    const contextBoxB = document.querySelector('.panel-fraud .event-context-box');
    if (currentScenarioB_is_fraud) {
        contextBoxB.innerHTML = `
          <div class="context-title">📥 Incoming Event: Suspicious Activity Burst</div>
          <div class="context-details">Customer: C-9102 | Signals: Phone Change + New Device + Crypto Tx</div>
          <div class="context-rule">Matched Router Rule: <code>R02-multi-signal-ambiguity</code> &rarr; <code>ReAct Agent Loop</code></div>
        `;
    } else {
        contextBoxB.innerHTML = `
          <div class="context-title">📥 Incoming Event: Minor Activity Sequence</div>
          <div class="context-details">Low value transaction with known device ID.</div>
          <div class="context-rule">Matched Router Rule: <code>R02-multi-signal-ambiguity</code> &rarr; <code>ReAct Agent Loop</code></div>
        `;
    }

    statusTag.innerText = "EXECUTING DUAL SCENARIOS...";
    
    pollIntervalA = setInterval(() => pollScenarioA(dataA.correlation_id), 1000);
    pollIntervalB = setInterval(() => pollScenarioB(dataB.correlation_id), 1000);
    currentCidB = dataB.correlation_id;
  } catch (err) {
    statusTag.innerText = "ERROR STARTING PIPELINE.";
    statusTag.style.color = "var(--accent-rose)";
    console.error(err);
  }
}

function renderAirflowTasks(tasks) {
  const dagContainer = document.getElementById('dag-master-timeline');
  if (tasks.length === 0) return;
  let html = '';
  tasks.forEach((t, idx) => {
    let color = 'var(--text-dim)';
    let statusText = t.state || 'PENDING';
    if (t.state === 'success') { color = 'var(--accent-emerald)'; statusText = '✓ COMPLETED'; }
    else if (t.state === 'running') { color = 'var(--accent-amber)'; statusText = 'RUNNING...'; }
    html += `
      <div class="dag-card ${t.state === 'running' ? 'dag-active' : ''}">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.3rem;">
          <span class="dag-step-num">${idx+1} — ${t.task_id}</span>
          <span style="font-size: 0.7rem; font-family: 'JetBrains Mono', monospace; color: ${color};">${statusText.toUpperCase()}</span>
        </div>
      </div>
    `;
  });
  dagContainer.innerHTML = html;
}

async function pollScenarioA(cid) {
  try {
    const res = await fetch(`/api/orchestrator/runs/${cid}`);
    if (res.status === 404) return;
    const runData = await res.json();
    
    const taskRes = await fetch(`/api/orchestrator/runs/${cid}/tasks`);
    if (taskRes.ok) {
        const tasksData = await taskRes.json();
        renderAirflowTasks(tasksData.tasks);
    }

    if (runData.overall_status === 'COMPLETED' || runData.overall_status === 'FAILED') {
      clearInterval(pollIntervalA);
      pollIntervalA = null;
      const outcomeDiv = document.getElementById('dag-final-outcome');
      
      if (currentScenarioA_is_fraud) {
        outcomeDiv.className = 'outcome-box outcome-warning';
        outcomeDiv.innerHTML = `
          <div class="outcome-title">🚫 POLICY DECISION: BLOCK_AND_ALERT (FRAUD)</div>
          <div class="outcome-desc">
            High risk score detected. Transaction blocked deterministically by Airflow.
          </div>
        `;
      } else {
        outcomeDiv.className = 'outcome-box outcome-success';
        outcomeDiv.innerHTML = `
          <div class="outcome-title">✓ POLICY DECISION: AUTO_ALLOW (LEGITIMATE)</div>
          <div class="outcome-desc">
            Transaction within safe threshold. Processed seamlessly with zero friction.
          </div>
        `;
      }
      
      outcomeDiv.classList.remove('hidden');
      checkAllDone();
    }
  } catch (err) {
    console.error("Error polling A:", err);
  }
}

let lastSequenceB = 0;
async function pollScenarioB(cid) {
  try {
    const res = await fetch(`/api/orchestrator/runs/${cid}`);
    if (res.status === 404) return;
    const runData = await res.json();

    const traceRes = await fetch(`/api/orchestrator/runs/${cid}/trace?after_sequence=${lastSequenceB}`);
    if (traceRes.ok) {
        const traceData = await traceRes.json();
        const reactContainer = document.getElementById('react-master-transcript');
        
        for (let evt of traceData.events) {
            lastSequenceB = Math.max(lastSequenceB, evt.sequence_number);
            
            let bubbleHtml = '';
            const ts = new Date(evt.timestamp).toLocaleTimeString();
            
            if (evt.event_type === 'AgentSelected') {
                bubbleHtml = `
                    <div class="chat-bubble role-assistant">
                        <div class="chat-meta">
                        <span class="chat-sender">GPT-4o THOUGHT (Iteration ${evt.data.iteration})</span>
                        <span style="color: var(--text-dim);">${ts}</span>
                        </div>
                        <div class="chat-content">Select Agent: ${evt.data.selected_agent}<br/><i>${evt.data.selection_reason}</i></div>
                    </div>
                `;
            } else if (evt.event_type === 'ObservationReceived') {
                bubbleHtml = `
                    <div class="chat-bubble role-agent">
                        <div class="chat-meta">
                        <span class="chat-sender">AGENT RESPONSE (${evt.data.agent_id})</span>
                        <span style="color: var(--text-dim);">${ts}</span>
                        </div>
                        <div class="chat-content">${evt.data.observation_summary}</div>
                    </div>
                `;
            } else if (evt.event_type === 'InvestigationGoalCreated') {
                bubbleHtml = `
                    <div class="chat-bubble role-assistant">
                        <div class="chat-meta">
                        <span class="chat-sender">GPT-4o INVESTIGATION PLAN</span>
                        <span style="color: var(--text-dim);">${ts}</span>
                        </div>
                        <div class="chat-content"><b>Goal:</b> ${evt.data.goal}</div>
                    </div>
                `;
            } else if (evt.event_type === 'NextStepSelected') {
                 if (evt.data.action === "COMPLETE") {
                    bubbleHtml = `
                        <div class="chat-bubble role-assistant">
                            <div class="chat-meta">
                            <span class="chat-sender">GPT-4o CONCLUSION</span>
                            <span style="color: var(--text-dim);">${ts}</span>
                            </div>
                            <div class="chat-content">${evt.data.decision_summary}</div>
                        </div>
                    `;
                 } else if (evt.data.action === "CALL_AGENT") {
                    bubbleHtml = `
                        <div class="chat-bubble role-assistant">
                            <div class="chat-meta">
                            <span class="chat-sender">GPT-4o REASONING (Iteration ${evt.data.iteration})</span>
                            <span style="color: var(--text-dim);">${ts}</span>
                            </div>
                            <div class="chat-content">${evt.data.decision_summary}</div>
                        </div>
                    `;
                 }
            }

            if (bubbleHtml) {
                // Clear "Waiting..." placeholder if present
                if (reactContainer.innerHTML.includes('Awaiting Execution Command...')) {
                    reactContainer.innerHTML = '';
                }
                reactContainer.innerHTML += bubbleHtml;
                reactContainer.scrollTop = reactContainer.scrollHeight;
            }
        }
    }

    if (runData.overall_status === 'COMPLETED' || runData.overall_status === 'FAILED' || 
        runData.overall_status === 'APPROVED' || runData.overall_status === 'REJECTED') {
      clearInterval(pollIntervalB);
      pollIntervalB = null;
      
      const outcomeDiv = document.getElementById('react-final-outcome');
      outcomeDiv.classList.remove('hidden');
      
      if (runData.overall_status === 'APPROVED' || runData.overall_status === 'REJECTED') {
          outcomeDiv.className = runData.overall_status === 'APPROVED' ? 'outcome-box outcome-success' : 'outcome-box outcome-warning';
          outcomeDiv.innerHTML = `
              <div class="outcome-title">✓ ACTION ${runData.overall_status} (HUMAN REVIEWED)</div>
              <div class="outcome-desc">
                Fraud Analyst reviewed the case.
              </div>
            `;
      } else if (runData.overall_status === 'COMPLETED') {
          outcomeDiv.className = 'outcome-box outcome-success';
          outcomeDiv.innerHTML = `
              <div class="outcome-title">✓ POLICY DECISION: AUTO_ALLOW (UNFRAUD)</div>
              <div class="outcome-desc">
                Agent verified activity is legitimate. Auto-Allowed transaction.
              </div>
            `;
      }
      
      checkAllDone();
    } else if (runData.overall_status === 'AWAITING_APPROVAL') {
       // Fetch pending approvals
       const appRes = await fetch(`/api/approvals?correlation_id=${encodeURIComponent(cid)}`);
       if (appRes.ok) {
           const approvals = await appRes.json();
           const pending = approvals.find(a => a.status === 'PENDING');
           if (pending) {
               currentApprovalId = pending.approval_id;
               clearInterval(pollIntervalB);
               pollIntervalB = null;
               
               const outcome = document.getElementById('react-final-outcome');
               outcome.classList.remove('hidden');
               outcome.innerHTML = `
                <div class="outcome-title">⚠️ POLICY DECISION: REQUIRE_APPROVAL (FRAUD RISK)</div>
                <div class="outcome-desc">
                  <b>Orchestrator Recommendation:</b> ${runData.routing?.rationale || pending.action_type}
                </div>
                <div class="approval-actions">
                  <button class="btn btn-approve" onclick="resolveMasterApproval('APPROVE')">Approve Action</button>
                  <button class="btn btn-reject" onclick="resolveMasterApproval('REJECT')">Reject Action</button>
                </div>
              `;
           }
       }
    }
    
  } catch (err) {
    console.error("Error polling B:", err);
  }
}

function checkAllDone() {
   if (!pollIntervalA && !pollIntervalB) {
        const statusTag = document.getElementById('master-status-tag');
        statusTag.innerText = "PIPELINE EXECUTION COMPLETE.";
        statusTag.style.color = "var(--accent-emerald)";
   }
}

async function resolveMasterApproval(decision) {
  if (!currentApprovalId) return;
  try {
     const res = await fetch(`/api/approvals/${currentApprovalId}/decision`, {
         method: 'POST',
         headers: { 'Content-Type': 'application/json' },
         body: JSON.stringify({
             decision: decision,
             reviewer_id: 'dashboard-user'
         })
     });
     
     if (res.ok) {
         // Resume polling to see the outcome
         pollIntervalB = setInterval(() => pollScenarioB(currentCidB), 1000);
         const outcome = document.getElementById('react-final-outcome');
         outcome.innerHTML = `<div class="outcome-title">Processing decision...</div>`;
     }
  } catch (err) {
     console.error("Approval error:", err);
  }
}
