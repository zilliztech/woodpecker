/* Presentation of source-owned check events and immutable report results. */
'use strict';
const diagnosticGroupOpen={};
function renderResultOrigin(){
 const saved=!!state.report&&state.resultOrigin==='saved';
 $('#diagnostic-result').dataset.origin=saved?'saved':'current';
 $('#output-mode').textContent=state.busy?'Running':saved?'Saved result':state.report?'Current result':'Ready';
 $('#result-origin').innerHTML=`<span class="result-origin-label">${saved?'SAVED RESULT':'CURRENT RESULT'}</span><div><strong>${esc(state.report?runName(state.report.request):'Check results')}</strong><p>${saved?'Viewing a recorded snapshot. Opening it did not run new checks.':'Collected by this run for the selected incident window.'}</p></div>${state.job?.created_at?`<time>${esc(stamp(state.job.created_at))}</time>`:''}`;
}
function renderDiagnosticPlan(){
 const defs=selectedDefinitions(),results=state.report?.checks||[],by=Object.fromEntries(results.map(r=>[r.id,r])),phases={};
 for(const event of state.job?.check_events||[])phases[event.check_id]=event.phase;
 const groups=new Map();for(const def of defs){if(!groups.has(def.step))groups.set(def.step,[]);groups.get(def.step).push(def);}
 const finalized=state.report?results.filter(r=>r.execution!=='pending').length:defs.filter(d=>phases[d.id]==='complete').length;
 const finalizing=state.busy&&defs.length>0&&finalized===defs.length;
 $('#check-progress').max=Math.max(1,defs.length);$('#check-progress').value=finalized;
 $('#check-progress-label').textContent=state.busy?`${finalized} / ${defs.length} finalized${finalizing?' · preparing report':''}`:state.report?`${finalized} / ${defs.length} finalized · ${state.report.status}`:`${defs.length} checks ready`;
 $('#completion').textContent=state.busy?(finalizing?'Preparing report':'Running'):state.report?(state.report.complete?'Execution complete':'Partial / incomplete'):'Ready';
 let html='';
 for(const [step,items] of groups){
  const done=items.filter(d=>by[d.id]?.execution&&by[d.id].execution!=='pending'||phases[d.id]==='complete').length;
  const active=state.busy&&items.some(d=>phases[d.id]==='collecting'),bad=items.filter(d=>by[d.id]?.status==='abnormal').length;
  const open=Object.hasOwn(diagnosticGroupOpen,step)?diagnosticGroupOpen[step]:active;
  html+=`<details class="check-stage" data-check-stage="${esc(step)}" ${open?'open':''}><summary><span class="stage-name">${active?'<i class="spinner"></i>':''}${esc(titleStep(step))}</span><span class="stage-count">${state.busy||state.report?done+' / ':''}${items.length} checks</span>${bad?`<span class="stage-warning">${bad} abnormal</span>`:''}</summary><div class="check-stage-items">`;
  for(const def of items){
   const result=by[def.id],phase=phases[def.id],collecting=state.busy&&phase==='collecting';
   const unavailable=!result&&!state.busy&&missingForCheck(def).length>0;
   const status=result?.status||'pending',label=result?.status_label||(state.busy?(phase==='complete'?'Collected':collecting?'Collecting':'Queued'):unavailable?'Needs configuration':'Not checked');
   html+=`<button class="check-row is-${status} ${unavailable?'needs-connection':''}" data-check="${esc(def.id)}"><span class="check-mark ${status}">${collecting?'<i class="spinner"></i>':status==='abnormal'?'!':status==='clear'?'✓':phase==='complete'?'·':'—'}</span><div class="check-main"><div class="check-title"><span class="evidence-reference">${esc(evidenceReference(def))}</span>${esc(evidenceTitle(def))}<span class="dependency-icon" data-check-requires="${esc(def.id)}" tabindex="0" role="img" title="${esc(checkRequirementText(def))}" aria-label="${esc(checkRequirementText(def))}">ⓘ</span></div><div class="check-desc">${esc(def.source)} · ${esc(result?.note||def.description.meaning)}</div></div><div class="check-reading"><span class="status ${status}">${esc(label)}${result&&(result.status==='unknown'||result.execution!=='completed')?helpIcon(helpKeyForCheck(result),result):''}</span></div></button>`;
  }
  html+='</div></details>';
 }
 $('#checks').innerHTML=html||'<div class="ops-empty">Choose Client / Server checks and observability to build the plan.</div>';
 for(const icon of document.querySelectorAll('[data-check-requires]'))icon.onclick=e=>{e.preventDefault();e.stopPropagation();};
 for(const panel of document.querySelectorAll('[data-check-stage]'))panel.querySelector('summary').onclick=()=>{diagnosticGroupOpen[panel.dataset.checkStage]=!panel.open;};
}
function renderDiagnosticDetails(){
 $('#filters').innerHTML=[['all','All checks'],['abnormal','Findings'],['gaps','Evidence gaps']].map(([key,label])=>`<button data-filter="${key}" class="${state.filter===key?'active':''}">${label}</button>`).join('');
 const results=(state.report?.checks||[]).filter(r=>state.filter==='all'||state.filter==='abnormal'&&r.status==='abnormal'||state.filter==='gaps'&&(r.status==='unknown'||r.execution!=='completed'));
 $('#result-detail-count').textContent=results.length+' checks';
 $('#result-detail-list').innerHTML=results.map(r=>`<button class="result-check-row" data-check="${esc(r.id)}"><div class="result-check-name"><span class="evidence-reference">${esc(evidenceReference(r.id))}</span><b>${esc(evidenceTitle(r.id))}</b><span>${esc(r.source)} · ${esc(titleStep(r.step))}</span></div><strong class="result-check-value">${esc(r.value||'—')}</strong><span class="status ${esc(r.status)}">${esc(r.status_label||r.status)}${r.status==='unknown'||r.execution!=='completed'?helpIcon(helpKeyForCheck(r),r):''}</span><span class="result-inspect">Inspect ↗</span></button>`).join('')||'<p class="run-history-empty">No checks in this filter.</p>';
}
const nativeCommandHistory=[];
function rememberCommandResult(c,run){
 if(!run?.id||!run.done)return;
 if(!nativeCommandHistory.some(entry=>entry.id===run.id))nativeCommandHistory.unshift({id:run.id,name:c.name,operation:run.operation,result:run.result});
 nativeCommandHistory.splice(8);renderCommandHistory();
}
function renderNativeExecution(run){
 const r=run?.result,steps=[['Submit command',!!run?.id,r&&!run.id],['Run native operation',!!run?.done,!!run&&!run.done],['Capture output and exit status',!!run?.done,false]];
 $('#command-execution').innerHTML=`<ol class="native-execution-steps">${steps.map(([label,done,active])=>`<li class="${active?'running':done?'finished':'waiting'}"><span>${active?'<i class="spinner"></i>':done?'✓':'·'}</span><b>${label}</b><small>${done?'Finished':active?'Running':'Waiting'}</small></li>`).join('')}</ol>`;
}
function renderCommandHistory(){
 const operation=state.tab==='runbooks'?{book:state.runbook,index:operationSteps[state.runbook]||0}:null;
 const rows=operation?nativeCommandHistory.filter(r=>r.operation?.book===operation.book&&r.operation?.index===operation.index):nativeCommandHistory;
 $('#command-history .panel-title').innerHTML='<span class="history-section-label">HISTORY</span> '+(operation?'Step run history':'Recent command results');
 const key=(operation?operation.book+':'+operation.index:'all')+'|'+rows.map(r=>r.id).join('|');if($('#command-history-list').dataset.revision===key)return;
 $('#command-history-list').dataset.revision=key;
 $('#command-history-list').innerHTML=rows.map(r=>`<details class="native-command-record"><summary><code>wp ${esc(r.name)}</code><span>${esc(r.result.status)} · exit ${esc(r.result.exit_code??'—')}</span><time>${esc(stamp(r.result.finished_at))}</time></summary><div class="native-command-record-output"><p>${esc(r.result.context||'No WP context')} · ${esc(r.result.working_directory||'')}</p><pre class="command-terminal">${esc(r.result.stdout||'No stdout.')}</pre>${r.result.stderr?`<pre class="command-terminal stderr">${esc(r.result.stderr)}</pre>`:''}</div></details>`).join('')||'<p class="run-history-empty">'+(operation?'No completed runs for this step.':'No completed command results in this page session.')+'</p>';
}
