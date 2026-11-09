/* Retained diagnostic runs and report-only Agent analysis. No check rules. */
'use strict';
let historyRevision=0,historyLoading=false,analysisSelectionRevision=0;
const historyPanels={};
function runAnalyses(id){return state.jobs.filter(j=>j.kind==='analysis'&&j.parent_id===id);}
function latestCompletedAnalysis(id){return runAnalyses(id).find(j=>j.status==='completed'&&j.report_ready);}
function runName(q){return state.catalog.scenarios.find(s=>s.id===q?.scenario)?.title||q?.check||q?.scenario||'Diagnostic run';}
async function loadRunHistory(){
 const revision=++historyRevision;historyLoading=true;
 try{const jobs=await api('/api/runs');if(revision!==historyRevision)return;state.jobs=jobs;state.historyError='';renderRunHistory();}
 catch(e){if(revision===historyRevision){state.historyError=e.message;renderRunHistory();}}
 finally{if(revision===historyRevision)historyLoading=false;}
}
function renderRunHistory(){
 if(!state.info||!state.catalog)return;
 const runs=state.jobs.filter(j=>j.kind==='diag').sort((a,b)=>Date.parse(b.created_at)-Date.parse(a.created_at));
 $('#run-history-count').textContent=runs.length+' run'+(runs.length===1?'':'s');
 $('#run-history-progress').hidden=!state.restoring;
 $('#run-history-error').hidden=!state.historyError;$('#run-history-error').textContent=state.historyError;
 $('#run-history-list').innerHTML=runs.map(j=>{
  const q=j.request||{},versions=runAnalyses(j.id),active=versions.find(a=>!a.report_ready),latest=latestCompletedAnalysis(j.id);
  const metrics=j.summary?.metrics,trace=j.summary?.trace,bad=(metrics?.abnormal||0)+(trace?.abnormal||0),failed=(metrics?.failed||0)+(trace?.failed||0);
  const canAnalyze=j.report_ready&&bad>0&&state.info.agent.ready&&!active&&!state.busy&&!state.restoring;
  const reason=!j.report_ready?'Wait for these checks to finish':!bad?'No abnormal findings':!state.info.agent.ready?'Configure an Agent in Settings':active?'Analysis is running':state.busy?'Wait for the active checks to finish':'';
  return `<details class="diagnostic-run ${state.job?.id===j.id?'selected':''}" data-run-record="${esc(j.id)}" ${historyPanels[j.id]?'open':''}><summary><div class="diagnostic-run-main"><div class="diagnostic-run-title"><b>${esc(runName(q))}</b>${state.job?.id===j.id?'<span class="run-selected-label">Viewing</span>':''}<span class="run-execution">${esc(j.status)}</span>${j.summary?`<span class="${bad?'run-abnormal':''}">${bad} abnormal checks</span>`:''}</div><p>${esc(j.target_label||q.target||'Client / Server checks')} · ${esc(stamp(j.created_at))}</p></div><span class="run-record-count">${versions.length} analysis reports</span><span class="run-expand">⌄</span></summary><div class="diagnostic-run-expanded"><p class="run-interval">Incident: ${esc(stamp(q.from))} → ${esc(stamp(q.to))}</p><div class="run-record-summary">${j.summary?`<span class="${bad?'run-abnormal':''}">${bad} abnormal checks</span><span>${failed} collection failures</span>`:'<span>Collecting source evidence…</span>'}<span>${versions.length} analysis report${versions.length===1?'':'s'}${active?' · analysis running':''}</span></div><div class="diagnostic-run-actions"><button data-history="${esc(j.id)}" ${state.restoring||state.busy&&state.job?.id!==j.id?'disabled':''}>${j.report_ready?'View result':'View progress'}</button><button class="text-link" data-reuse-inputs="${esc(j.id)}" ${state.busy||state.restoring?'disabled':''}>Reuse inputs</button>${versions.length?`<button class="text-link" data-run-analyses="${esc(j.id)}" ${state.busy||state.restoring?'disabled':''}>View analyses (${versions.length}) ↗</button>`:''}<button class="text-link" data-run-analyze="${esc(j.id)}" ${canAnalyze?'':'disabled'} title="${esc(reason)}">${latest?'Continue analysis':'Analyze findings'} ↗</button></div></div></details>`;
 }).join('')||'<p class="run-history-empty">No diagnostic runs yet. Run checks to create a record.</p>';
 for(const record of document.querySelectorAll('[data-run-record]'))record.querySelector('summary').onclick=()=>{historyPanels[record.dataset.runRecord]=!record.open;};
}
async function history(){await loadRunHistory();$('#diagnostic-history').open=true;$('#diagnostic-history').scrollIntoView({behavior:'smooth',block:'start'});}
function renderAnalysis(){
 const versions=runAnalyses(state.job?.id),previous=state.analysis?.status==='completed'&&!!state.analysis.text;
 $('#analysis-context-note').textContent=previous?'Uses the original evidence and the selected analysis as context.':'Uses the selected run’s evidence. No new data is collected.';
 $('#analyze').textContent=previous?'Continue analysis':'Analyze findings';
 $('#analysis-question').disabled=!state.report||!state.info.agent.ready||!!state.analysisJob;
 const panel=$('#investigation');panel.hidden=!versions.length&&!state.analysis&&!state.analysisJob;
 if(panel.hidden){panel.innerHTML='';delete panel.dataset.content;return;}
 const html=`<div class="panel-head"><div class="panel-title">Analysis reports</div><span class="field-hint">${versions.length} retained</span></div><div class="analysis-report-toolbar"><label for="analysis-version">Report</label><select id="analysis-version" ${state.analysisJob||state.restoring?'disabled':''}><option value="">Choose an analysis</option>${versions.map(a=>`<option value="${esc(a.id)}" ${state.analysisID===a.id?'selected':''}>${esc(stamp(a.created_at))} · ${esc(a.status)}</option>`).join('')}</select><button class="text-link" id="download-analysis" ${state.analysisID&&state.analysis?'':'disabled'}>Download JSON</button></div>${state.analysisJob?'<div class="analysis-progress" role="status"><i class="spinner"></i> Analysis running. Previous reports remain available.</div>':''}${state.analysis?`<div class="result-content agent-answer"><div class="analysis-report-meta"><span>${esc(state.analysis.provider||'Agent')} · ${esc(state.analysis.status)}</span><span>${esc(stamp(state.analysis.finished_at))}</span></div>${state.analysis.question?`<p class="analysis-question"><b>Question:</b> ${esc(state.analysis.question)}</p>`:''}${state.analysis.previous_analysis?'<p class="analysis-context">Context: original evidence + a previous analysis. Previous conclusions are not new evidence.</p>':''}${state.analysis.text?formatAgentAnswer(state.analysis.text):`<p>${esc(state.analysis.code||state.analysis.status)}</p>`}<p class="proof-note">Interpretation of supplied evidence. Follow-up suggestions have not been executed.</p></div>`:''}`;
 // Polls should not replace identical content or disrupt the selected control.
 if(panel.dataset.content!==html){panel.innerHTML=html;panel.dataset.content=html;}
 $('#analysis-version').onchange=()=>viewAnalysis($('#analysis-version').value).catch(error);
 $('#download-analysis').onclick=()=>downloadAnalysis().catch(error);
}
async function viewAnalysis(id){
 if(!id||state.analysisJob)return;
 const selection=++analysisSelectionRevision,revision=state.rev,run=state.job?.id;
 const entry=runAnalyses(run).find(j=>j.id===id);if(!entry)return;
 if(!entry.report_ready){state.analysisJob=entry;render();await watch(id,revision,true);return;}
 const result=await api('/api/runs/'+id+'/report');if(selection!==analysisSelectionRevision||revision!==state.rev||run!==state.job?.id)return;
 state.analysis=result;state.analysisID=id;$('#analysis-question').value='';render();
}
async function downloadAnalysis(){
 const id=state.analysisID;if(!id)return;
 const response=await fetch('/api/runs/'+id+'/report');if(!response.ok)throw Error('Analysis expired.');
 download(await response.text(),'wp-analysis-'+id+'.json');
}
async function analyze(){
 if(!state.report||state.analysisJob||$('#analyze').disabled)return;
 const run=state.job.id,rev=state.rev,q={question:$('#analysis-question').value.trim()};
 if(state.analysisID&&state.analysis?.status==='completed')q.previous_analysis_id=state.analysisID;
 $('#analyze').disabled=true;
 try{const job=await api('/api/diag/runs/'+run+'/analyze',q);if(rev!==state.rev)return;state.analysisJob=job;$('#analysis-question').value='';await loadRunHistory();render();await watch(job.id,rev,true);}
 finally{if(rev===state.rev)render();}
}
async function restore(id,section='result',reuse=false){
 if(state.busy&&state.job?.id!==id)throw Error('Stop or wait for the active checks before opening a different run.');
 const rev=++state.rev;state.restoring=true;render();
 try{
 const job=await api('/api/runs/'+id);if(rev!==state.rev)return;
 if(job.kind!=='diag')throw Error('Select a diagnostic run.');
 const q=job.request||{};
 if(reuse&&q.bindings)restoreDiagnosticBindings(q.bindings,q);
 state.resultOrigin=job.report_ready?'saved':'current';state.job=job;state.report=null;state.analysis=null;state.analysisID='';state.analysisJob=null;state.tab='diagnostics';state.filter='all';$('#analysis-question').value='';
 if(reuse){
 if(q.scenario){state.scenario=q.scenario;$('#mode').value='scenario';}else if(q.source){$('#mode').value=q.source;populateAtomic();$('#atomic-check').value=q.check;}
 if(q.from&&q.to){$('#period').value='custom';$('#from').value=inputTime(new Date(q.from));$('#to').value=inputTime(new Date(q.to));}
 }
 renderConnections();
 if(!job.report_ready){state.busy=true;state.restoring=false;render();await loadRunHistory();await watch(id,rev);return;}
 const result=await api('/api/runs/'+id+'/report');if(rev!==state.rev)return;
 state.report=result;state.busy=false;await loadRunHistory();if(rev!==state.rev)return;
 render();
 const versions=runAnalyses(id),latest=latestCompletedAnalysis(id)||versions.find(a=>a.report_ready),active=versions.find(a=>!a.report_ready);
 if(latest)await viewAnalysis(latest.id);
 if(rev!==state.rev)return;
 if(active){state.analysisJob=active;render();watch(active.id,rev,true).catch(error);}
 if(section==='analysis'&&versions.length)$('#investigation').scrollIntoView({behavior:'smooth',block:'start'});
 else if(section==='analyze')$('#analysis-question').focus();
 else $('#result-banner').scrollIntoView({behavior:'smooth',block:'start'});
 }finally{if(rev===state.rev){state.restoring=false;render();}}
}
$('#refresh-run-history').onclick=()=>loadRunHistory();
$('#diagnostic-history').ontoggle=()=>{if($('#diagnostic-history').open)loadRunHistory();};
document.addEventListener('click',e=>{
 const reuse=e.target.closest('[data-reuse-inputs]');if(reuse&&!reuse.disabled){restore(reuse.dataset.reuseInputs,'result',true).catch(error);return;}
 const old=e.target.closest('[data-history]');if(old){restore(old.dataset.history).catch(error);return;}
 const analyses=e.target.closest('[data-run-analyses]');if(analyses){restore(analyses.dataset.runAnalyses,'analysis').catch(error);return;}
 const analyzeRun=e.target.closest('[data-run-analyze]');if(analyzeRun&&!analyzeRun.disabled){restore(analyzeRun.dataset.runAnalyze,'analyze').catch(error);}
});
setInterval(()=>{if(state.tab==='diagnostics'&&!document.hidden&&!historyLoading&&state.jobs.some(j=>['diag','analysis'].includes(j.kind)&&!j.report_ready))loadRunHistory();},3000);
