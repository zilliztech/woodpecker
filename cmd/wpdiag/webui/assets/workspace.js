/* Presentation only. Collection, formulas, assessment and summaries belong to Go. */
'use strict';
const $=s=>document.querySelector(s);
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const state={info:null,catalog:null,scenario:'slow-writes',tab:'overview',report:null,job:null,busy:false,analysis:null,analysisJob:null,filter:'all',rev:0,clusterRev:0,metaRev:0,jobs:[],analysisID:'',historyError:'',restoring:false,resultOrigin:'current',tool:'',commands:null,commandCategory:'all',commandSearch:'',runbook:'write-stall',reviewed:{},runbookSource:null,commandName:'',commandEditorKey:'',commandRuns:{},commandSwitching:false};
const names={clear:'Within rule',abnormal:'Abnormal',unknown:'Inconclusive',context:'Context',pending:'Not checked'};
function workspaceIcon(name){
 const paths={overview:'<rect x="3" y="3" width="6" height="6" rx="1"/><rect x="13" y="3" width="6" height="6" rx="1"/><rect x="3" y="13" width="6" height="6" rx="1"/><rect x="13" y="13" width="6" height="6" rx="1"/>',diagnostics:'<path d="M2 11h4l3-7 4 14 3-7h4"/>',runbooks:'<path d="M13.7 5.3a1 1 0 0 0 0 1.4l1.6 1.6a1 1 0 0 0 1.4 0l3.77-3.77a6 6 0 0 1-7.94 7.94l-6.91 6.91a2.12 2.12 0 0 1-3-3l6.91-6.91a6 6 0 0 1 7.94-7.94Z"/>',commands:'<rect x="2" y="3" width="18" height="16" rx="2"/><path d="m6 8 3 3-3 3m6 0h4"/>',settings:'<path d="M3 5h16M3 11h16M3 17h16"/><circle cx="8" cy="5" r="2"/><circle cx="15" cy="11" r="2"/><circle cx="8" cy="17" r="2"/>'};
 return `<svg viewBox="0 0 22 22" width="20" height="20" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name]||paths.commands}</svg>`;
}
for(const icon of document.querySelectorAll('[data-workspace-icon]'))icon.innerHTML=workspaceIcon(icon.dataset.workspaceIcon);
let dialogRevision=0,focusBeforeDialog;
async function api(path,body){const response=await fetch(path,body===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json','X-WPDiag-Session':$('meta[name="wpdiag-session"]').content},body:JSON.stringify(body)});let value;try{value=await response.json();}catch{throw Error('Invalid response from the local workspace.');}if(!response.ok)throw Error(value.error?.message||value.message||`Request failed (${response.status})`);return value;}
function stamp(v){const d=new Date(v);if(!Number.isFinite(+d))return '—';return $('#zone').value==='utc'?d.toISOString().replace('T',' ').replace('Z',' UTC'):d.toLocaleString();}
function inputTime(d){if($('#zone').value==='utc')return d.toISOString().slice(0,19);const x=new Date(+d-d.getTimezoneOffset()*60000);return x.toISOString().slice(0,19);}
function parsedInput(id){const value=$('#'+id).value;if(!value)return null;return new Date(value+($('#zone').value==='utc'?'Z':''));}
function timeSelection(){let to=new Date(Date.now()-1000),from;const value=$('#period').value;if(value==='custom'){from=parsedInput('from');to=parsedInput('to');}else{const minutes=value==='lookback'?Number($('#lookback').value):Number(value);if(value==='lookback'&&$('#anchor').value)to=parsedInput('anchor');from=new Date(+to-minutes*60000);}if(!from||!to||!Number.isFinite(+from)||!Number.isFinite(+to)||from>=to||to>Date.now())throw Error('Choose a valid past interval with From before To.');return {from:from.toISOString(),to:to.toISOString()};}
function target(){if(!state.info)return null;const b=initialBindings();return {label:diagnosticBindingSummary(),metrics:b.client?.metrics||b.server?.metrics,trace:b.client?.trace||b.server?.trace};}
function definition(id){return state.catalog.checks.find(c=>c.id===id);}
function titleStep(id){return id.split('-').map(s=>s[0]?.toUpperCase()+s.slice(1)).join(' ');}
function evidenceDefinition(value){return typeof value==='string'?definition(value)||state.report?.checks.find(c=>c.id===value)||{id:value}:value;}
function evidenceReference(value){
 const d=evidenceDefinition(value),match=d.id?.match(/^([A-Z]+)(\d+)$/);
 const groups={W:'Writer',R:'Reader',S:'Server',B:'Maintenance',H:'Resources',OR:'Open Reader',OW:'Open Writer',M:'Metadata',TW:'Writer',TA:'Client append',TS:'Server append',TF:'Server flush',TR:'Reader',TB:'Server read',TO:'Open Reader',TP:'Open Writer'};
 if(!match)return d.title||titleStep(d.name||d.step||'check');
 return `${groups[match[1]]||titleStep(d.step||'check')} ${d.source==='trace'?'trace':match[1]==='M'?'view':'evidence'} ${match[2]}`;
}
function evidenceTitle(value){const d=evidenceDefinition(value),title=d.title||titleStep(d.name||d.step||'check');return title.charAt(0).toUpperCase()+title.slice(1);}
function evidenceLabel(value){return evidenceReference(value)+' · '+evidenceTitle(value);}
function ready(source,t){if(t){const ref=source==='metrics'?(t.metrics||t.prometheus):t.trace;return !!state.info.connections?.[source]?.some(c=>c.name===ref&&c.enabled);}return anyDiagnosticSource(source);}
function selectedDefinitions(){if(state.report)return state.report.checks.map(r=>definition(r.id)).filter(Boolean);const mode=$('#mode').value;if(mode!=='scenario')return state.catalog.checks.filter(c=>c.source===mode&&c.name===$('#atomic-check').value);const scenario=state.catalog.scenarios.find(s=>s.id===state.scenario);return state.catalog.checks.filter(c=>scenario?.checks.includes(c.id));}
function error(e){$('#request-error').hidden=false;$('#request-error').textContent=e.message||String(e);}
function reset(){state.resultOrigin='current';state.rev++;state.restoring=false;state.report=null;state.job=null;state.analysis=null;state.analysisID='';state.analysisJob=null;$('#analysis-question').value='';state.filter='all';$('#request-error').hidden=true;render();}
function dialog(title,kicker,body){dialogRevision++;focusBeforeDialog=document.activeElement;$('.drawer').classList.remove('config-source-drawer');$('#drawer-title').textContent=title;$('#drawer-kicker').textContent=kicker;$('#drawer-body').innerHTML=body;$('#overlay').hidden=false;$('.drawer').focus();}
function closeDialog(){dialogRevision++;$('#overlay').hidden=true;focusBeforeDialog?.focus();}
function targetText(labels){return ['service','operation','instance','node_id','log_ns','log_id','reader_name','path'].filter(k=>labels?.[k]!==undefined).map(k=>`${k}=${labels[k]}`).join(' · ')||'Selected target';}
function formatAgentAnswer(text){
 const known=new Set((state.report?.checks||[]).map(c=>c.id));
 const inline=value=>esc(value).replace(/`([^`]+)`/g,'<code>$1</code>').replace(/\*\*([^*]+)\*\*/g,'<strong>$1</strong>').replace(/\[([A-Z]{1,2}\d{2}(?:\s*[,/]\s*[A-Z]{1,2}\d{2})*)\]/g,(original,refs)=>refs.split(/\s*[,/]\s*/).map(id=>known.has(id)?`<button class="agent-evidence" data-check="${id}" title="${esc(evidenceLabel(id)+' · ID: '+id)}">${esc(evidenceReference(id))}</button>`:`[${id}]`).join(' '));
 let html='',list='',paragraph=[];const flush=()=>{if(paragraph.length){html+=`<p>${inline(paragraph.join(' '))}</p>`;paragraph=[];}};const close=()=>{if(list){html+=`</${list}>`;list='';}};
 for(const raw of String(text).split(/\r?\n/)){const line=raw.trim();if(!line){flush();close();continue;}const h=line.match(/^#{1,3}\s+(.+)$/);if(h){flush();close();html+=`<h3>${inline(h[1])}</h3>`;continue;}const b=line.match(/^(?:[-*]|\d+[.)])\s+(.+)$/);if(b){flush();const tag=/^\d/.test(line)?'ol':'ul';if(list!==tag){close();list=tag;html+=`<${tag}>`;}html+=`<li>${inline(b[1])}</li>`;continue;}close();paragraph.push(line);}flush();close();return html;
}
function renderOverview(){
 const card=(id,title,description,status,reason,action,enabled,attrs,setup)=>`<article class="surface module-card ${enabled?'available':'needs-setup'}" data-module="${id}"><div class="module-head"><div class="module-heading"><span class="capability-icon">${workspaceIcon(id)}</span><h2>${title}</h2></div><span class="module-status ${enabled?'configured':'missing'}">${esc(status)}</span></div><p class="module-purpose">${description}</p><p class="module-requirement">${esc(reason)}</p><div class="module-actions"><button class="${enabled?'primary':''}" ${attrs} ${enabled?'':'disabled'}>${action}</button>${setup||''}</div></article>`;
 const info=state.info;
 if(!info){$('#overview-modules').innerHTML=[['diagnostics','Diagnostics','Find problems.','Start diagnosis'],['runbooks','Operations','Confirm issues and take action.','Choose a scenario'],['commands','Command Explorer','All operations commands.','Explore commands']].map(([id,title,purpose,action])=>card(id,title,purpose,'Loading configuration','Preparing this local workspace…',action,false,'','')).join('');return;}
 const configuredSources=[...info.connections.metrics,...info.connections.trace].filter(c=>c.enabled);
 const diagnosticReady=!info.diagnostics_error&&(configuredSources.length>0||anyDiagnosticSource('metrics')||anyDiagnosticSource('trace'));
 $('#overview-modules').innerHTML=
 card('diagnostics','Diagnostics','Find problems.',diagnosticReady?'Configured':'Setup needed',diagnosticReady?'Choose a check, review its prefilled connections and scope, then select an incident interval.':info.diagnostics_error||'Configure a Prometheus or Trace connection, or enter an endpoint in Diagnostics.','Start diagnosis',diagnosticReady,'data-tab="diagnostics"',!diagnosticReady?'<button class="text-link" data-tab="diagnostics">Configure sources ↗</button>':'')+
 card('runbooks','Operations','Confirm issues and take action.','Scenario guides','Choose a write, read or segment scenario. Follow the steps to review evidence and choose the next operation.','Choose a scenario',true,'data-tab="runbooks"','')+
 card('commands','Command Explorer','All operations commands.','Browse without setup',`${state.commands?.commands.length||'Available'} CLI operations, grouped by topic. Set shared parameters, run a command and inspect the result.`,'Explore commands',true,'data-tab="commands"','');

}
async function refresh(){const [info,catalog,jobs,commands]=await Promise.all([api('/api/workspace'),api('/api/diag/catalog').catch(e=>({checks:[],scenarios:[],configuration_error:e.message})),api('/api/runs'),api('/commands.json')]);if(catalog.configuration_error)info.diagnostics_error=catalog.configuration_error;state.info=info;state.catalog=catalog;state.commands=commands;state.jobs=jobs;renderConnections();render();loadAgentStatus(info);}
function loadAgentStatus(info){
 if(info.agent?.code!=='agent_status_pending')return;
 api('/api/workspace/agent').then(agent=>{if(state.info!==info)return;info.agent=agent;render();}).catch(()=>{if(state.info!==info)return;info.agent={...info.agent,ready:false,code:'agent_status_unavailable',message:'Agent login check failed. Reload to retry; diagnosis remains available.'};render();});
}
function choice(selectID, nameID, rows, current, empty){
 const select=$('#'+selectID),name=$('#'+nameID);
 if(!rows.some(x=>x.id===current))current='';
 select.innerHTML=(rows.length>1&&!current?'<option value="">Choose a connection</option>':'')+rows.map(x=>`<option value="${esc(x.id)}">${esc(x.label)}</option>`).join('');
 select.value=current|| (rows.length===1?rows[0].id:'');
 select.hidden=rows.length<=1;name.hidden=rows.length>1;name.textContent=rows.length===1?rows[0].label:empty;
}
function metadataControls(){
 const connected=!!state.info.session.metadata,log=$('#meta-log').value.trim();
 $('#meta-log').disabled=!connected;
 for(const button of document.querySelectorAll('[data-meta]')){
  button.disabled=!connected||(button.dataset.meta!=='M03'&&!log);
  button.title=!connected?'Connect to client etcd first':button.dataset.meta!=='M03'&&!log?'Enter a log name':'';
 }
}
function renderConnections(){
 const info=state.info;
 choice('wp-context','wp-context-name',info.contexts.map(c=>({id:c,label:c})),info.session.context,'No WP Admin context configured');
 renderDiagnosticBindings();
 $('#manual-meta').open=true;
 const meta=info.session.metadata;
 $('#metadata-attachment').textContent=meta?`Selected namespace (not connectivity-tested): ${meta.etcd} · ${meta.prefix}`:'No client metadata connection';
 $('#disconnect-meta').disabled=!meta;metadataControls();
 $('#metadata-history-notice').hidden=!info.metadata_history_error;$('#metadata-history-notice').textContent=info.metadata_history_error||'';
 $('#metadata-history-location').textContent='Temporary input candidates: '+info.candidate_path;
 $('#refresh-cluster').disabled=!(info.available_contexts||info.contexts).includes(info.session.context);
 $('#cluster-requirement').textContent=!$('#refresh-cluster').disabled?'Uses this WP Admin context. Metrics, Trace and client metadata are not required.':'Add a contexts entry in cli.yaml, then restart the workspace to read WP service state.';
 $('#agent-status').textContent=info.agent.ready?'Ready':info.agent.enabled?'Unavailable':'Off';
 $('#agent-description').textContent=info.agent.message;
 $('#config-path').textContent='Configuration: '+info.config_path;
 const group=(title,items)=>`<div class="connection-group"><h3>${title}</h3>${items.length?items.map(x=>`<p>${esc(x)}</p>`).join(''):'<p class="muted">Not configured</p>'}</div>`;
 $('#connection-inventory').innerHTML=group('WP Admin contexts',info.contexts)+group('Metrics',info.connections.metrics.map(c=>`${c.name} · ${c.enabled?'enabled':'disabled'}`))+group('Trace',info.connections.trace.map(c=>`${c.name} · ${c.enabled?'enabled':'disabled'}`));
 $('#dashboard-links').innerHTML=Object.entries(info.links||{}).map(([name,url])=>`<p><a href="${esc(url)}" target="_blank" rel="noopener noreferrer">Open ${esc(name)} ↗</a></p>`).join('');
}
function render(){
 if(!state.catalog||!state.info)return;
 renderOverview();
 for(const view of ['overview','diagnostics','runbooks','commands','settings'])$('#'+view+'-view').hidden=state.tab!==view;
 for(const button of document.querySelectorAll('[data-tab]'))button.classList.toggle('active',button.dataset.tab===state.tab);
 $('#workspace-page').textContent=state.tab==='commands'&&state.tool?(state.tool==='cluster'?'Cluster & nodes':'Metadata'):({overview:'Overview',diagnostics:'Diagnostics',runbooks:'Operations',commands:'Command Explorer',settings:'Settings'}[state.tab]||'Overview');
 $('#cluster-view').hidden=state.tab!=='commands'||state.tool!=='cluster';
 $('#metadata-view').hidden=state.tab!=='commands'||state.tool!=='metadata';
 $('#command-catalogue').hidden=!!state.tool;$('#command-return').hidden=!state.tool;$('#visual-view-name').textContent=state.tool==='cluster'?'Cluster & nodes':'Client metadata';
 renderOperations();
 const mode=$('#mode').value;
 $('#scenario').innerHTML=state.catalog.scenarios.map(s=>`<option value="${esc(s.id)}">${esc(s.title)}</option>`).join('');
 $('#scenario').value=state.scenario;
 const selectedCheck=state.catalog.checks.find(c=>c.source===mode&&c.name===$('#atomic-check').value);$('#atomic-check-description').textContent=selectedCheck?evidenceTitle(selectedCheck):'';
 $('#scenario-field').hidden=mode!=='scenario';$('#atomic-field').hidden=mode==='scenario';
 $('#custom-range').hidden=$('#period').value!=='custom';$('#lookback-range').hidden=$('#period').value!=='lookback';
 const t=target();
 renderDiagnosticBindings();
 $('#source-summary').innerHTML=['metrics','trace'].map(source=>`<span><i class="source-dot ${ready(source)?'':'off'}"></i>${source==='metrics'?'Metrics':'Trace'}: ${ready(source)?'Configured':'Not configured'}</span>`).join(' · ');
 updateDiagnosticValidation();
 $('#run').disabled=state.restoring||state.commandSwitching;$('#run').innerHTML=state.busy?'<i class="spinner"></i> Stop checks':state.report?'↻ Run checks':'▶ Run checks';
 for(const id of ['mode','scenario','atomic-check','period','zone','from','to','lookback','anchor'])$('#'+id).disabled=state.busy;
 for(const control of document.querySelectorAll('#diagnostic-components input,#diagnostic-components select,#diagnostic-observability input,#diagnostic-observability select,#diagnostic-observability textarea'))control.disabled=state.busy;
 $('#analysis-requirement').textContent=state.info.agent.ready?'Optional model call using this result.':state.info.agent.code==='agent_status_pending'?'Checking local Agent login…':state.info.agent.enabled?'Agent is unavailable. Checks and report export remain available.':'Agent is not configured. Checks and report export remain available.';
 const r=state.report,results=r?.checks||[],bad=results.filter(c=>c.status==='abnormal').length,gaps=results.filter(c=>c.status==='unknown'||c.execution!=='completed').length;
 const banner=$('#result-banner');banner.className='result-banner '+(state.busy?'ready':!r?'ready':bad?'warn':gaps||r.status!=='completed'?'unknown':'clear-state');
 const sourceCard=(name,v)=>{
 if(!v.selected)return `<div class="source-summary-card"><b>${name}: Not selected ${helpIcon('not_selected')}</b><p>This atomic check does not query this source.</p></div>`;
 const key=!v.configured?'not_configured':v.failed?'incomplete':'completed',label=!v.configured?'Not configured — skipped':v.failed?'Collection incomplete':'Checks completed';
 return `<div class="source-summary-card ${v.abnormal||v.failed?'hot':''}"><b>${name}: ${label} ${helpIcon(key)}</b>${v.configured?`<p class="summary-counts">${v.checks} ${v.checks===1?'check':'checks'} · <span class="summary-term"><strong>${v.abnormal} abnormal</strong>${helpIcon('abnormal')}</span> · <span class="summary-term">${v.inconclusive} unassessed${helpIcon('unassessed')}</span> · <span class="summary-term">${v.failed} failed${helpIcon('failed')}</span> · <span class="summary-term">${v.skipped} skipped${helpIcon('skipped')}</span></p>${name==='Trace'?`<p>${v.failed&&!v.matched_traces?'Trace count unavailable':`${v.matched_traces||0} ${results.some(c=>c.source==='trace'&&c.selection_mode?.endsWith('_candidates'))?'matching candidate traces':'matched traces'} · ${v.traces_with_abnormal_spans||0} with abnormal spans`}</p>`:''}`:''}</div>`;
 };

 banner.innerHTML=`<div class="panel-head"><div class="panel-title">${state.busy?'Collecting source evidence':r?'Check summary':'Ready to inspect'}</div><span>${esc(r?.status||'No model call')}</span></div><div class="result-content"><h2>${state.busy?'Checks are running…':!r?'Choose Client / Server sources and an incident window':`${bad} abnormal checks · ${gaps} evidence gaps`}</h2>${r?`<div class="source-summary-grid">${sourceCard('Metrics',r.summary.metrics)}${sourceCard('Trace',r.summary.trace)}</div><p>${esc(r.target.label)} · ${esc(stamp(r.request.from))} → ${esc(stamp(r.request.to))}</p>${r.problems.map(p=>`<div class="error-box">${esc(p.code+': '+p.message)} ${helpIcon(helpKeyForCheck({code:p.code,execution:'failed'}),p)}</div>`).join('')}${r.coverage_gaps.map(g=>`<p class="small muted">${esc(g)}</p>`).join('')}`:`<p>${state.busy?'Each check retains its own evidence and verdict. Stop to keep partial results.':'A scenario runs its Metrics and Trace checks and summarizes the observed findings. At least one source must be configured.'}</p>`}</div>`;
 renderDiagnosticPlan();renderDiagnosticDetails();renderResultOrigin();
 $('#diagnostic-result').hidden=!r;
 for(const id of ['export-report','packet'])$('#'+id).disabled=!r||state.busy;
 $('#analyze').disabled=state.restoring||!r||!bad||!state.info.agent.ready||!!state.analysisJob;
 $('#analyze').title=!state.info.agent.ready?'Configure an available Agent in Settings before analyzing.':'';
 $('#cancel-analysis').hidden=!state.analysisJob;
 renderRunHistory();renderAnalysis();
}
function populateAtomic(){const mode=$('#mode').value,old=$('#atomic-check').value;$('#atomic-check').innerHTML=state.catalog.checks.filter(c=>c.source===mode).map(c=>`<option value="${esc(c.name)}">${esc(c.name)}</option>`).join('');if([...$('#atomic-check').options].some(o=>o.value===old))$('#atomic-check').value=old;}
async function select(body){
 if(body.action==='use-target'&&state.busy)return;
 const oldMetadata=state.info.session.metadata;
 state.info=await api('/api/workspace/session',body);
 const newMetadata=state.info.session.metadata;
 if(oldMetadata?.etcd!==newMetadata?.etcd||oldMetadata?.prefix!==newMetadata?.prefix)$('#meta-log').value=newMetadata?.log_name||'';
 if(body.action==='use-context'){adoptSharedContext(body.context);state.commandEditorKey='';state.clusterRev++;$('#cluster-result').textContent='Read cluster state to fetch its current memberlist.';}
 else if(body.action==='use-target')reset();
 else{if(body.action==='connect-meta')adoptSharedMetadata(state.info.session.metadata);state.metaRev++;$('#metadata-result').textContent=state.info.session.metadata?'Choose a metadata view.':'Connect to client etcd to read its current metadata.';}
 renderConnections();render();loadAgentStatus(state.info);
}
async function watch(id,rev,analysis=false){for(;;){const job=await api('/api/runs/'+id);if(rev!==state.rev)return;if(analysis)state.analysisJob=job;else state.job=job;if(job.report_ready){render();const result=await api('/api/runs/'+id+'/report');if(rev!==state.rev)return;if(analysis){state.analysis=result;state.analysisID=id;state.analysisJob=null;}else{rememberEvidenceConnections(result);state.report=result;state.busy=false;state.filter=result.checks.some(c=>c.status==='abnormal')?'abnormal':'all';}await loadRunHistory();render();return;}render();await new Promise(r=>setTimeout(r,500));}}
async function run(atomic){if(state.busy){if(state.job)await api('/api/runs/'+state.job.id+'/cancel',{});return;}if(state.restoring||state.commandSwitching)return;if(!validateDiagnosticInputs(atomic?[atomic]:configuredDefinitions()))return;const mode=$('#mode').value,q={bindings:diagnosticRequestBindings(),...diagnosticTraceParameters(),...timeSelection(),details:'all'};if(atomic){q.source=atomic.source;q.check=atomic.name;}else if(mode==='scenario')q.scenario=state.scenario;else{q.source=mode;q.check=$('#atomic-check').value;}state.resultOrigin='current';state.rev++;const rev=state.rev;state.report=null;state.job=null;state.analysis=null;state.analysisID='';state.analysisJob=null;$('#analysis-question').value='';state.busy=true;state.filter='all';$('#request-error').hidden=true;render();try{state.catalog=await api('/api/diag/catalog');state.job=await api('/api/diag/runs',q);render();await loadRunHistory();render();await watch(state.job.id,rev);}catch(e){state.busy=false;render();throw e;}}
async function showEvidence(id){const def=definition(id);if(!def)return;const row=state.report?.checks.find(c=>c.id===id);dialog(evidenceLabel(def),`${def.source.toUpperCase()} · Key: ${def.name}`,'');const revision=dialogRevision;renderInspection(def,{...def.description,status:'pending',status_label:'Not checked',value:'—',value_label:'Result',conclusion:'Run this check to collect source evidence.',measurements:[],anomalies:[],curves:[],observation_ids:[]},row);if(!row)return;try{const detail=await api(`/api/diag/runs/${state.job.id}/inspect/${id}`);if(revision===dialogRevision&&!$('#overlay').hidden)renderInspection(def,detail,row);}catch(e){if(revision===dialogRevision)$('#drawer-body').textContent=e.message;}}
function renderInspection(def,x,row){
 const assessment=row?.assessment||def.assessment;
 $('#drawer-body').innerHTML=`<div class="inspection-actions"><button class="primary" id="run-single-check" ${state.busy||missingForCheck(def).length>0?'disabled':''}>▶ Run this check</button><p>Uses this component’s sources, scope and incident window. No other checks are triggered.</p></div><section class="inspection-section"><h3>${def.source==='metrics'?'Metric':'Span operation'} / source</h3><div class="inspection-metrics">${x.metrics.map(m=>`<code>${esc(m)}</code>`).join('')}</div><p>${esc(x.meaning)}</p></section><section class="inspection-section"><h3>Calculation</h3><p class="inspection-rule-key">Evidence key: <code>${esc(row?.name||def.name)}</code></p><p class="inspection-formula">${esc(x.formula)}</p><p class="inspection-criterion">${esc(x.criterion)}</p>${assessment?`<p class="field-hint">${esc(assessment.mode)}${assessment.threshold!==undefined?` · ${esc(assessment.threshold)} ${esc(assessment.unit)}`:''}<br>Rule source: <code>${esc(assessment.configured_by)}</code></p>`:''}</section><section id="inspection-result" class="inspection-result is-${esc(x.status)}"><div class="inspection-result-top"><h3>Result</h3><span class="status ${esc(x.status)}">${esc(x.status_label)} ${helpIcon(helpKeyForCheck(row||x),row||x)}</span></div><span class="inspection-value-label">${esc(x.value_label)}</span><strong>${esc(x.value)}</strong><p>${esc(x.conclusion)}</p></section>${row?.identity_scope==='service'?'<p class="scope-message">Service-scoped Trace evidence. The selected WP log/node identity is not fully mapped; this is not automatic Metrics/Trace correlation.</p>':''}<section class="inspection-section"><h3>Time series / retained spans</h3><div id="inspection-series"></div></section><section class="inspection-section"><h3>Abnormal data</h3><div id="inspection-anomalies"></div></section><details class="inspection-source"><summary>Executed queries, scope and original evidence</summary>${(row?.queries||[]).map(q=>`<pre class="report">${esc(q.expression)}</pre>`).join('')}${(row?.coverage_notes||[]).map(n=>`<p>${esc(n)}</p>`).join('')}${x.observation_ids.map(id=>`<button class="observation-link" data-observation="${esc(id)}">${esc(id)} ↗</button>`).join('')}</details>`;
 $('#run-single-check').onclick=()=>{closeDialog();run(def).catch(error);};
 const rows=x.anomalies||[];let page=0;const size=5;
 const show=()=>{const part=rows.slice(page*size,(page+1)*size);$('#inspection-anomalies').innerHTML=part.length?`<table class="span-table"><thead><tr><th>Time / target</th><th>Observed</th></tr></thead><tbody>${part.map(a=>`<tr><td>${a.from_ms?esc(stamp(a.from_ms))+'<br>→ '+esc(stamp(a.to_ms)):''}<div class="small muted">${esc(targetText(a.labels))}</div></td><td><b>${esc(a.value)}</b><p>${esc(a.detail||'')}</p></td></tr>`).join('')}</tbody></table>${rows.length>size?`<button id="previous" ${page?'':'disabled'}>Previous</button> ${page*size+1}–${Math.min((page+1)*size,rows.length)} / ${rows.length} <button id="next" ${(page+1)*size<rows.length?'':'disabled'}>Next</button>`:''}`:'<p class="inspection-empty">No recorded abnormal samples. Review the result and data availability above.</p>';if($('#previous'))$('#previous').onclick=()=>{page--;show();};if($('#next'))$('#next').onclick=()=>{page++;show();};};show();
 if(x.spans?.length){$('#inspection-series').innerHTML=`<p class="small muted">${x.spans.length} ${row?.selection_mode==='duration_candidates'?'matching slow-span candidates':row?.selection_mode==='errors_candidates'?'matching error candidates':'matching sampled spans'}; span start lies inside the incident window.</p><table class="span-table"><thead><tr><th>Operation / time</th><th>Duration</th><th>Trace</th></tr></thead><tbody>${x.spans.slice(0,100).map(s=>`<tr><td>${esc(s.operation)}<br>${esc(stamp(s.from))}</td><td>${esc(s.duration_ms)} ms${s.error?' · error flagged':''}${s.abnormal?' · reference matched':''}</td><td>${esc(s.trace_id)}</td></tr>`).join('')}</tbody></table>${x.spans.length>100?'<p>Showing the first 100 spans. Download JSON for all retained spans.</p>':''}`;}
 else if(x.curves?.length){$('#inspection-series').innerHTML=`<select id="inspection-curve">${x.curves.map((c,i)=>`<option value="${i}">${esc(c.metric+' · '+targetText(c.labels))}</option>`).join('')}</select><div id="inspection-chart"></div>`;const plot=()=>renderMetricCurve(x.curves[Number($('#inspection-curve').value)]);$('#inspection-curve').onchange=plot;plot();}
 else $('#inspection-series').innerHTML='<p class="inspection-empty">No matching measurements available to plot.</p>';
}
function download(body,name){const link=document.createElement('a'),url=URL.createObjectURL(new Blob([body],{type:'application/json'}));link.href=url;link.download=name;link.click();setTimeout(()=>URL.revokeObjectURL(url),2000);}
async function exportReport(){const response=await fetch('/api/runs/'+state.job.id+'/report');if(!response.ok)throw Error('Report expired.');download(await response.text(),'wp-diag-'+state.job.id+'.json');}
async function packet(){const result=await api('/api/diag/runs/'+state.job.id+'/packet');dialog('Agent input','SUPPLIED FINDINGS ONLY',`<p>Only this compact packet is sent by optional analysis. Source collection is already complete.</p><pre class="report">${esc(JSON.stringify(result,null,2))}</pre>`);}
function sources(section){
 const path=`<p>Configuration: <code>${esc(state.info.config_path)}</code></p>`;
 const snippets={cluster:'contexts:\n  production:\n    endpoint: http://your-wp-admin:9091',metadata:'Etcd endpoints: http://your-client-etcd:2379\nFull metadata prefix: your-client/woodpecker\nLog name: required only for log-specific views',diagnostics:'contexts:\n  production:\n    observability:\n      prometheus:\n        endpoint: http://your-prometheus:9090\n        match_labels: {namespace: your-namespace}'};
 const notes={cluster:'This page only uses the selected WP Admin context. A failed diagnostic source does not block it.',metadata:'Enter etcd endpoints and full prefix explicitly. Each field suggests independent successful values. The WP Admin context is not used.',diagnostics:'Server context owns the Admin entry point, host map and observability links. Log scope selects the metric population. Submitted form values take precedence. Retained checks do not query client etcd or WP Admin. Saved sources keep their authentication settings; manual endpoints are used only for this local selection.'};
 dialog(section==='cluster'?'WP Admin connection':section==='metadata'?'Client metadata connection':'Diagnostic connections','PAGE SETUP',path+`<p>${esc(notes[section])}</p><pre class="report">${esc(snippets[section])}</pre><p>Edit cli.yaml and restart to update saved definitions. Direct metadata entry is used only in this browser session.</p>`);
}
function configFileLocation(path){return `<section class="config-file-location"><span>FILE LOCATION</span><code>${esc(path)}</code></section>`;}
function renderConfigFile(path,content,note,extraClass=''){
 const lines=content?content.split('\n').length-(content.endsWith('\n')?1:0):0;
 $('#drawer-body').innerHTML=configFileLocation(path)+`<p class="field-hint config-file-note">${esc(note)}</p><pre class="config-source-yaml ${extraClass}" tabindex="0" aria-label="Complete file contents"></pre><p class="config-file-size">${lines.toLocaleString()} lines · ${new TextEncoder().encode(content).length.toLocaleString()} bytes · Full file</p>`;
 // textContent keeps source characters/newlines intact, without HTML parsing.
 $('#drawer-body .config-source-yaml').textContent=content;
}
async function viewConfig(){
 dialog('cli.yaml','ENVIRONMENT CONFIGURATION','<p>Loading configuration…</p>');$('.drawer').classList.add('config-source-drawer');
 const revision=dialogRevision;
 try{const value=await api('/api/workspace/config');if(revision!==dialogRevision)return;
 renderConfigFile(value.path,value.yaml,'Current file on disk · read-only. Restart wp webui to apply file changes to the active runtime configuration.');
 }catch(e){if(revision===dialogRevision)$('#drawer-body').textContent=e.message;}
}
async function viewRules(source){
 dialog(source==='metrics'?'Metrics rules':'Trace rules','EVIDENCE ASSESSMENT','<p>Loading rules…</p>');$('.drawer').classList.add('config-source-drawer');const revision=dialogRevision;
 try{const value=await api('/api/workspace/rules/'+source);if(revision!==dialogRevision)return;
 renderConfigFile(value.path,value.yaml,value.exists?'Current rule file · read-only. Changes apply to the next run. Saved results retain their original rules.':'File does not exist · showing the built-in template. Save it at this path to customize the next run.');
 }catch(e){if(revision===dialogRevision)$('#drawer-body').textContent=e.message;}
}
async function viewRecentConfig(){
 dialog('Recent input config','TEMPORARY INPUT CANDIDATES','<p>Loading recent input candidates…</p>');$('.drawer').classList.add('config-source-drawer');
 const revision=dialogRevision;
 try{const value=await api('/api/workspace/candidates/config');if(revision!==dialogRevision)return;
 renderConfigFile(value.path,value.content,'Each key lists up to 5 successful values, newest first. Fields are independent; these values do not change cli.yaml.','recent-config-json');
 }catch(e){if(revision===dialogRevision)$('#drawer-body').textContent=e.message;}
}
document.addEventListener('click',e=>{if(e.target.closest('[data-view-config]'))viewConfig();else if(e.target.closest('[data-view-recent-config]'))viewRecentConfig();else if(e.target.closest('[data-view-rules]'))viewRules(e.target.closest('[data-view-rules]').dataset.viewRules);});
function agentSetup(){dialog('Optional Agent','SUPPLIED EVIDENCE ANALYSIS',`<p>${esc(state.info.agent.message)}</p><p>The Agent is independent of ordinary checks and scenario execution.</p><pre class="report">agent:\n  enabled: true\n  provider: codex\n  auth: cli\n  executable: codex\n  timeout_ms: 120000</pre><p>Put this under contexts.&lt;name&gt;.agent in cli.yaml. Use the login of the OS user running wp webui. Analysis only interprets supplied evidence; it cannot query systems or execute repairs.</p>`);}
function catalog(){dialog('All evidence','METRICS & TRACE CHECKS',state.catalog.checks.map(c=>`<button class="job-row" data-check="${esc(c.id)}"><b>${esc(evidenceLabel(c))}</b><br>${esc(c.source+' · '+titleStep(c.step))}<br><code class="evidence-key">${esc(c.name)}</code></button>`).join(''));}
async function cluster(){const rev=++state.clusterRev;$('#cluster-result').textContent='Reading Admin memberlist…';try{const result=await api('/api/ops/cluster');if(rev!==state.clusterRev)return;$('#cluster-result').innerHTML=`<p>${esc(result.context)} · ${esc(stamp(result.observed_at))}</p><table><thead><tr><th>Node</th><th>Service address</th><th>State</th><th>Region / AZ</th></tr></thead><tbody>${result.memberlist.members.map(m=>`<tr><td>${esc(m.id)}</td><td>${esc(m.service_addr)}</td><td>${esc(m.state)}</td><td>${esc(m.region+' / '+m.az)}</td></tr>`).join('')}</tbody></table>`;}catch(e){if(rev===state.clusterRev)$('#cluster-result').textContent=e.message;}}
async function meta(id){const rev=++state.metaRev;$('#metadata-result').textContent='Reading current metadata…';try{const result=await api('/api/ops/metadata/'+id+'?log_name='+encodeURIComponent($('#meta-log').value.trim()));if(rev!==state.metaRev)return;const data=result.data;let view='';
 if(data?.state){view=`<h2>${data.state==='ok'?'Existing consistency checks passed':`Existing check: ${esc(data.state)}`}</h2>${Array.isArray(data.issues)?`<p>${data.issues.length} reported violations</p><ul>${data.issues.slice(0,50).map(x=>`<li>${esc(x)}</li>`).join('')}</ul>`:''}`;}
 else if(Array.isArray(data?.records)){view=`<h2>${data.records.length} records</h2><p>Snapshot revision: ${esc(data.snapshot_revision)}</p><table><thead><tr><th>Kind</th><th>Key</th><th>Revision</th></tr></thead><tbody>${data.records.slice(0,100).map(x=>`<tr><td>${esc(x.kind)}</td><td>${esc(x.key)}</td><td>${esc(x.revision)}</td></tr>`).join('')}</tbody></table>${data.records.length>100?'<p>First 100 records shown; full JSON is below.</p>':''}`;}
 else if(data?.kind){view=`<h2>${esc(data.kind)} snapshot</h2><p>${esc(data.key)}</p><p>Revision: ${esc(data.revision)}</p>`;}
 $('#metadata-result').innerHTML=`<p>Current snapshot · ${esc(stamp(result.finished_at))}</p>${view}<details ${view?'':'open'}><summary>Original JSON</summary><pre>${esc(JSON.stringify(data,null,2))}</pre></details>`;}catch(e){if(rev===state.metaRev)$('#metadata-result').textContent=e.message;}}
$('#run').onclick=()=>run().catch(error);$('#close').onclick=closeDialog;$('#drawer-close').onclick=closeDialog;
$('#overlay').onclick=e=>{if(e.target===$('#overlay'))closeDialog();};
$('#wp-context').onchange=()=>select({action:'use-context',context:$('#wp-context').value}).catch(error);

$('#scenario').onchange=()=>{state.scenario=$('#scenario').value;reset();};
$('#meta-log').oninput=()=>{state.metaRev++;metadataControls();$('#metadata-result').textContent=state.info.session.metadata?'Choose a metadata view for this log.':'Connect to client etcd to read its current metadata.';};
$('#mode').onchange=()=>{populateAtomic();reset();};$('#atomic-check').onchange=reset;
for(const id of ['period','from','to','lookback','anchor'])$('#'+id).onchange=reset;
$('#zone').onchange=()=>{if(state.report){$('#from').value=inputTime(new Date(state.report.request.from));$('#to').value=inputTime(new Date(state.report.request.to));}render();};
$('#catalog').onclick=catalog;$('#history').onclick=()=>history().catch(error);$('#agent-settings').onclick=agentSetup;
$('#export-report').onclick=()=>exportReport().catch(error);$('#packet').onclick=()=>packet().catch(error);$('#analyze').onclick=()=>analyze().catch(error);
$('#cancel-analysis').onclick=async()=>{try{if(state.analysisJob)await api('/api/runs/'+state.analysisJob.id+'/cancel',{});}catch(e){error(e);}};
$('#refresh-cluster').onclick=cluster;
$('#connect-manual').onclick=()=>select({action:'connect-meta',etcd:$('#meta-etcd').value,prefix:$('#meta-prefix').value,log_name:$('#meta-log').value}).catch(error);
$('#disconnect-meta').onclick=()=>select({action:'disconnect-meta'}).catch(error);
document.addEventListener('click',e=>{const tab=e.target.closest('[data-tab]');if(tab){navigate(tab.dataset.tab);return;}const setup=e.target.closest('[data-setup]');if(setup){sources(setup.dataset.setup);return;}const filter=e.target.closest('[data-filter]');if(filter){state.filter=filter.dataset.filter;render();return;}const check=e.target.closest('[data-check]');if(check){showEvidence(check.dataset.check).catch(error);return;}const view=e.target.closest('[data-meta]');if(view){meta(view.dataset.meta);return;}const observation=e.target.closest('[data-observation]');if(observation){api('/api/diag/runs/'+state.job.id+'/observation/'+encodeURIComponent(observation.dataset.observation)).then(value=>dialog('Original evidence','IMMUTABLE SOURCE OBSERVATION',`<pre class="report">${esc(JSON.stringify(value,null,2))}</pre>`)).catch(error);}});
document.addEventListener('keydown',e=>{if($('#overlay').hidden)return;if(e.key==='Escape')closeDialog();if(e.key==='Tab'){const nodes=[...$('.drawer').querySelectorAll('button:not(:disabled),a[href],input,select,[tabindex="0"]')].filter(n=>n.getClientRects().length);if(!nodes.length)return;const first=nodes[0],last=nodes.at(-1);if(e.shiftKey&&(document.activeElement===first||document.activeElement===$('.drawer'))){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}});
window.addEventListener('resize',()=>{if(!$('#overlay').hidden&&$('#inspection-curve'))$('#inspection-curve').dispatchEvent(new Event('change'));});
