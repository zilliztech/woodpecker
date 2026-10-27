'use strict';

// Presentation only. Profiles, plans, findings and associations come from Go.
// No diagnostic thresholds, collector queries or health rules live here.
const $ = id => document.getElementById(id);
const state = {catalog:null, revision:0, planned:null, selected:'', active:false, generation:0, timer:null, raw:null, report:null, evidenceGeneration:0};
const sources = ['wpcli','prometheus','trace','loki'];
const names = {wpcli:'WP runtime',prometheus:'Prometheus metrics',trace:'Jaeger traces',loki:'Loki logs'};
const moduleNames={wpcli:'WP runtime',prometheus:'Metrics overview',trace:'Traces',loki:'Logs','milvus.append':'Write / append','milvus.persistence':'Persistence / checkpoint','milvus.consume':'WAL consumption / visibility','milvus.query':'Query / search'};
const fieldNames = {deployment:'Deployment',instance:'Exporter instance / Pod',node_id:'WP node ID',operation_id:'Request / operation ID',trace_id:'Trace ID',log_id:'Log ID',segment_id:'Segment ID',reader_id:'Reader ID',collection:'Collection',channel:'Channel'};
const targetValues = {};

function el(tag, text, attrs={}) {
 const node=document.createElement(tag);
 if(text!==undefined && text!==null) node.textContent=String(text);
 for(const [key,value] of Object.entries(attrs)) node.setAttribute(key,String(value));
 return node;
}
function clear(id){$(id).replaceChildren();return $(id);}
function badge(text){return el('span',text,{class:'badge '+String(text).replace(/[^a-z_]/g,'')});}
function notice(parent,text,kind=''){parent.append(el('div',text,{class:'notice '+kind}));}
function time(value){return value ? new Date(value).toISOString().replace('T',' ').replace('Z',' UTC') : 'Not recorded';}
function pretty(value){return JSON.stringify(value,null,2);}
function checked(id){return [...$(id).querySelectorAll('input:checked')].map(input=>input.value);}
function list(parent,values){const ul=el('ul');for(const value of values||[])ul.append(el('li',value));parent.append(ul);}
function details(parent,title){const d=el('details');d.append(el('summary',title));parent.append(d);return d;}
async function api(path,body){
 const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),30000);
 try{
  const response=await fetch(path,{cache:'no-store',signal:controller.signal,headers:body===undefined?{}:{'Content-Type':'application/json','X-WPDiag-Session':document.querySelector('meta[name=wpdiag-session]').content},method:body===undefined?'GET':'POST',body:body===undefined?undefined:JSON.stringify(body)});
  const raw=await response.text();let value;
  try{value=JSON.parse(raw);}catch{throw Error('The local server returned an unreadable response.');}
  if(!response.ok){const error=Error(value.error?.message||'The request failed.');error.code=value.error?.code;error.plan=value.plan;throw error;}
  return {value,raw};
 }catch(error){if(error.name==='AbortError')throw Error('The local server did not respond in time. Existing runs are not cancelled by this timeout.');throw error;}
 finally{clearTimeout(timer);}
}
function errorText(error){return (error.code?error.code+': ':'')+error.message;}
function checkRow(id,value,label,description=''){
 const input=el('input',null,{type:'checkbox',id,value});
 const row=el('label',null,{class:'check',for:id});const text=el('span',label);
 text.append(el('small',description));row.append(input,text);return row;
}
function matchingProfiles(module){
 const scope=$('scope').value, flow=$('flow').value, mode=$('mode').value;
 const childScope=module.source==='wpcli' && scope==='e2e'?'wp':scope;
 const childMode=module.source==='wpcli' && mode==='historical' && $('current-native').checked?'snapshot':mode;
 return module.profiles.filter(p=>p.scope===childScope&&p.flow===flow&&p.modes.includes(childMode));
}
function sourceChoices(){
 if($('execution').value==='modules')return [...new Set(checked('modules').map(id=>state.catalog.modules.find(m=>m.id===id).source))];
 if($('auto-sources').checked)return sources.filter(s=>(state.catalog.connections[s]||[]).some(c=>c.enabled)&&state.catalog.modules.some(m=>m.source===s&&matchingProfiles(m).length));
 return checked('sources');
}
function refreshChoices(){
 if(!state.catalog)return;
 const individual=$('execution').value==='modules';
 $('source-group').hidden=individual;$('module-group').hidden=!individual;
 $('stage-group').hidden=individual||$('scope').value==='wp';
 for(const input of $('sources').querySelectorAll('input'))input.disabled=$('auto-sources').checked;
 for(const module of state.catalog.modules){
  const input=$('module-'+module.id);const available=matchingProfiles(module).length>0;
  input.disabled=!available&&!input.checked;
  input.parentElement.querySelector('small').textContent=available?module.id+' · '+matchingProfiles(module).map(p=>'v'+p.version+' / '+p.stage).join(', '):'Unavailable for this scope / flow / window. Preview explains retained selections.';
 }
 const selected=checked('modules');const old=$('profile').value;
 $('profile').replaceChildren(el('option','Automatic matching profile',{value:''}));
 $('profile').disabled=!individual||selected.length!==1;
 if(individual&&selected.length===1){
  for(const p of matchingProfiles(state.catalog.modules.find(m=>m.id===selected[0])))$('profile').append(el('option',p.id+' · v'+p.version,{value:p.id}));
  if([...$('profile').options].some(o=>o.value===old))$('profile').value=old;
 }
 const stageValues=checked('stages');clear('stages');
 const stages=[...new Set(state.catalog.modules.filter(m=>m.stage&&m.profiles.some(p=>p.flow===$('flow').value)).map(m=>m.stage))];
 for(const stage of stages){const row=checkRow('stage-'+stage,stage,stage);row.querySelector('input').checked=stageValues.includes(stage);$('stages').append(row);}
 const mode=$('mode').value;
 $('live-fields').hidden=mode!=='live';$('historical-fields').hidden=mode!=='historical';
 $('duration').disabled=mode!=='live';$('interval').disabled=mode!=='live';
 $('from').required=mode==='historical';$('to').required=mode==='historical';
 $('from').disabled=mode!=='historical';$('to').disabled=mode!=='historical';
 $('native-context-label').hidden=$('scope').value==='milvus';
 renderTargets();
 if(mode==='snapshot'&&sourceChoices().includes('trace'))$('targets-section').open=true;
}
function renderTargets(){
 for(const input of $('targets').querySelectorAll('input')){targetValues[input.dataset.source]??={};targetValues[input.dataset.source][input.dataset.field]=input.value;}
 clear('targets');
 for(const source of sourceChoices()){
  const fields=new Set(state.catalog.modules.filter(m=>m.source===source&&($('execution').value!=='modules'||checked('modules').includes(m.id))).flatMap(m=>matchingProfiles(m)).flatMap(p=>p.target_fields));
  for(const [field,value] of Object.entries(targetValues[source]||{}))if(value)fields.add(field);
  const group=el('div');group.append(el('h3',names[source],{class:'target-title'}));
  if(!fields.size)group.append(el('p','No compatible profile for this selection. Preview will show the coverage limit.',{class:'hint'}));
  for(const field of fields){
   const id='target-'+source+'-'+field;const wrap=el('div',null,{class:'field'});
   wrap.append(el('label',(field==='instance'?(source==='loki'?'Kubernetes Pod':'Exporter instance'):(fieldNames[field]||field))+' · '+source,{for:id}));
   wrap.append(el('input',null,{id,type:'text',maxlength:'256',value:targetValues[source]?.[field]||'','data-source':source,'data-field':field,autocomplete:'off'}));group.append(wrap);
  }
  $('targets').append(group);
 }
}
function setRunDisabled(value){$('run').disabled=value;$('run-plan').disabled=value;}
function invalidate(){
 state.revision++;state.planned=null;setRunDisabled(true);
 $('form-status').textContent='Selection changed. Preview a new plan before running.';
 $('plan-state').textContent='Selection changed';$('plan-state').className='badge';
}
function milliseconds(id){const n=Number($(id).value)*1000;if(!Number.isSafeInteger(n))throw Error('Durations must use whole milliseconds.');return n;}
function request(){
 if(!state.catalog)throw Error('Wait for the module catalog to load.');
 const selection={sources:[],scope:$('scope').value,flow:$('flow').value,target:{},window:{mode:$('mode').value}};
 if(selection.window.mode==='live'){selection.window.duration_ms=milliseconds('duration');if($('interval').value)selection.window.interval_ms=milliseconds('interval');}
 if(selection.window.mode==='historical'){
  for(const key of ['from','to']){if(!$(key).value)throw Error('Select both historical UTC endpoints.');selection.window[key]=new Date($(key).value+'Z').toISOString();}
 }
 const q={selection,connection:'',connections:{},source_targets:{},limits:{budget_ms:milliseconds('budget'),max_input_bytes:Math.round(Number($('input-limit').value)*1048576),max_output_bytes:Math.round(Number($('output-limit').value)*1048576),max_targets:Number($('target-limit').value)}};
 if($('execution').value==='modules'){
  q.modules=checked('modules');if(!q.modules.length)throw Error('Choose at least one module.');
  if($('profile').value)selection.profile=$('profile').value;
 }else{
  if(!$('auto-sources').checked){selection.sources=checked('sources');if(!selection.sources.length)throw Error('Choose at least one source or enable auto selection.');}
  if(!$('stage-group').hidden&&checked('stages').length)q.stages=checked('stages');
 }
 q.current_native_context=selection.window.mode==='historical'&&selection.scope!=='milvus'&&$('current-native').checked;
 const chosen=sourceChoices();
 for(const source of chosen){if($('connection-'+source).value)q.connections[source]=$('connection-'+source).value;}
 for(const input of $('targets').querySelectorAll('input')){
  if(input.value&&chosen.includes(input.dataset.source)){q.source_targets[input.dataset.source]??={};q.source_targets[input.dataset.source][input.dataset.field]=input.value;}
 }
 return q;
}
function renderPlan(p){
 $('plan-state').textContent=p.status;$('plan-state').className='badge '+p.status;
 const root=clear('plan-content');
 for(const problem of p.problems||[])notice(root,problem.code+': '+problem.message,'error');
 for(const entry of p.entries||[]){
  const row=el('div',null,{class:'plan-entry'});const heading=el('div',null,{class:'entry-heading'});heading.append(el('strong',entry.id),badge(entry.status));row.append(heading);
  const s=entry.request.selection,l=entry.request.limits;
  row.append(el('p',(entry.profile.id||'No supported profile')+' · v'+(entry.profile.version||'?'),{class:'meta'}));
  row.append(el('p',s.scope+' / '+s.flow+' / '+s.window.mode+' · '+entry.role+' · connection '+(entry.request.connection||'(choose a connection)'),{class:'meta'}));
  row.append(el('p','Input '+l.max_input_bytes+' B · output '+l.max_output_bytes+' B · targets '+l.max_targets+' · time '+l.budget_ms+' ms',{class:'meta'}));
  for(const problem of entry.problems||[])notice(row,problem.code+': '+problem.message,entry.status==='unsupported'?'warning':'error');root.append(row);
 }
 for(const item of p.auto_omissions||[])notice(root,'Auto omission · '+item.message);
 const semantics=details(root,'Coverage and timing semantics');list(semantics,p.semantics);
 const exact=details(root,'Exact plan JSON');exact.append(el('pre',pretty(p)));
}
async function preview(event){
 event.preventDefault();const revision=state.revision;$('preview').disabled=true;$('form-status').textContent='Validating the plan locally…';
 try{
  const q=request();const {value:p}=await api('/api/plan',q);
  if(revision!==state.revision)return;
  renderPlan(p);$('plan-panel').scrollIntoView({block:'start',behavior:'smooth'});
  const ready=!(p.problems||[]).length&&(p.entries||[]).some(e=>e.status==='ready');
  state.planned=ready?q:null;setRunDisabled(!ready||state.active);
  $('form-status').textContent=ready?'Plan ready. Only Run diagnosis contacts the selected backends.':'This plan cannot run. Review the profile, connection and selection messages.';
 }catch(error){if(revision===state.revision){state.planned=null;setRunDisabled(true);$('form-status').textContent=errorText(error);}}
 finally{$('preview').disabled=false;}
}
async function refreshHistory(){
 const {value:rows}=await api('/api/runs');const select=clear('history');select.append(el('option','Select a recent run',{value:''}));
 for(const row of rows)select.append(el('option',time(row.created_at)+' · '+row.status+' · '+row.id.slice(0,8),{value:row.id}));
 if(rows.some(r=>r.id===state.selected))select.value=state.selected;
 return rows;
}
async function start(){
 if(!state.planned||state.active)return;
 setRunDisabled(true);$('form-status').textContent='Starting the previewed selection…';
 try{const {value:j}=await api('/api/runs',state.planned);await refreshHistory();selectRun(j.id);$('run-panel').scrollIntoView({block:'start',behavior:'smooth'});$('form-status').textContent='Run started. Editing the form will not change this run.';}
 catch(error){$('form-status').textContent=errorText(error);if(error.plan)renderPlan(error.plan);setRunDisabled(!state.planned||state.active);}
}
function renderProgress(job){
 const root=clear('progress');const rows=el('div',null,{class:'progress-list'});
 const latest=new Map();for(const e of job.events||[])if(e.module_id)latest.set(e.module_id,e);
 for(const entry of job.plan.entries||[]){const e=latest.get(entry.id);const row=el('div');row.append(el('span',entry.id+' '),badge(e?.status||entry.status));rows.append(row);}
 if(rows.childNodes.length)root.append(rows);
 if(job.events?.length){const d=details(root,'Progress events · advisory');d.append(el('pre',pretty(job.events)));}
}
function selectRun(id){
 clearTimeout(state.timer);state.generation++;state.selected=id;state.raw=null;state.report=null;
 $('history').value=id;$('download').disabled=true;$('cancel').disabled=true;clear('progress');clear('report');clear('run-status');
 if(id){state.active=true;poll(id,state.generation);}else{state.active=false;updateActions();}
}
function updateActions(){
 setRunDisabled(!state.planned||state.active);$('cancel').disabled=!state.selected||!state.active;$('download').disabled=!state.raw;
}
async function poll(id,generation){
 try{
  const {value:j}=await api('/api/runs/'+encodeURIComponent(id));
  if(generation!==state.generation)return;
  state.active=!j.report_ready;updateActions();
  const status=clear('run-status');status.append(badge(j.status),el('p','Started '+time(j.created_at)+(j.finished_at?' · finished '+time(j.finished_at):''),{class:'meta'}));
  status.append(el('p','Run '+j.id+' · Execution status is separate from findings and impact.',{class:'hint'}));
  renderProgress(j);
  if(j.report_ready){
   const {value:report,raw}=await api('/api/runs/'+encodeURIComponent(id)+'/report');
   if(generation!==state.generation)return;
   state.report=report;state.raw=raw;renderReport(report);updateActions();await refreshHistory();
  }else{state.timer=setTimeout(()=>poll(id,generation),500);}
 }catch(error){
  if(generation!==state.generation)return;
  notice(clear('run-status'),errorText(error),'error');
  if(error.code==='run_not_found'){state.active=false;updateActions();}
  else state.timer=setTimeout(()=>poll(id,generation),1500);
 }
}
async function cancel(){
 if(!state.selected)return;const id=state.selected;$('cancel').disabled=true;
 try{await api('/api/runs/'+encodeURIComponent(id)+'/cancel',{});if(state.selected===id)$('form-status').textContent='Cancellation requested. Already collected evidence is retained in the final report.';}
 catch(error){$('form-status').textContent=errorText(error);updateActions();}
}
function evidenceButton(module,id,label,kind=''){
 const b=el('button',label||id,{type:'button',class:kind,title:id,'data-observation-id':id,'data-module-id':module});
 b.addEventListener('click',()=>openEvidence(module,id));return b;
}
function referenceLinks(parent,module,ids,label,kind=''){
 if(!ids?.length)return;parent.append(el('p',label,{class:'hint'}));const links=el('div',null,{class:'evidence-links'});
 const child=state.report.runs.find(r=>r.module_id===module);
 for(const id of ids){const obs=child?.report.results?.flatMap(r=>r.observations||[]).find(o=>o.id===id);links.append(evidenceButton(module,id,(obs?.source_id||obs?.kind||'Observation')+' · '+id.slice(-12),kind));}
 parent.append(links);
}
function findingCard(f,module,sourceFindings){
 const card=el('article',null,{class:'finding','data-finding-id':f.id});
 const head=el('div',null,{class:'entry-heading'});head.append(badge(f.severity),el('span','Evidence: '+f.evidence_strength,{class:'badge'}));card.append(head);
 card.append(el('h3',f.outcome),el('p',f.summary,{class:'summary'}),el('p',f.rule_id+' · v'+f.rule_version+' · '+f.id,{class:'identity'}));
 if(Object.keys(f.target||{}).length){const d=details(card,'Observed target');d.append(el('pre',pretty(f.target)));}
 referenceLinks(card,module,f.supporting_observations,'Supporting observations');
 referenceLinks(card,module,f.contradicting_observations,'Contradicting observations','contradiction');
 if(f.unknown?.length){const d=details(card,'Uncertainty · '+f.unknown.length);list(d,f.unknown);}
 if(f.next_read_only_checks?.length){const next=el('div',null,{class:'next-check'});next.append(el('strong','Suggested next checks · not executed'));list(next,f.next_read_only_checks);card.append(next);}
 if(f.derived_from_findings?.length){
  const d=details(card,'Original source findings · '+f.derived_from_findings.length);
  for(const id of f.derived_from_findings){const original=sourceFindings.find(x=>x.id===id);if(original){d.append(el('p',original.rule_id+' · '+original.outcome),el('p',original.summary),el('p',original.id,{class:'identity'}));referenceLinks(d,module,original.supporting_observations,'Original support');}else notice(d,'Unresolved original finding: '+id,'warning');}
 }
 return card;
}
function gapSection(parent,title,gaps){
 if(!gaps?.length)return;const d=details(parent,title+' · '+gaps.length);let shown=0;
 const container=el('ul',null,{class:'gap-list'});d.append(container);
 const more=el('button','Show more',{type:'button',class:'secondary'});
 function add(){const end=Math.min(shown+50,gaps.length);for(;shown<end;shown++)container.append(el('li',gaps[shown].code+': '+(gaps[shown].detail||gaps[shown].message||'')));more.hidden=shown>=gaps.length;}
 more.addEventListener('click',add);d.append(more);add();
}
function renderReport(report){
 const root=clear('report');const runs=report.runs||[];const jumps=new Map();
 root.append(el('p','Report '+(report.run_id||'unavailable')+' · collected '+time(report.started_at)+' → '+time(report.finished_at),{class:'meta'}));
 for(const p of report.plan?.problems||[])notice(root,p.code+': '+p.message,'error');
 const strip=el('div',null,{class:'summary-strip'});strip.append(el('span',runs.length+' modules'),el('span',(report.summary?.findings||[]).length+' finding references'),el('span',(report.summary?.associations||[]).length+' evidence associations'));root.append(strip);
 const attention=[];
 for(const run of runs){
  const originals=[...(run.report.results||[]).flatMap(r=>r.findings||[]),...(run.report.assessments||[]).flatMap(a=>a.findings||[])];
  for(const view of (report.summary?.findings||[]).filter(v=>v.module_id===run.module_id)){
   const finding=originals.find(f=>f.id===view.id);
   if(finding&&['warning','critical'].includes(finding.severity))attention.push({module:run.module_id,finding});
  }
  if(run.report.status!=='completed'){
   const problems=(run.report.results||[]).flatMap(r=>r.problems||[]).map(p=>p.code+': '+p.message);
   const gaps=(run.report.results||[]).flatMap(r=>r.coverage_gaps||[]);const seen=new Set();
   if(!problems.length)for(const gap of gaps){if(!seen.has(gap.code)){seen.add(gap.code);problems.push(gap.code+': '+gap.detail);}if(problems.length>=3)break;}
   notice(root,run.module_id+' · '+run.report.status+': '+(problems.join('; ')||'Inspect module status and gaps below.'),'warning');
  }
 }
 const first=el('div',null,{class:'attention'});first.append(el('h3','Where to start'));
 if(attention.length){
  first.append(el('p',attention.length+' source-reported warning findings. These are hypotheses to inspect, not independent confirmations.',{class:'hint'}));
  for(const item of attention.slice(0,10)){
   const button=el('button',item.module+' · '+item.finding.summary,{type:'button',class:'attention-link'});
   button.addEventListener('click',()=>jumps.get(item.module)?.(item.finding.id));first.append(button);
  }
  if(attention.length>10)first.append(el('p','More warnings remain in the ordered module findings below.',{class:'hint'}));
 }else first.append(el('p','No warning finding was returned. Review inconclusive findings, unavailable sources and coverage gaps before assessing operation.',{class:'hint'}));
 root.append(first);
 if(report.plan?.request?.selection?.window?.mode==='historical')notice(root,'Historical incident. Any current_context module is a separate present-day snapshot, not a record of this incident.','warning');
 for(const run of runs){
  const child=run.report;const section=el('details',null,{class:'module','data-module':run.module_id});const title=el('summary',run.module_id);title.append(badge(child.status));section.append(title);const content=el('div',null,{class:'module-content'});section.append(content);
  const selection=child.selection||{};content.append(el('p',(selection.scope||'')+' / '+(selection.flow||'')+' / '+(selection.window?.mode||'')+' · '+run.role+' · '+(selection.profile||''),{class:'meta'}));
  content.append(el('p','Collection '+time(child.started_at)+' → '+time(child.finished_at),{class:'meta'}));
  if(run.role==='current_context')content.append(el('p','Current snapshot only. This does not confirm or contradict the historical incident.',{class:'scope-note'}));
  const sourceFindings=(child.results||[]).flatMap(r=>r.findings||[]);const assessments=child.assessments||[];
  const all=[...sourceFindings,...assessments.flatMap(a=>a.findings||[])];
  const views=(report.summary?.findings||[]).filter(v=>v.module_id===run.module_id);
  const findings=views.map(v=>all.find(f=>f.id===v.id)).filter(Boolean);
  if(!findings.length&&all.length)notice(content,'No validated summary references. Inspect original findings and lineage gaps in the JSON report.','warning');
  section.open=child.status!=='completed'||runs.indexOf(run)===0||findings.some(f=>['critical','warning'].includes(f.severity));
  for(const result of child.results||[])for(const problem of result.problems||[])notice(content,problem.code+': '+problem.message,'error');
  let shown=0;const cards=el('div');content.append(cards);const more=el('button','Show more findings',{type:'button',class:'secondary'});
  function add(){const end=Math.min(shown+25,findings.length);for(;shown<end;shown++)cards.append(findingCard(findings[shown],run.module_id,sourceFindings));more.hidden=shown>=findings.length;}
  more.addEventListener('click',add);content.append(more);add();
  jumps.set(run.module_id,id=>{
   const index=findings.findIndex(f=>f.id===id);while(shown<=index)add();section.open=true;
   const target=[...cards.querySelectorAll('[data-finding-id]')].find(n=>n.dataset.findingId===id);
   if(target){target.tabIndex=-1;target.focus();target.scrollIntoView({block:'start',behavior:'smooth'});}
  });
  gapSection(content,'Source / stage coverage gaps',[...(child.results||[]).flatMap(r=>r.coverage_gaps||[]),...assessments.flatMap(a=>a.coverage_gaps||[])]);
  const original=details(content,'Child selection and original window');original.append(el('pre',pretty(selection)));root.append(section);
 }
 const associations=report.summary?.associations||[];
 if(associations.length){const d=details(root,'Related evidence · '+associations.length);d.className='report-details';
  for(const a of associations){const row=el('div',null,{class:'association'});row.append(el('h3',a.kind),el('p','Observed trace identity: '+a.identity));list(row,a.unknown);const refs=el('div',null,{class:'evidence-links'});
   for(const ref of a.observations||[])refs.append(evidenceButton(ref.module_id,ref.id,ref.module_id+' · '+(ref.span_id?'span '+ref.span_id:ref.event_id?'event '+ref.event_id:ref.id)));
   row.append(refs);const ids=details(row,'Exact original references');ids.append(el('pre',pretty(a)));d.append(row);
  }
 }
 gapSection(root,'Cross-source coverage / identity gaps',report.summary?.coverage_gaps);
 gapSection(root,'Auto-plan omissions',report.plan?.auto_omissions);
 const plan=details(root,'Executed run plan');plan.className='report-details';plan.append(el('pre',pretty(report.plan)));
 const raw=details(root,'Exact report JSON');raw.className='report-details';raw.addEventListener('toggle',()=>{if(raw.open&&!raw.querySelector('pre'))raw.append(el('pre',state.raw));});
 if(!runs.length)notice(root,'No module evidence is available. Inspect the report status and plan problems.','warning');
}
function fact(parent,label,value){if(value===undefined||value===null||value==='')return;const p=el('p',null,{class:'fact-line'});p.append(el('strong',label+':'),el('span',typeof value==='object'?JSON.stringify(value):value));parent.append(p);}
async function openEvidence(module,id){
 const generation=++state.evidenceGeneration;const dialog=$('evidence-dialog');if(!dialog.open)dialog.showModal();
 $('evidence-title').textContent='Original observation';$('evidence-context').textContent=module+' · '+id;clear('evidence-facts');$('evidence-body').textContent='Loading exact source evidence…';
 try{
  const {value:o,raw}=await api('/api/runs/'+encodeURIComponent(state.selected)+'/evidence?'+new URLSearchParams({module_id:module,observation_id:id}));
  if(generation!==state.evidenceGeneration)return;
  $('evidence-title').textContent=o.kind;const box=clear('evidence-facts');fact(box,'Observation status',o.status);fact(box,'Target',o.target);fact(box,'Collection start',time(o.started_at));fact(box,'Collection finish',time(o.finished_at));
  if(o.query){fact(box,'Query language',o.query.language);fact(box,'Query executed',o.query.executed?'Yes · attempted by collector':'No · suggested only');box.append(el('pre',o.query.expression));}
  if(o.data){fact(box,'Evidence from',o.data.from);fact(box,'Evidence to',o.data.to);fact(box,'Signal',o.data.signal);fact(box,'Unit',o.data.unit);
   for(const f of (o.data.facts||[]).slice(0,10)){
    const population=details(box,'Observed population · '+(f.population_id||f.exporter_identity||'source fact'));
    fact(population,'Labels',f.labels);fact(population,'Observed completions',f.observed_completions);fact(population,'Completions / second',f.completions_per_second);fact(population,'Mean (ms)',f.mean_ms);fact(population,'p95 lower bound (ms)',f.p95_lower_ms);fact(population,'p95 upper bound (ms)',f.p95_upper_ms);fact(population,'Configured threshold (ms)',f.threshold_ms);
    if(f.gauge){fact(population,'First',f.gauge.first);fact(population,'Last',f.gauge.last);fact(population,'Peak',f.gauge.peak);}
   }
   for(const event of (o.data.events||[]).slice(0,10))fact(box,'Retained log excerpt',event.excerpt);
   for(const span of (o.data.spans||[]).filter(x=>x.selected).slice(0,10)){fact(box,'Selected span',span.operation+' · '+span.span_id);fact(box,'Start (µs)',span.start_us);fact(box,'Duration (µs)',span.duration_us);}
  }
  $('evidence-body').textContent=raw;
 }catch(error){if(generation===state.evidenceGeneration)$('evidence-body').textContent=errorText(error);}
}
function download(){
 if(!state.raw)return;const blob=new Blob([state.raw],{type:'application/json'});const url=URL.createObjectURL(blob);const a=el('a',null,{href:url,download:'wpdiag-'+(state.report?.run_id||state.selected)+'.json'});document.body.append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
async function initialize(){
 const {value:catalog}=await api('/api/catalog');state.catalog=catalog;
 $('budget').value=catalog.default_limits.budget_ms/1000;$('input-limit').value=catalog.default_limits.max_input_bytes/1048576;$('output-limit').value=catalog.default_limits.max_output_bytes/1048576;$('target-limit').value=catalog.default_limits.max_targets;
 for(const source of sources){
  const rows=catalog.connections[source]||[];
  $('sources').append(checkRow('source-'+source,source,names[source],rows.length?rows.map(c=>c.name+(c.enabled?'':' (disabled)')).join(', '):'No configured connection'));
  const group=el('div',null,{class:'field'});group.append(el('label',names[source],{for:'connection-'+source}));const select=el('select',null,{id:'connection-'+source});select.append(el('option',rows.length?'Choose / use sole connection':'No configured connection',{value:''}));
  for(const row of rows)select.append(el('option',row.name+(row.enabled?'':' · disabled'),{value:row.name}));group.append(select);$('connections').append(group);
 }
 for(const module of catalog.modules)$('modules').append(checkRow('module-'+module.id,module.id,moduleNames[module.id]||module.id));
 const end=new Date(Date.now()-60000);$('to').value=end.toISOString().slice(0,19);$('from').value=new Date(end-300000).toISOString().slice(0,19);
 refreshChoices();$('form-status').textContent='Connections loaded. Preview validates locally without backend access.';
 const rows=await refreshHistory();if(rows.length)selectRun(rows[0].id);
}
$('selection').addEventListener('input',event=>{invalidate();if(!event.target.closest('#targets'))refreshChoices();});
$('selection').addEventListener('change',event=>{if(event.target.matches('select,input[type=checkbox]')){invalidate();refreshChoices();}});
$('selection').addEventListener('submit',preview);$('run').addEventListener('click',start);$('run-plan').addEventListener('click',start);$('cancel').addEventListener('click',cancel);$('download').addEventListener('click',download);$('history').addEventListener('change',()=>selectRun($('history').value));
$('close-evidence').addEventListener('click',()=>$('evidence-dialog').close());
$('theme').addEventListener('click',()=>{const dark=document.documentElement.dataset.theme==='dark'||(!document.documentElement.dataset.theme&&matchMedia('(prefers-color-scheme: dark)').matches);document.documentElement.dataset.theme=dark?'light':'dark';});
initialize().catch(error=>{const box=$('connection-error');box.hidden=false;box.textContent=errorText(error);$('preview').disabled=true;$('form-status').textContent='Reconnect to the printed local server URL.';});
