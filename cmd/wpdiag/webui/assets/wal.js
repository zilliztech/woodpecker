/* Problem-oriented WAL workbench. Decisions belong to the Go backend. */
'use strict';
const $ = s => document.querySelector(s);
const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels = {pending:'Not checked',running:'Collecting',abnormal:'Abnormal',clear:'Within rule',unknown:'Inconclusive',context:'Context',not_applicable:'Not applicable',idle:'Idle'};
const symbols = {pending:'·',running:'↻',abnormal:'!',clear:'✓',unknown:'?',context:'i',not_applicable:'–',idle:'–'};
const state = {catalog:null,defs:{},problem:'write_slow',target:'',filter:'all',report:null,job:null,analysis:null,analysisJob:null,busy:false,error:'',events:[],generation:0};
let focusBeforeDialog;
let dialogRevision=0;
async function api(path, body) {
 const res = await fetch(path, body === undefined ? {} : {method:'POST',headers:{'Content-Type':'application/json','X-WPDiag-Session':$('meta[name="wpdiag-session"]').content},body:JSON.stringify(body)});
 const text = await res.text(); let value; try { value = JSON.parse(text); } catch { throw new Error('The local server returned an invalid response.'); }
 if (!res.ok) throw new Error(value.message || value.error?.message || value.error || `Request failed (${res.status})`);
 return value;
}
function problem() { return state.catalog.catalog.problems.find(p => p.id === state.problem); }
function target() { return state.catalog.targets.find(t => t.id === state.target); }
function ids() { if(state.report?.check)return state.report.evidence.map(e=>e.id); if(state.busy&&state.job?.wal_request?.check)return state.job.wal_checks||[]; const p=problem(); return [...p.evidence,...($('#milvus').checked ? p.extension : [])]; }
function evidence() { return Object.fromEntries((state.report?.evidence || []).map(e=>[e.id,e])); }
function count(status) { return state.report?.evidence.filter(e=>e.status===status).length || 0; }
function hasAnalysis() { return !!state.analysisJob && !state.analysisJob.report_ready && ['running','cancelling'].includes(state.analysisJob.status); }
function stamp(v) { const d=new Date(v); return $('#zone').value==='utc' ? d.toISOString().replace('T',' ').replace('.000Z',' UTC') : d.toLocaleString(); }
function rangeLabel() { const r=state.report?.time_range; if (r?.mode==='snapshot') return 'Current snapshot · '+stamp(r.observed_at); if (r) return stamp(r.start)+' → '+stamp(r.end); return state.problem==='metadata' ? 'Current read-only snapshot' : $('#period').value==='custom' ? 'Custom incident window' : `Last ${$('#period').value} minutes · fixed when the run starts`; }
function timeSelection() {
 if (state.problem==='metadata') return {};
 let to=new Date(Date.now()-1000), from=new Date(to.getTime()-Number($('#period').value)*60000);
 if ($('#period').value==='custom') {
  const suffix=$('#zone').value==='utc'?'Z':'';
  from=new Date($('#from').value+suffix); to=new Date($('#to').value+suffix);
 }
 if (!Number.isFinite(+from)||!Number.isFinite(+to)||from>=to||to>Date.now()||to-from>86400000) throw new Error('Choose a past interval with From before To, at most 24 hours. The source may impose a smaller limit.');
 return {from:from.toISOString(),to:to.toISOString()};
}
function reset() { state.generation++; state.report=null;state.job=null;state.analysis=null;state.analysisJob=null;state.filter='all';state.error='';state.events=[];render(); }
function statusFor(id) {
 if (state.report) return evidence()[id] || {status:'unknown',value:'Not in this report',note:'This selection differs from the stored report.'};
 if (state.busy) return {status:state.events.some(e=>e.evidence_id===id)?'running':'pending',note:'Waiting for the bounded source collection to finish'};
 return {status:'pending'};
}
// A small, escaped text formatter: headings, lists, emphasis and known evidence
// references only. Model HTML and URLs are never interpreted as markup/links.
function formatAgentAnswer(text) {
 const known=new Set((state.report?.evidence||[]).map(e=>e.id));
 const inline=value=>esc(value)
  .replace(/`([^`]+)`/g,'<code>$1</code>')
  .replace(/\*\*([^*]+)\*\*/g,'<strong>$1</strong>')
  .replace(/\[([A-Z]{1,2}\d{2}(?:\s*[,/]\s*[A-Z]{1,2}\d{2})*)\]/g,(original,refs)=>{
   return refs.split(/\s*[,/]\s*/).map(id=>known.has(id)?`<button class="agent-evidence" data-evidence="${id}" aria-label="Open evidence ${id}">${id}</button>`:`[${id}]`).join(' ');
  });
 let html='',list='',paragraph=[];
 const flushParagraph=()=>{if(paragraph.length){html+=`<p>${inline(paragraph.join(' '))}</p>`;paragraph=[];}};
 const closeList=()=>{if(list){html+=`</${list}>`;list='';}};
 for(const raw of String(text).split(/\r?\n/)) {
  const line=raw.trim();if(!line){flushParagraph();closeList();continue;}
  const heading=line.match(/^#{1,3}\s+(.+)$/);
  if(heading){flushParagraph();closeList();html+=`<h3>${inline(heading[1])}</h3>`;continue;}
  const bullet=line.match(/^(?:[-*]|\d+[.)])\s+(.+)$/);
  if(bullet){flushParagraph();const tag=/^\d/.test(line)?'ol':'ul';if(list!==tag){closeList();list=tag;html+=`<${tag}>`;}html+=`<li>${inline(bullet[1])}</li>`;continue;}
  if(list){closeList();}paragraph.push(line);
 }
 flushParagraph();closeList();return html;
}

function card(title,icon,body,kind='') { return `<section class="surface aside-card ${kind}"><div class="action-card-head"><div class="action-icon">${icon}</div><h3>${title}</h3></div><div class="action-card-body">${body}</div></section>`; }
function render() {
 if(!state.catalog) return;
 const p=problem(),t=target(),list=ids(),a=count('abnormal'),u=count('unknown'),r=state.report,done=r?.evidence.length||0;
 $('#problem-list').innerHTML=state.catalog.catalog.problems.map(p=>`<button class="problem ${p.id===state.problem?'active':''}" data-problem="${p.id}" ${state.busy||hasAnalysis()?'disabled':''} aria-current="${p.id===state.problem?'page':'false'}"><span class="problem-icon">${esc(p.icon)}</span><div><strong>${esc(p.title)}</strong><span class="sub">${esc(p.sub)}</span></div></button>`).join('');
 $('#question-header').innerHTML=`<div><div class="problem-eyebrow">${state.problem==='metadata'?'CURRENT METADATA':state.problem.startsWith('read')||state.problem==='open_reader'?'READ PATH':'WRITE PATH'}</div><h1>${esc(p.title)}</h1><p>${esc(p.description)}</p></div><div class="run-area"><button id="run" class="primary" ${(!t||hasAnalysis()||(state.busy&&!state.job))?'disabled':''}>${state.busy?'<i class="spinner"></i>Stop checks':r?(r.check?'▶ Run scenario':'↻ Run again'):'▶ Run checks'}</button><span class="action-caption">${state.busy?'Keep a partial report':`${list.length} evidence points · No AI`}</span></div>`;
 $('#history').disabled=state.busy||hasAnalysis();
 const meta=state.problem==='metadata';
 for(const el of document.querySelectorAll('.timefield')) el.hidden=meta;
 $('#metadata-time').hidden=!meta;$('#custom-range').hidden=meta||$('#period').value!=='custom';
 for(const sel of ['#instance','#period','#zone','#from','#to','#milvus']) $(sel).disabled=state.busy||hasAnalysis();
 $('#milvus').disabled=state.busy||hasAnalysis()||!t?.milvus_ready||!p.extension.length;
 $('#milvus-binding').hidden=!$('#milvus').checked;
 $('#milvus-binding').textContent='Only the explicitly configured Milvus channel association is checked. Shared Kubernetes or Grafana does not establish identity. General search, query and data-file persistence are outside scope.';
 $('#source-summary').innerHTML=`<span class="source-state">${esc(meta?'WP context: '+(t?.context||t?.wpcli||'not configured'):'Prometheus: '+(t?.prometheus||'not configured'))}</span><button class="ghost" data-action="sources">View source setup ↗</button>`;
 $('#agent-status').textContent=state.catalog.agent.ready?'Ready':state.catalog.agent.enabled?'Unavailable':'Off';
 $('#request-error').hidden=!state.error;$('#request-error').textContent=state.error;
 const progress=state.busy?state.events.filter(e=>e.phase==='complete').length:done;
 $('#workflow').innerHTML=['Choose target','Run checks','Review evidence'].map((s,i)=>`<span class="${i===(state.busy?1:r?2:0)?'active':''}"><b>${i+1}</b>${s}</span>`).join('');
 let title='Ready to inspect '+p.title.toLowerCase(),note='Collect source evidence and apply explicit reference rules. No model is involved.',tone='ready',label='READY TO CHECK';
 if(state.busy){title='Collecting evidence…';note=state.events.at(-1)?.message||'Starting bounded source requests. You can stop and keep a partial report.';label='CHECKS IN PROGRESS';}
 else if(r){
  title=a?`${a} abnormal evidence point${a===1?'':'s'} need review`:u?'The available evidence is incomplete':'No configured rule matched';
  note=a?'Start with the highlighted findings. A rule match is not a confirmed root cause.':u?'No data, missing completions and source failures are shown separately. Review these gaps before drawing a conclusion.':'These observations do not establish a root cause or prove the whole WAL path healthy.';
  tone=a?'warn':u?'unknown':'clear-state';label=a?'REVIEW FINDINGS':u?'EVIDENCE GAPS':'CHECKS COMPLETE';
  if(r.check)note=`Single check ${r.check}, including required supporting checks. ${note}`;
  if(!r.complete||r.status!=='completed'){note=`Run status: ${r.status}. ${note}`;if(!a)tone='unknown';}
 }
 $('#result-banner').className='result-banner '+tone;
 $('#result-banner').innerHTML=`<div class="panel-head result-head"><div class="panel-title">Check result</div><div class="outcome-label">${label}</div></div><div class="result-content"><h2>${esc(title)}</h2><p>${esc(note)}</p>${r?.problems?.length?`<div class="error-box">${r.problems.map(p=>esc(p.message+' ('+p.code+')')).join('<br>')}</div>`:''}${r?`<div class="summary-stats"><div class="summary-stat ${a?'hot':''}"><b>${a}</b><span>Abnormal</span></div><div class="summary-stat"><b>${u}</b><span>Unassessed</span></div><div class="summary-stat"><b>${done}</b><span>Evidence points</span></div></div>`:''}<div class="outcome-bottom"><span>${esc(rangeLabel())}</span><span>${state.busy?`${progress} / ${list.length} finalized`:r?`${r.complete?'Collection finished':'Partial collection'} · ${esc(r.target.label)}`:'Read-only · Exact exporter populations'}</span></div></div>`;
 $('#evidence-heading').textContent=r?'Evidence review':'What these checks inspect';
 $('#evidence-subtitle').textContent=r?'Open a row for the measurement, exact rule and original source.':'Every point declares its scope, decision rule and limitations.';
 $('#completion').textContent=r?`${done} points recorded`:`${list.length} defined points`;
 $('#filters').innerHTML=[['all','All evidence',list.length],['abnormal','Findings',a],['unknown','Missing evidence',u]].map(([key,title,n])=>`<button data-filter="${key}" class="${state.filter===key?'active':''}" aria-pressed="${state.filter===key}">${title} <span class="filter-count">${n}</span></button>`).join('');
 let group='',rows='';
 for(const id of list){const d=state.defs[id],e=statusFor(id);if(state.filter!=='all'&&e.status!==state.filter)continue;
  if(group!==d.group){group=d.group;rows+=`<div class="group-heading">${esc(group)}</div>`;}
  rows+=`<button class="check-row is-${e.status}" data-evidence="${id}"><span class="check-mark ${e.status}">${e.status==='running'?'<i class="spinner"></i>':symbols[e.status]||'?'}</span><div class="check-main"><div class="check-title"><span class="evidence-id">${id}</span>${esc(d.title)}</div><div class="check-desc">${esc(e.note||`${d.source} · ${d.disposition==='defer'?'Not automated in this version':d.scope}`)}</div></div><div class="check-reading">${e.value?`<span class="reading-value ${e.status==='abnormal'?'hot':''}">${esc(e.value)}</span>`:''}<span class="status ${e.status}">${esc(e.status_label||labels[e.status]||e.status)}</span></div><span class="chevron">›</span></button>`;
 }
 $('#checks').innerHTML=rows||'<div class="empty-list">No points in this filter. Select All evidence to see the full check.</div>';
 $('#step-path').innerHTML=p.path.map(s=>`<span>${esc(s)}</span>`).join('<i>→</i>');
 const ready=!!r&&!state.busy;
 $('#support').innerHTML=`<div class="side-kicker">${r?'WHAT YOU CAN DO NEXT':'HOW TO USE THIS CHECK'}</div>`+(r?card('Export evidence','↓',`<p>Share the immutable report, including observations and gaps.</p><button class="action" data-action="export" ${ready?'':'disabled'}>↓ Export JSON report</button><button class="action-link" data-action="markdown" ${ready?'':'disabled'}>Export readable summary ↗</button><button class="action-link" data-action="packet" ${ready?'':'disabled'}>Preview Agent input ↗</button>`):card('Start with one check run','↗','<div class="ready-steps"><div class="ready-step"><i>1</i><span>Confirm the target and incident window.</span></div><div class="ready-step"><i>2</i><span>Select <b>Run checks</b>.</span></div><div class="ready-step"><i>3</i><span>Review findings and missing evidence.</span></div></div>'))+card('Agent analysis <span class="optional-badge">Optional AI</span>','✧',`<p>Explain abnormal findings and relevant context. No automatic queries or actions.</p><button class="action agent" data-action="analyze" ${ready&&a>0&&state.catalog.agent.ready&&!hasAnalysis()?'':'disabled'}>${hasAnalysis()?'<i class="spinner"></i>Analyzing…':'✧ Analyze findings'}</button>${hasAnalysis()?`<button class="action-link" data-action="cancel-analysis" ${state.analysisJob?.id?'':'disabled'}>Stop analysis</button>`:''}<button class="action-link" data-action="agent">${state.catalog.agent.ready?'View Agent configuration':'Set up local Agent →'}</button><div class="proof-note">${a?'Only the supplied findings packet is sent.':'No model call for normal or missing-only results.'}<br>Source evidence remains available if analysis fails.</div>`,'agent-card')+card('Check coverage','◎',`<div class="coverage-line"><span>Abnormal / unassessed</span><b>${a} / ${u}</b></div><div class="coverage-line"><span>Catalog</span><b>${list.length} points</b></div><button class="action-link" data-action="catalog">Review check definitions ↗</button>`,'coverage-card');
 $('#investigation').hidden=!state.analysis&&!hasAnalysis();
 $('#investigation').innerHTML=state.analysis||hasAnalysis()?`<div class="panel-head"><div class="panel-title">Agent findings analysis</div><span class="status context">${esc(state.analysis?.status||'Running')}</span></div><div class="result-content">${state.analysis?.text?`<div class="agent-answer">${formatAgentAnswer(state.analysis.text)}</div>`:`<p class="analysis-copy">${esc(state.analysis?.code||'Waiting for the configured model.')}</p>`}${state.analysis?'<button class="action-link" data-action="export-analysis">↓ Export analysis result</button>':''}<div class="proof-note">Model interpretation of supplied evidence. Follow-up suggestions have not been executed.</div></div>`:'';
}
function dialog(title,kicker,body,sub='') {
 dialogRevision++;
 focusBeforeDialog=document.activeElement;$('#drawer-title').textContent=title;$('#drawer-kicker').textContent=kicker;$('#drawer-sub').textContent=sub;$('#drawer-body').innerHTML=body;$('#drawer-bottom').innerHTML='<button data-action="close">Close</button>';$('#overlay').hidden=false;$('.drawer').focus();
}
function closeDialog(){ dialogRevision++;$('#overlay').hidden=true;focusBeforeDialog?.focus(); }
function block(title,value) { return `<section class="detail-block"><h3>${esc(title)}</h3><p>${esc(value)}</p></section>`; }
function targetText(labels={}) {
 return ['instance','node_id','log_ns','log_id','reader_name','az_scope','status','path'].filter(k=>labels[k]!==undefined).map(k=>`${k}=${labels[k]}`).join(' · ')||'Selected target';
}
function sameChartTarget(a={},b={}) {
 const keys=new Set([...Object.keys(a),...Object.keys(b)]);
 return [...keys].filter(k=>k!=='__name__'&&k!=='le').every(k=>a[k]===b[k]);
}
async function showEvidence(id) {
 const d=state.defs[id],e=statusFor(id);if(!d)return;
 const description=state.catalog.descriptions[id];
 dialog(`${id} · ${d.title}`,'EVIDENCE','');
 const revision=dialogRevision;
 const initial={...description,evidence_id:id,status:e.status,status_label:e.status_label,value:e.value||labels[e.status],value_label:"Window result",conclusion:state.busy?'Collection is in progress.':'Not checked.',measurements:[],anomalies:[],curves:[],observation_ids:[]};
 renderEvidence(initial);
 if(!state.report?.evidence.some(e=>e.id===id))return;
 $('#inspection-result .inspection-conclusion').textContent='Loading recorded evidence…';
 try {
  const detail=await api(`/api/wal/runs/${state.job.id}/inspect/${id}`);
  if(revision!==dialogRevision||$('#overlay').hidden)return;
  renderEvidence({...detail,evidence_id:id});
 }catch(err){if(revision===dialogRevision&&!$('#overlay').hidden)$('#inspection-result .inspection-conclusion').textContent=err.message;}
}
function renderEvidence(detail) {
 const status=detail.status_label||labels[detail.status]||detail.status;
 const empty=detail.status==='unknown'?'Not established from the available data.':detail.status==='pending'||detail.status==='running'?'No completed assessment.':detail.status==='context'?'No independent anomaly threshold.':detail.status==='not_applicable'?'Branch not applicable.':'No rule match.';
 $('#drawer-body').innerHTML=`<div class="inspection">
 <div class="inspection-actions"><button class="primary" id="run-single-check" ${state.busy||hasAnalysis()||!target()||![...problem().evidence,...($('#milvus').checked?problem().extension:[])].includes(detail.evidence_id)?'disabled':''}>▶ Run this check</button><p>Includes required supporting checks. Uses the selected target and incident window.</p></div>
 <section class="inspection-section"><h3>Metric / source</h3><div class="inspection-metrics">${detail.metrics.map(m=>`<code>${esc(m)}</code>`).join('')}</div><p class="inspection-meaning">${esc(detail.meaning)}</p></section>
 <section class="inspection-section"><h3>Calculation</h3><p class="inspection-formula">${esc(detail.formula)}</p><p class="inspection-criterion">${esc(detail.criterion)}</p></section>
 <section id="inspection-result" class="inspection-result is-${esc(detail.status)}"><div class="inspection-result-top"><h3>Result</h3><span class="status ${esc(detail.status)}">${esc(status)}</span></div><span class="inspection-value-label">${esc(detail.value_label)}</span><strong>${esc(detail.value)}</strong><p class="inspection-conclusion">${esc(detail.conclusion)}</p></section>
 <section class="inspection-section"><h3>Time series</h3><div id="inspection-series"></div></section>
 <section class="inspection-section"><div class="inspection-section-heading"><h3>Abnormal data</h3><span>${detail.anomalies.length} records</span></div><div id="inspection-anomalies"></div></section>
 ${detail.observation_ids.length?`<details class="inspection-source"><summary>Source queries &amp; raw data</summary>${detail.observation_ids.map(o=>`<button class="observation-link" data-observation="${esc(o)}">${esc(o)} ↗</button>`).join('')}</details>`:''}
 </div>`;
 $('#run-single-check').onclick=()=>{const id=detail.evidence_id;closeDialog();run(id).catch(error);};
 let page=0;const pageSize=4;
 const anomalyTargets=[...new Set(detail.anomalies.map(row=>targetText(row.labels)))];
 const anomalies=()=>{
  const rows=detail.anomalies.slice(page*pageSize,page*pageSize+pageSize);
  $('#inspection-anomalies').innerHTML=rows.length?`${anomalyTargets.length===1?`<p class="inspection-anomaly-target">${esc(anomalyTargets[0])}</p>`:''}<div class="inspection-table-wrap"><table class="inspection-table"><thead><tr><th>Time / target</th><th>Measurement</th></tr></thead><tbody>${rows.map(row=>`<tr><td>${row.from_ms?`${esc(stamp(row.from_ms))}<br>→ ${esc(stamp(row.to_ms))}`:''}${anomalyTargets.length>1?`<span class="inspection-target">${esc(targetText(row.labels))}</span>`:''}</td><td><b>${esc(row.value)}</b><span>${esc(row.detail)}</span></td></tr>`).join('')}</tbody></table></div>${detail.anomalies.length>pageSize?`<div class="inspection-pagination"><button id="anomaly-previous" ${page?'':'disabled'}>Previous</button><span>${page*pageSize+1}–${Math.min((page+1)*pageSize,detail.anomalies.length)} / ${detail.anomalies.length}</span><button id="anomaly-next" ${(page+1)*pageSize<detail.anomalies.length?'':'disabled'}>Next</button></div>`:''}`:`<p class="inspection-empty">${esc(empty)}</p>`;
  if($('#anomaly-previous'))$('#anomaly-previous').onclick=()=>{page--;anomalies();};
  if($('#anomaly-next'))$('#anomaly-next').onclick=()=>{page++;anomalies();};
 };
 anomalies();
 const curves=detail.curves;
 if(!curves.length){$('#inspection-series').innerHTML=`<p class="inspection-empty">${state.report?.time_range.mode==='snapshot'?'Current snapshot; no time series.':detail.status==='pending'?'Run checks to collect the curve.':'No time-series measurements available.'}</p>`;return;}
 const first=curves.findIndex(c=>detail.anomalies.some(a=>sameChartTarget(a.labels,c.labels)));
 $('#inspection-series').innerHTML=`<label class="inspection-series-label" for="inspection-curve">Metric / target</label><select id="inspection-curve">${curves.map((c,i)=>`<option value="${i}">${esc(c.metric.replace('woodpecker_','')+' · '+targetText(c.labels))}</option>`).join('')}</select><div id="inspection-chart"></div>`;
 $('#inspection-curve').value=String(first<0?0:first);
 const plot=()=>renderMetricCurve(curves[Number($('#inspection-curve').value)]);
 $('#inspection-curve').onchange=plot;plot();
}
function renderMetricCurve(curve) {
 const valid=curve.points.filter(p=>Number.isFinite(p.value));
 if(!valid.length){$('#inspection-chart').innerHTML=`<p class="inspection-empty">${curve.kind==='interval'?'P99 data is unavailable for this interval.':'No measurements available to plot.'}</p>`;return;}
 const width=Math.max(320,Math.min(760,$('#inspection-chart').clientWidth||760)),height=245,left=57,right=18,top=20,bottom=44;
 const start=curve.points.reduce((v,p)=>Math.min(v,p.time_ms),Infinity),end=curve.points.reduce((v,p)=>Math.max(v,p.time_ms),-Infinity);
 const low=valid.reduce((v,p)=>Math.min(v,p.value),0);
 let high=valid.reduce((v,p)=>Math.max(v,Number.isFinite(p.upper)?p.upper:p.value),curve.reference||0);if(high<=low)high=low+1;
 high+=(high-low)*.1;
 const x=t=>left+(t-start)/Math.max(1,end-start)*(width-left-right),y=v=>height-bottom-(v-low)/(high-low)*(height-top-bottom);
 const number=v=>Number(v).toLocaleString(undefined,{maximumSignificantDigits:4});
 const line=field=>{let path='',open=false;for(const p of curve.points){const value=p[field];if(!Number.isFinite(value)||p.code){open=false;continue;}path+=(open?' L':' M')+x(p.time_ms).toFixed(2)+','+y(value).toFixed(2);open=true;}return path;};
 const ticks=[0,.25,.5,.75,1].map(f=>{const v=low+(high-low)*f;return `<line x1="${left}" x2="${width-right}" y1="${y(v)}" y2="${y(v)}" class="chart-grid"/><text x="${left-8}" y="${y(v)+4}" text-anchor="end">${esc(number(v))}</text>`;}).join('');
 const ref=Number.isFinite(curve.reference)?`<line x1="${left}" x2="${width-right}" y1="${y(curve.reference)}" y2="${y(curve.reference)}" class="chart-reference"/><text x="${width-right}" y="${y(curve.reference)-6}" text-anchor="end" class="chart-reference-label">Reference ${esc(number(curve.reference))}</text>`:'';
 const stride=Math.max(1,Math.ceil(curve.points.length/600));
 const points=curve.points.map((p,i)=>{
  if(!Number.isFinite(p.value)||(i%stride!==0&&i!==curve.points.length-1))return '';
  const upper=Number.isFinite(p.upper)?p.upper:null;
  const label=`${stamp(p.time_ms)} · ${upper===null?(curve.kind==='interval'?'>= ':'')+number(p.value):number(p.value)+'–'+number(upper)} ${curve.unit}${p.count!==undefined?' · '+p.count+' completions':''}${p.code?' · '+p.code:p.matched_rule?' · matched sustained rule':p.above_reference?' · above reference':''}`;
  return `<g class="chart-point ${p.code?'unqualified':p.matched_rule?'matched':p.above_reference?'above':''}" data-chart-point="${i}" tabindex="0" aria-label="${esc(label)}"><title>${esc(label)}</title>${upper!==null?`<line x1="${x(p.time_ms)}" x2="${x(p.time_ms)}" y1="${y(p.value)}" y2="${y(upper)}" class="chart-range"/>`:''}<circle cx="${x(p.time_ms)}" cy="${y(p.value)}" r="${p.matched_rule?4:3}"/>${upper===null&&curve.kind==='interval'?`<text x="${x(p.time_ms)}" y="${y(p.value)-7}" text-anchor="middle">↑</text>`:''}<circle cx="${x(p.time_ms)}" cy="${y(p.value)}" r="11" class="chart-hit"/></g>`;
 }).join('');
 const axisTime=t=>new Date(t).toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit',timeZone:$('#zone').value==='utc'?'UTC':undefined});
 $('#inspection-chart').innerHTML=`<div class="inspection-chart-caption"><b>${esc(curve.kind==='interval'?'Minute P99 bucket interval':curve.unit==='events / scrape'?'Count increase per scrape':'Measured values')}</b><span>${esc(curve.unit)}</span></div><svg class="inspection-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(curve.metric+' time series')}">${ticks}${ref}<path d="${line('value')}" class="chart-line"/>${curve.kind==='interval'?`<path d="${line('upper')}" class="chart-line upper"/>`:''}${points}<text x="${left}" y="${height-14}">${esc(axisTime(start))}</text><text x="${width-right}" y="${height-14}" text-anchor="end">${esc(axisTime(end))}</text></svg><div class="inspection-chart-legend"><span class="legend-measured">Measured</span>${curve.reference!==undefined?'<span class="legend-above">Above reference</span>':''}${curve.kind==='interval'?'<span class="legend-matched">Matched sustained rule</span><span class="legend-unqualified">Insufficient / unqualified samples</span>':''}</div><output id="inspection-chart-reading">Hover or focus a sample for its time and value.</output>`;
 const show=event=>{const point=event.target.closest('[data-chart-point]');if(point)$('#inspection-chart-reading').textContent=point.getAttribute('aria-label');};
 $('#inspection-chart').onpointerover=show;$('#inspection-chart').onfocusin=show;
}

function configExample(text) { return esc(state.catalog.frontend==='wp'?'diagnostics:\n'+text.split('\n').map(line=>'  '+line).join('\n'):text); }
function showSources() {
 const t=target();dialog('Source setup','PRIVATE BACKEND CONFIGURATION',`<p>Connections are read from the private configuration used to start this UI. A configured connection is not a connectivity test; running a check records reachability errors as evidence gaps.</p>${block('Selected target',JSON.stringify(t,null,2))}${state.catalog.links?.grafana?`<p><a href="${esc(state.catalog.links.grafana)}" target="_blank" rel="noopener noreferrer">Open Grafana ↗</a></p>`:''}${state.catalog.sources.map(s=>block('Boundary',s)).join('')}<div class="detail-block"><h3>Configure a target</h3><pre class="report">${configExample(`workbench:
  targets:
    my-log:
      label: My WP log
      prometheus: local
      log_namespace: your-client-namespace
      log_id: "1001"
      node_id: your-mapped-node
      # Optional existing metadata views:
      # wp diag automatically binds this target to its WP context.
      # Legacy wpdiag: wpcli: local
      log_name: your-log-name
      metadata:
        etcd: http://127.0.0.1:2379
        prefix: your-client-prefix/woodpecker
      # Optional explicit association; disabled by default:
      # milvus:
      #   prometheus: local
      #   channel: your-milvus-channel`)}</pre><p>For wp diag serve, put this workbench section under diagnostics in cli.yaml and restart wp diag serve. Legacy wpdiag serve uses a separate wpdiag.yaml. Do not infer a client metadata prefix from the WP service prefix.</p></div><p><a href="/advanced">Open independent source tools in Advanced ↗</a></p>`);
}
function showAgent() {
 const agent=state.catalog.agent;
 dialog('Local Agent setup','CODEX / CLAUDE · SUPPLIED FINDINGS ONLY',`<p class="scope-note">Current provider: <b>${esc(agent.provider||'claude')}</b> · Authentication: <b>${esc(agent.auth||'api_key')}</b></p>${block('Status',agent.message||'Agent is disabled.')}<button id="refresh-agent">Check local login / executable</button><div class="detail-grid"><section class="detail-block"><h3>Codex · use existing CLI login</h3><pre class="report">${configExample(`workbench:
  agent:
    enabled: true
    provider: codex
    auth: cli
    executable: codex
    timeout_ms: 120000
    # model: an-available-model`)}</pre><p>Run these commands as the user who starts the diagnostic tool:</p><pre class="report">codex --version
codex login status
# If login is missing:
codex login</pre><p>Uses the CLI's existing login. No API key is entered in this page. Codex CLI 0.166.0 was validated; the adapter requires its strict config, ignore-user-config and ephemeral flags.</p></section><section class="detail-block"><h3>API-key authentication · optional</h3><pre class="report"># Codex:
provider: codex
auth: api_key
api_key_env: CODEX_API_KEY

# Claude Code:
provider: claude
auth: api_key
api_key_env: ANTHROPIC_API_KEY</pre><p>For wp diag, these fields belong under diagnostics.workbench.agent in cli.yaml. Legacy wpdiag uses workbench.agent. Set the named environment variable in the server process.</p></section></div><p class="scope-note">Edit the private YAML and restart the diagnostic server. Analysis starts only after you select Analyze findings on an abnormal result. Shell, search, images, plugins and MCP access are disabled for Codex; unsupported interaction/tool requests fail the analysis. Source evidence remains available.</p>`);
 $('#refresh-agent').onclick=async()=>{
  const button=$('#refresh-agent');button.disabled=true;button.textContent='Checking local authentication…';
  try {const catalog=await api('/api/wal/catalog');state.catalog.agent=catalog.agent;render();showAgent();}catch(e){button.disabled=false;button.textContent=e.message;}
 };
}
function showCatalog() {
 const used=new Set(ids());dialog('Evidence catalog','VERSIONED CHECK DEFINITIONS',`<p>${state.catalog.catalog.evidence.length} design points. Some are deferred and show Not implemented; this is not a claim that all are automated checks. Highlighted entries belong to the selected problem.</p><input class="catalog-search" id="catalog-search" type="search" placeholder="Filter by ID, step or metric" aria-label="Search evidence catalog"><div id="catalog-entries">${state.catalog.catalog.evidence.map(d=>`<button class="catalog-entry" data-evidence="${d.id}" data-search="${esc((d.id+' '+d.title+' '+d.metrics.join(' ')).toLowerCase())}"><strong>${used.has(d.id)?'● ':''}${d.id} · ${esc(d.title)}</strong><p>${esc(d.source)} · ${esc(d.disposition)} · ${esc(d.group)}</p></button>`).join('')}</div>`);
}
async function watch(id,generation,analysis=false) {
 for(;;) {
  const job=await api('/api/runs/'+id);
  if(generation!==state.generation)return;
  if(analysis)state.analysisJob=job;else{state.job=job;state.events=job.wal_events||[];}
  if(job.report_ready){
   const report=await api('/api/runs/'+id+'/report');
   if(generation!==state.generation)return;
   if(analysis){state.analysis=report;}else{state.report=report;state.busy=false;state.filter=report.evidence.some(e=>e.status==='abnormal')?'abnormal':'all';}
   render();return;
  }
  render();await new Promise(resolve=>setTimeout(resolve,600));
 }
}
async function run(check) {
 if(state.busy){if(state.job)await api('/api/runs/'+state.job.id+'/cancel',{});return;}
 const q={problem:state.problem,check:check||undefined,target:state.target,include_milvus:$('#milvus').checked,...timeSelection()};
 state.generation++;const gen=state.generation;state.report=null;state.job=null;state.analysis=null;state.analysisJob=null;state.events=[];state.busy=true;state.error='';state.filter='all';render();
 try{state.job=await api('/api/wal/runs',q);render();await watch(state.job.id,gen);}catch(err){state.busy=false;throw err;}
}
function download(text,name,type='application/json') { const a=document.createElement('a'),url=URL.createObjectURL(new Blob([text],{type}));a.href=url;a.download=name;a.click();setTimeout(()=>URL.revokeObjectURL(url),2000); }
async function exportReport() { const res=await fetch('/api/runs/'+state.job.id+'/report');if(!res.ok)throw new Error('Stored report was evicted or is unavailable.');download(await res.text(),`wpdiag-${state.report.problem}-${state.job.id}.json`); }
function markdown() { const r=state.report;let text=`# ${problem().title}\n\nTarget: ${r.target.label}\nWindow: ${rangeLabel()}\nStatus: ${r.status}; complete: ${r.complete}\n\nRule matches are initial findings, not root-cause verdicts. Missing evidence is not a pass.\n\n`;for(const e of r.evidence)text+=`## ${e.id} · ${state.defs[e.id].title}\n\n${e.status_label||labels[e.status]}: ${e.value}\n\n${e.note}\n\nRule: ${state.defs[e.id].rule}\nLimits: ${state.defs[e.id].limits}\nOriginal observations: ${e.observation_ids.join(', ')||'None'}\n\n`;text+='\nExport the JSON report for original measurements and executed queries.\n';download(text,`wpdiag-${r.problem}-${state.job.id}.md`,'text/markdown'); }
async function packet() { const p=await api('/api/wal/runs/'+state.job.id+'/packet');dialog('Agent input preview','STORED REPORT · FINDINGS & RELEVANT CONTEXT',`<p>${p.findings.length} abnormal findings. Exact input is derived on the backend, with the stored report SHA-256. Raw sample matrices and credentials are excluded.</p><pre class="report">${esc(JSON.stringify(p,null,2))}</pre><button id="download-packet">↓ Export findings packet</button>`);$('#download-packet').onclick=()=>download(JSON.stringify(p,null,2),`wpdiag-findings-${state.job.id}.json`); }
async function analyze() { const generation=state.generation;state.analysis=null;state.analysisJob={status:'running',report_ready:false};render();try{state.analysisJob=await api('/api/wal/runs/'+state.job.id+'/analyze',{});render();await watch(state.analysisJob.id,generation,true);}catch(e){if(!state.analysisJob?.id)state.analysisJob=null;throw e;} }
async function history() {
 const list=await api('/api/runs');
 dialog('Recent runs','THIS SERVER SESSION',`<p>Only bounded in-memory history is retained. Restarting the server clears this list; exported reports remain yours.</p>${list.filter(j=>j.kind==='wal').map(j=>`<button class="catalog-entry" data-history="${j.id}"><strong>${esc(j.status)}</strong><p>${esc(stamp(j.created_at))} · ${j.id}</p></button>`).join('')||'<div class="empty-list">No WAL runs in this session yet.</div>'}`);
}
async function loadHistory(id) {
 const generation=++state.generation;
 const job=await api('/api/runs/'+id);if(!job.report_ready)throw new Error('This run is still collecting. Wait for completion.');
 const r=await api('/api/runs/'+id+'/report');if(generation!==state.generation)return;state.problem=r.problem;state.target=r.target.id;$('#instance').value=state.target;$('#milvus').checked=r.evidence.some(e=>e.id.startsWith('MV'));state.job=job;state.report=r;state.analysis=null;state.analysisJob=null;state.filter=r.evidence.some(e=>e.status==='abnormal')?'abnormal':'all';state.error='';
 if(r.time_range.mode==='historical'){ $('#period').value='custom';$('#from').value=toInput(new Date(r.time_range.start));$('#to').value=toInput(new Date(r.time_range.end)); }
 closeDialog();render();
 const jobs=await api('/api/runs');if(generation!==state.generation)return;const analysis=jobs.find(j=>j.kind==='analysis'&&j.parent_id===id);
 if(analysis){const job=await api('/api/runs/'+analysis.id);if(generation!==state.generation)return;state.analysisJob=job;render();await watch(analysis.id,generation,true);}
}
function toInput(d) { return $('#zone').value==='utc'?d.toISOString().slice(0,19):new Date(+d-d.getTimezoneOffset()*60000).toISOString().slice(0,19); }
async function action(name) {
 switch(name){case 'close':closeDialog();break;case 'sources':showSources();break;case 'agent':showAgent();break;case 'catalog':showCatalog();break;case 'export':await exportReport();break;case 'markdown':markdown();break;case 'packet':await packet();break;case 'analyze':await analyze();break;case 'cancel-analysis':await api('/api/runs/'+state.analysisJob.id+'/cancel',{});await watch(state.analysisJob.id,state.generation,true);break;case 'export-analysis':{const r=await fetch('/api/runs/'+state.analysisJob.id+'/report');if(!r.ok)throw new Error('Analysis result is unavailable.');download(await r.text(),'wpdiag-analysis-'+state.analysisJob.id+'.json');break;}}
}
function error(err) {state.error=String(err.message||err);render();}
document.addEventListener('click', async e=>{try{
 const p=e.target.closest('[data-problem]');if(p){state.problem=p.dataset.problem;$('#milvus').checked=false;reset();return;}
 const f=e.target.closest('[data-filter]');if(f){state.filter=f.dataset.filter;render();return;}
 const d=e.target.closest('[data-evidence]');if(d){showEvidence(d.dataset.evidence);return;}
 const o=e.target.closest('[data-observation]');if(o){const res=await fetch(`/api/wal/runs/${state.job.id}/observation/${encodeURIComponent(o.dataset.observation)}`);if(!res.ok)throw new Error('Original observation unavailable.');dialog('Original source observation','EXACT COLLECTED JSON',`<pre class="report">${esc(await res.text())}</pre>`);return;}
 const h=e.target.closest('[data-history]');if(h){await loadHistory(h.dataset.history);return;}
 const a=e.target.closest('[data-action]');if(a)await action(a.dataset.action);
 }catch(err){error(err);}});
$('#run')?.addEventListener('click',()=>run().catch(error));
$('#question-header').addEventListener('click',e=>{if(e.target.closest('#run'))run().catch(error);});
$('#catalog').onclick=showCatalog;$('#data-settings').onclick=showSources;$('#agent-settings').onclick=showAgent;$('#history').onclick=()=>history().catch(error);$('#close').onclick=closeDialog;
$('#overlay').onclick=e=>{if(e.target===$('#overlay'))closeDialog();};
document.addEventListener('keydown',e=>{
 if($('#overlay').hidden)return;
 if(e.key==='Escape')closeDialog();
 if(e.key==='Tab'){const nodes=[...$('.drawer').querySelectorAll('button:not(:disabled),a[href],input:not(:disabled),select:not(:disabled),[tabindex="0"]')].filter(n=>!n.hidden&&n.getClientRects().length);if(!nodes.length){e.preventDefault();return;}const first=nodes[0],last=nodes.at(-1);if(e.shiftKey&&(document.activeElement===first||document.activeElement===$('.drawer'))){e.preventDefault();last.focus();}else if(!e.shiftKey&&document.activeElement===last){e.preventDefault();first.focus();}}
});
document.addEventListener('input',e=>{if(e.target.id==='catalog-search'){const q=e.target.value.toLowerCase();document.querySelectorAll('[data-search]').forEach(n=>{n.hidden=!n.dataset.search.includes(q);});}});
$('#instance').onchange=()=>{state.target=$('#instance').value;$('#milvus').checked=false;reset();};
for(const id of ['period','from','to','milvus'])$('#'+id).onchange=reset;
$('#zone').onchange=()=>{if(state.report?.time_range.mode==='historical'){ $('#from').value=toInput(new Date(state.report.time_range.start));$('#to').value=toInput(new Date(state.report.time_range.end)); }render();};
(async()=>{try{
 state.catalog=await api('/api/wal/catalog');state.defs=Object.fromEntries(state.catalog.catalog.evidence.map(d=>[d.id,d]));state.target=state.catalog.default_target||state.catalog.targets[0]?.id||'';
 $('#instance').innerHTML=state.catalog.targets.map(t=>`<option value="${esc(t.id)}">${esc(t.label)}</option>`).join('')||'<option>No configured target</option>';
 $('#instance').value=state.target;
 const now=new Date(Date.now()-1000);$('#to').value=toInput(now);$('#from').value=toInput(new Date(+now-15*60000));
 $('#product-boundary').onclick=()=>dialog('Scope and behavior','WOODPECKER WAL FIRST','<p>Seven problem entries organize deterministic read-only evidence checks. Optional explicitly associated Milvus WAL observations add upstream context.</p><p>General Milvus search, query, indexing and data-file persistence are outside this workbench. Existing source tools remain in Advanced.</p><p>Metadata is a current snapshot using existing wpcli commands. No new dynamic metadata engine or repairs are introduced.</p><p>Trace/log correlation and historical readable tail are not silently inferred. Deferred points show Not implemented. A completed run can still contain missing evidence.</p><p>Agent analysis is an optional interpretation of an immutable stored findings packet. It cannot query systems or execute actions.</p>');
 render();if(!state.target){state.error='No target is configured. Add a Prometheus connection or workbench.targets in the private server YAML, then restart.';render();}
 }catch(err){$('#question-header').innerHTML='<h1>WAL Troubleshooting</h1>';$('#request-error').hidden=false;$('#request-error').textContent=err.message;}})();

window.addEventListener('resize',()=>{if(!$('#overlay').hidden&&$('#inspection-curve'))$('#inspection-curve').dispatchEvent(new Event('change'));});
