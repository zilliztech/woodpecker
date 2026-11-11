/* Shared, requirement-driven connection form. Defaults seed drafts; submitted values win. */
'use strict';
let diagnosticBindings=null,diagnosticBindingProfiles={},diagnosticBindingsRevision=0,connectionContext=null,connectionDrafts={},clientMetaDraft=null,traceSelections={};
const emptyComponent=()=>({name:'',metadata:{},metrics:'',trace:'',metric_labels:{}});
function initialBindings(){
 if(diagnosticBindings)return diagnosticBindings;
 connectionContext=state.info.session.context||'';
 const defaults=state.info.context_defaults?.[connectionContext]||{};
 diagnosticBindings=structuredClone({client:defaults.client||emptyComponent(),server:defaults.server||emptyComponent()});
 clientMetaDraft={};
 for(const role of ['client','server']){diagnosticBindings[role]??=emptyComponent();diagnosticBindings[role].metadata={};}
 diagnosticBindings.client.metadata=clientMetaDraft;
 return diagnosticBindings;
}
function sharedContext(){initialBindings();return connectionContext||'';}
function sharedMetadata(){initialBindings();return clientMetaDraft||{};}
function saveDiagnosticBindings(){} // Current request drafts are kept in memory only.
function checkComponent(def){return def.requires?.component||def.component||'server';}
function configuredDefinitions(){const mode=$('#mode').value;if(mode!=='scenario')return state.catalog.checks.filter(c=>c.source===mode&&c.name===$('#atomic-check').value);const scenario=state.catalog.scenarios.find(s=>s.id===state.scenario);return state.catalog.checks.filter(c=>scenario?.checks.includes(c.id));}
function componentEnabled(role){return configuredDefinitions().some(def=>checkComponent(def)===role);}
function sourceConfigured(scope,source){if(!scope)return false;const ref=scope[source];if(ref==='__manual')return !!scope[source+'_endpoint'];return !!state.info.connections?.[source]?.some(c=>c.enabled&&c.name===ref);}
function anyDiagnosticSource(source){return ['client','server'].some(role=>sourceConfigured(initialBindings()[role],source));}
function checkSourceConfigured(def){const v=initialBindings()[checkComponent(def)];return sourceConfigured(v,def.source);}
function componentName(role){return role==='client'?'Client':'Server';}
function traceSelection(){return traceSelections.current??={trace_id:'',request_id:''};}
function requirementsForChecks(defs){
 const req={definitions:defs,roles:{},metadata:false,admin:false,contextRequired:false,window:defs.length>0,trace:false,requestIDSupported:false};
 for(const def of defs){
  const role=checkComponent(def),rules=def.requires||{},need=req.roles[role]??={sources:new Set(),parameters:new Set(),optional:new Set(),fields:new Set(),wpMetrics:false,trace:false};
  need.sources.add(def.source);need.wpMetrics ||=def.source==='metrics';need.trace ||=def.source==='trace';
  for(const key of rules.parameters||['window']){need.parameters.add(key);need.fields.add(key);}
  for(const key of rules.optional_parameters||[]){need.optional.add(key);need.fields.add(key);}
  const conn=sourceConnection(initialBindings()[role],'trace');
  for(const [key,condition]of Object.entries(rules.conditional_parameters||{})){
   const supported=condition==='request_id_tag'?!!conn?.request_id_tag:!!conn?.identity_tags?.[condition.split('.').at(-1)];
   if(supported){need.optional.add(key);need.fields.add(key);}
  }
 }
 const traces=Object.entries(req.roles).filter(([,need])=>need.trace);req.trace=traces.length>0;
 req.requestIDSupported=req.trace&&traces.every(([role])=>!!sourceConnection(initialBindings()[role],'trace')?.request_id_tag);
 return req;
}
function diagnosticTraceParameters(defs=configuredDefinitions()){
 const req=requirementsForChecks(defs),selection=traceSelection(),out={};
 if(req.trace&&selection.trace_id.trim())out.trace_id=selection.trace_id.trim();
 if(req.requestIDSupported&&selection.request_id.trim())out.request_id=selection.request_id.trim();
 return out;
}
function commandCheckDefinitions(c){const draft=commandDrafts[commandDraftKey(c)]||{arguments:{}},name=$('[data-command-argument="0"]')?.value||draft.arguments[c.arguments[0]?.name]||'';if(c.name==='diag scenario')return state.catalog.checks.filter(d=>state.catalog.scenarios.find(s=>s.id===name)?.checks.includes(d.id));const source=c.name==='diag metrics check'?'metrics':c.name==='diag trace check'?'trace':'';return state.catalog.checks.filter(d=>source&&d.source===source&&d.name===name);}
function connectionRequirements(c){if(!c)return requirementsForChecks(configuredDefinitions());const connection=commandConnection(c);if(connection==='telemetry')return requirementsForChecks(commandCheckDefinitions(c));return {definitions:[],roles:{},metadata:['metadata','mixed','etcd'].includes(connection),raw:connection==='etcd',admin:['service','mixed'].includes(connection),contextRequired:['service','mixed'].includes(connection),window:false};}
function requirementKey(req){return JSON.stringify({checks:req.definitions.map(d=>d.id),metadata:req.metadata,raw:req.raw,admin:req.admin});}
function diagnosticRequestBindings(defs=configuredDefinitions()){
 const b={},req=requirementsForChecks(defs);
 for(const [role,need]of Object.entries(req.roles)){
  const v=structuredClone(initialBindings()[role]);delete v._labels_error;delete v.metadata;
  for(const key of ['log_namespace','log_id','reader_name','node_id','path'])if(!need.fields.has(key))delete v[key];
  if(need.wpMetrics){if(v.metrics==='__manual')v.metrics='';else delete v.metrics_endpoint;}else{v.metrics='';delete v.metrics_endpoint;delete v.metric_labels;delete v.path;}
  delete v.milvus;
  if(need.trace){if(v.trace==='__manual')v.trace='';else delete v.trace_endpoint;}else{v.trace='';delete v.trace_endpoint;delete v.trace_service;}
  b[role]=v;
 }
 return b;
}
function diagnosticBindingSummary(){return Object.keys(connectionRequirements().roles).map(role=>componentName(role)+': '+(initialBindings()[role].name||sharedContext()||'form values')).join(' + ');}
function diagnosticScopeHTML(){return Object.keys(connectionRequirements().roles).map(role=>{const v=initialBindings()[role],pairs=role==='client'?[['Metrics Log namespace',v.log_namespace],['Log ID',v.log_id],['Reader',v.reader_name]]:[['Metrics node_id',v.node_id],['WAL path',v.path]];return `<b>${componentName(role)} scope</b><dl>${pairs.filter(([,value])=>value).map(([k,value])=>`<div><dt>${esc(k)}</dt><dd>${esc(value)}</dd></div>`).join('')||'<div><dt>Scope</dt><dd>Selected source populations</dd></div>'}</dl>`;}).join('');}
function missingForCheck(def){const role=checkComponent(def),v=initialBindings()[role],missing=[];if(!checkSourceConfigured(def))missing.push(componentName(role)+' '+(def.source==='metrics'?'Prometheus':'Trace'));if(sourceConfigured(v,def.source)&&def.requires?.parameters.includes('node_id')&&!String(v.node_id||'').trim())missing.push(componentName(role)+' Metrics node_id');return missing;}
function bindingsError(req=connectionRequirements()){
 for(const source of requiredEvidenceSources(req)){const address=sharedEvidenceAddress(req,source);if(address.mixed)return 'Enter one '+(source==='metrics'?'Prometheus':'Trace')+' address for these checks.';if(address.value&&!normalizedSourceURL(address.value))return 'Use an HTTP(S) '+(source==='metrics'?'Prometheus':'Trace')+' address without embedded credentials or query parameters.';}
 if(req.contextRequired&&!(state.info.available_contexts||[]).includes(sharedContext()))return 'Choose a WP context with an Admin endpoint.';
 const meta=sharedMetadata();if(req.metadata&&(!meta.etcd||!req.raw&&!meta.prefix))return req.raw?'Enter etcd endpoints.':'Enter client etcd endpoints and a full metadata prefix.';
 for(const [role,need]of Object.entries(req.roles)){const v=initialBindings()[role];
  if(need.wpMetrics&&v.metrics==='__manual'&&(!v.metrics_endpoint||!Object.keys(v.metric_labels||{}).length))return componentName(role)+': Prometheus endpoint and population labels are required.';
  if(need.sources.has('trace')&&v.trace==='__manual'&&(!v.trace_endpoint||!v.trace_service))return componentName(role)+': Trace endpoint and Trace service are required.';
  if(need.wpMetrics&&v._labels_error)return componentName(role)+': metric labels must be a JSON object of strings.';
 }
 if(req.definitions.length===1){const missing=missingForCheck(req.definitions[0]);if(missing.length)return 'Required: '+missing.join(', ');}
 if(req.definitions.length>1){for(const def of req.definitions){if(checkSourceConfigured(def)){const missing=missingForCheck(def);if(missing.length)return 'Required: '+missing.join(', ');}}if(!req.definitions.some(d=>checkSourceConfigured(d)))return 'Configure at least one required Prometheus or Trace source.';}
 return '';
}
function checkRequirementText(def){const r=def.requires||{component:checkComponent(def),connections:[def.source],parameters:['window']},items=[...r.connections.map(s=>componentName(r.component)+' '+(s==='metrics'?'Prometheus':'Trace')),...r.parameters.map(p=>({window:'incident From / To',node_id:'Metrics node_id'}[p]||p))],v=initialBindings()[checkComponent(def)],ref=v[def.source],conn=state.info.connections[def.source]?.find(c=>c.name===ref),endpoint=ref==='__manual'?v[def.source+'_endpoint']:conn?.endpoint;return 'Key: '+def.name+'\nLegacy reference: '+def.id+'\nRequires: '+items.join(' · ')+(r.optional_parameters?.length?'\nOptional: '+r.optional_parameters.join(', '):'')+(Object.keys(r.conditional_parameters||{}).length?'\nConditional: '+Object.entries(r.conditional_parameters).map(([k,c])=>k+' → '+c).join(' · '):'')+(endpoint?'\nConnection: '+endpoint:'')+((def.source==='metrics'?def.metrics:def.operations)?.length?'\nEvidence: '+(def.source==='metrics'?def.metrics:def.operations).join(', '):'')+(missingForCheck(def).length?'\nMissing: '+missingForCheck(def).join(', '):'');}
function contextConnectionLink(value){
 try{const u=new URL(value);if(['http:','https:'].includes(u.protocol)&&!u.username&&!u.password)return `<a href="${esc(u.href)}" target="_blank" rel="noopener noreferrer">${esc(value)} ↗</a>`;}catch{}
 return esc(value);
}
function connectionContextHTML(req){
 const ctx=state.info.connection_profiles?.[sharedContext()]?.server||{},rows=[];
 for(const source of ['metrics','trace']){const seen=new Set();for(const role of ['client','server']){const v=initialBindings()[role],ref=v[source],conn=state.info.connections[source]?.find(c=>c.name===ref),endpoint=ref==='__manual'?v[source+'_endpoint']:conn?.endpoint;if(!ref&&!endpoint)continue;const key=ref+'|'+endpoint;if(seen.has(key))continue;seen.add(key);rows.push([source==='metrics'?'Prometheus':'Trace',endpoint||ref]);}}
 const grafana=ctx.grafana;if(grafana)rows.push(['Grafana',grafana]);
 const mappings=Object.entries(ctx.node_admin_urls||{});
 return `<div class="shared-context server-context">
 <label><span class="scope-field-label">Server context ${req.contextRequired?'<em class="required-mark">Required</em>':'<span class="optional-mark">Environment</span>'}</span><select data-shared-context><option value="">No context / explicit connections</option>${state.info.contexts.map(name=>`<option value="${esc(name)}" ${name===sharedContext()?'selected':''}>${esc(name)}</option>`).join('')}</select></label>
 ${state.tab==='diagnostics'?'':'<div class="context-config-actions"><button type="button" class="text-link" data-view-config>View cli.yaml ↗</button><span>Read-only</span></div>'}
 <p class="field-hint">Server entry point, host map and observability connections. Client meta is selected separately.</p>
 <details class="binding-details" data-binding-panel="server-context-details"><summary>Context connection details</summary><div class="binding-fields">
 <div class="context-connections-scroll" role="region" aria-label="Context service connections" tabindex="0"><dl class="context-connections"><div><dt>Admin entry point</dt><dd>${esc(ctx.endpoint||'Not configured')}</dd></div>${rows.map(([key,value])=>`<div><dt>${key}</dt><dd>${contextConnectionLink(value)}</dd></div>`).join('')}</dl></div>
 <h4>Host map · node Admin URLs</h4>
 ${mappings.length?`<div class="host-map-scroll" role="region" aria-label="Node Admin URL mappings" tabindex="0"><table class="host-map-table"><tbody>${mappings.map(([node,url])=>`<tr><th scope="row"><code>${esc(node)}</code></th><td class="host-map-arrow" aria-hidden="true">→</td><td><code>${esc(url)}</code></td></tr>`).join('')}</tbody></table></div>`:'<p class="field-hint">No explicit mapping; native WP address resolution applies.</p>'}
 <p class="field-hint">Server configuration comes from cli.yaml. Per-run evidence overrides below do not edit the file.</p>
 </div></details></div>`;
}
function clientMetaHTML(req,input){
 const m=sharedMetadata();
 return `<section class="component-config client-meta"><h3>${req.raw?'Etcd connection':'Client meta'}</h3>${input('client','metadata.etcd','Etcd endpoints',m.etcd,'http://127.0.0.1:2379',true)}${req.raw?'':input('client','metadata.prefix','Full metadata prefix',m.prefix,'tenant-instance/woodpecker',true)}<p class="field-hint">${req.raw?'Reads keys using these etcd endpoints.':'Reads the selected tenant metadata namespace.'} Click a field for recent values. Server context changes keep these fields.</p></section>`;
}
function sourceConnection(scope,source){return state.info.connections[source]?.find(c=>c.name===scope?.[source]);}
function sourceURL(scope,source){return scope?.[source+'_endpoint']||sourceConnection(scope,source)?.endpoint||'';}
function normalizedSourceURL(value){try{const u=new URL(value.trim());return ['http:','https:'].includes(u.protocol)&&!u.username&&!u.password&&!u.search&&!u.hash?u.href.replace(/\/+$/,''):'';}catch{return '';}}
function requiredEvidenceSources(req){return ['metrics','trace'].filter(source=>source==='metrics'?Object.values(req.roles).some(need=>need.wpMetrics):Object.values(req.roles).some(need=>need.trace));}
function sharedEvidenceAddress(req,source){const values=[...new Set(Object.keys(req.roles).filter(role=>req.roles[role].sources.has(source)).map(role=>sourceURL(initialBindings()[role],source).trim()).filter(Boolean))];return {value:values.length===1?values[0]:'',mixed:values.length>1};}
function setSharedEvidenceAddress(source,value){
 const b=initialBindings(),endpoint=value.trim(),url=normalizedSourceURL(endpoint),contextRef=state.info.connection_profiles?.[sharedContext()]?.server?.[source==='metrics'?'prometheus':'trace'];
 for(const role of ['client','server']){
  const v=b[role],previous=sourceConnection(v,source),reference=[v[source],contextRef].find(ref=>state.info.connections[source]?.some(c=>c.enabled&&c.name===ref&&url&&normalizedSourceURL(c.endpoint||'')===url));
  if(!endpoint){v[source]='';delete v[source+'_endpoint'];continue;}
  if(reference){v[source]=reference;delete v[source+'_endpoint'];continue;}
  if(source==='metrics'&&!Object.keys(v.metric_labels||{}).length&&previous?.metric_labels)v.metric_labels=structuredClone(previous.metric_labels);
  if(source==='trace'&&!v.trace_service&&previous?.trace_service)v.trace_service=previous.trace_service;
  v[source]='__manual';v[source+'_endpoint']=endpoint;
 }
}
function rememberEvidenceConnections(report){} // Successful queries are recorded server-side.
function connectionFieldsHTML(req,part='all'){
 const input=(role,key,label,value,placeholder='',required=false,hint='')=>`<label><span class="scope-field-label">${label}${required?'<em class="required-mark">Required</em>':''}</span><input ${`data-recent-field="${key}" autocomplete="off"`} data-binding-role="${role}" data-binding-key="${key}" value="${esc(value||'')}" placeholder="${esc(placeholder)}">${hint?`<small class="field-hint">${hint}</small>`:''}</label>`;
 let parameters='';
 const fieldInfo={
  log_namespace:['Log namespace (log_ns)','example-bucket/tenant/wal',`Object storage: bucket + "/" + root_path. Use the emitted log_ns label; this is not the etcd meta prefix.`],
  log_id:['Log ID (log_id)','All matched logs',''],
  reader_name:['Reader name','All matched readers',''],
  node_id:['Node ID (node_id)','woodpecker-0','Required for node-level checks; Log-level checks may use it as an optional filter.'],
  path:['WAL filesystem path','/woodpecker/data','Server filesystem path; this is not the client object-storage root_path.']
 };
 for(const [role,need]of Object.entries(req.roles)){
  const v=initialBindings()[role],manual=need.wpMetrics&&v.metrics==='__manual'||need.trace&&v.trace==='__manual',conn=sourceConnection(v,'trace');
  const scope=Object.entries(fieldInfo).filter(([key])=>need.fields.has(key)).map(([key,[label,placeholder,hint]])=>input(role,key,label,v[key],placeholder,need.parameters.has(key),hint)).join('');
  const traceCoverage=need.trace?`<p class="field-hint trace-scope-note">${Object.keys(conn?.identity_tags||{}).length?'Trace identity filters use this source’s configured tag mappings. Actual tag coverage is reported with the evidence.':'Trace is scoped to its service. This source has no WP identity tag mapping; Log or Node filters do not establish a Trace association.'}</p>`:'';
  const sourceNames=[...need.sources].map(source=>source==='metrics'?'Prometheus':'Trace').join(' + ');
  parameters+=`<section class="component-config"><h3>${componentName(role)} evidence filters</h3><p class="field-hint evidence-filter-source">Queries ${sourceNames}. No etcd connection required.</p>${scope||'<p class="field-hint">This check has no additional Log or Node parameters.</p>'}${traceCoverage}
  ${need.wpMetrics||need.trace?`<details class="binding-details" data-binding-panel="${role}-evidence-scope" ${manual&&(need.wpMetrics&&v.metrics==='__manual'&&!Object.keys(v.metric_labels||{}).length||need.trace&&v.trace==='__manual'&&!v.trace_service)?'open':''}><summary>Evidence scope <span>${manual?'Review for this address':'Optional overrides'}</span></summary><div class="binding-fields">
  ${need.trace&&v.trace?input(role,'trace_service','Trace service',v.trace_service,'Use the context’s saved search service',v.trace==='__manual'):''}
  ${need.wpMetrics?`<label>Exact metric labels (JSON)<textarea data-binding-role="${role}" data-binding-labels rows="3" placeholder='{"namespace":"my-stack"}'>${esc(Object.keys(v.metric_labels||{}).length?JSON.stringify(v.metric_labels,null,2):'')}</textarea></label><p class="field-hint">Saved source labels apply unless overridden. A new address requires an explicit population.</p>`:''}
  </div></details>`:''}
  </section>`;
 }
 if(req.trace){const selection=traceSelection();parameters+=`<section class="component-config trace-selection"><h3>Trace selection</h3><label>Trace ID <span class="optional-mark">Optional</span><input data-recent-field="trace_id" autocomplete="off" data-trace-query="trace_id" value="${esc(selection.trace_id)}" placeholder="Exact 16/32-digit hexadecimal ID"></label>${req.requestIDSupported?`<label>Request ID <span class="optional-mark">Optional</span><input data-recent-field="request_id" autocomplete="off" data-trace-query="request_id" value="${esc(selection.request_id)}" placeholder="Exact request ID"></label>`:'<p class="field-hint">Request ID filtering requires request_id_tag on every selected Trace source.</p>'}</section>`;}
 const sources=requiredEvidenceSources(req).map(source=>{const address=sharedEvidenceAddress(req,source),label=source==='metrics'?'Prometheus':'Trace (Jaeger)';return `<div class="evidence-address"><span class="scope-field-label">${label}<em class="required-mark">${req.definitions.length===1?'Required':'Used by '+source+' checks'}</em>${helpIcon('connection')}</span><div class="evidence-address-controls"><input aria-label="${label} query API address" type="url" data-shared-evidence="${source}" data-recent-field="${source}_endpoint" autocomplete="off" spellcheck="false" value="${esc(address.value)}" placeholder="${source==='metrics'?'http://127.0.0.1:9090':'http://127.0.0.1:16686'}"><button type="button" class="ghost connection-check-button" data-check-connection="${source}">Check connection</button></div>${address.mixed?'<small class="field-hint">Client and Server have different addresses. Enter one address for these checks.</small>':''}<div class="connection-check-feedback" data-connection-feedback="${source}" role="status" aria-live="polite"></div><p class="evidence-rules-link"><button type="button" class="text-link" data-view-rules="${source}">${source==='metrics'?'Metrics':'Trace'} rules ↗</button></p></div>`;}).join('');

 const contextEditor=sources?`<details class="context-evidence-overrides binding-details" data-binding-panel="context-evidence" ${req.definitions.some(def=>!checkSourceConfigured(def))?'open':''}><summary>Evidence connections in Server context</summary><div class="binding-fields">${sources}<p class="field-hint">History keeps the last 5 addresses successfully checked or queried. Select a suggestion or type an address.</p></div></details>`:'';
 const identity=`<div class="server-context-bundle">${connectionContextHTML(req)}${contextEditor}</div>`+(req.metadata?clientMetaHTML(req,input):'');
 return part==='identity'?identity:part==='sources'?parameters:identity+parameters;
}
function bindConnectionFields(host,req,changed){
 const notify=rebuild=>{saveDiagnosticBindings();changed(rebuild);};
 for(const button of host.querySelectorAll('[data-check-connection]')){const source=button.dataset.checkConnection;updateConnectionFeedback(host,source,req);button.onclick=()=>checkEvidenceConnection(host,source,req);}

 for(const control of host.querySelectorAll('[data-trace-query]'))control.oninput=()=>{traceSelection()[control.dataset.traceQuery]=control.value;notify(false);};
 for(const select of host.querySelectorAll('[data-shared-context]'))select.onchange=()=>switchSharedContext(select.value).catch(error);
 for(const control of host.querySelectorAll('[data-shared-evidence]')){
  control.oninput=()=>{setSharedEvidenceAddress(control.dataset.sharedEvidence,control.value);updateConnectionFeedback(host,control.dataset.sharedEvidence,req);notify(false);};
  control.onchange=()=>{const source=control.dataset.sharedEvidence,focused=document.activeElement===control;setSharedEvidenceAddress(source,control.value);notify(true);if(focused)host.querySelector(`[data-shared-evidence="${source}"]`)?.focus({preventScroll:true});};
 }

 for(const control of host.querySelectorAll('[data-binding-key]')){const change=()=>{const {bindingRole:role,bindingKey:key}=control.dataset,v=initialBindings()[role],bits=key.split('.');let parent=v;for(const part of bits.slice(0,-1))parent=parent[part]??={};parent[bits.at(-1)]=control.value.trim();notify(key==='metrics'||key==='trace');};control.onchange=change;if(control.tagName==='INPUT')control.oninput=()=>{const {bindingRole:role,bindingKey:key}=control.dataset,v=initialBindings()[role],bits=key.split('.');let parent=v;for(const part of bits.slice(0,-1))parent=parent[part]??={};parent[bits.at(-1)]=control.value.trim();notify(false);};}
 for(const text of host.querySelectorAll('[data-binding-labels]'))text.oninput=()=>{const v=initialBindings()[text.dataset.bindingRole];try{const labels=JSON.parse(text.value||'{}');if(!labels||Array.isArray(labels)||typeof labels!=='object'||Object.values(labels).some(x=>typeof x!=='string'))throw Error();v.metric_labels=labels;delete v._labels_error;}catch{v._labels_error=true;}notify(false);};
 for(const panel of host.querySelectorAll('[data-binding-panel]'))panel.ontoggle=()=>{connectionPanels[panel.dataset.bindingPanel]=panel.open;};
}
const connectionPanels={};
function restoreConnectionPanels(host){for(const panel of host.querySelectorAll('[data-binding-panel]'))if(Object.hasOwn(connectionPanels,panel.dataset.bindingPanel))panel.open=connectionPanels[panel.dataset.bindingPanel];}
async function switchSharedContext(name){state.commandSwitching=true;$('#run').disabled=true;if($('#execute-command'))$('#execute-command').disabled=true;saveDiagnosticBindings();const tenantScope={};for(const key of ['log_namespace','log_id','reader_name'])tenantScope[key]=initialBindings().client[key];const serverScope={};for(const key of ['node_id','path'])serverScope[key]=initialBindings().server[key];connectionContext=name;const defaults=state.info.context_defaults?.[name]||{};diagnosticBindings=structuredClone({client:defaults.client||emptyComponent(),server:defaults.server||emptyComponent()});for(const role of ['client','server']){diagnosticBindings[role]??=emptyComponent();diagnosticBindings[role].metadata??={};}Object.assign(diagnosticBindings.client,tenantScope);Object.assign(diagnosticBindings.server,serverScope);diagnosticBindings.client.metadata=clientMetaDraft;diagnosticBindingProfiles={};diagnosticBindingsRevision++;saveDiagnosticBindings();try{state.info=await api('/api/workspace/session',{action:'use-context',context:name});}finally{state.commandSwitching=false;}state.commandEditorKey='';if(state.report||state.job)reset();renderConnections();render();}
function renderDiagnosticBindings(force=false){initialBindings();const req=connectionRequirements(),key=requirementKey(req)+'|'+diagnosticBindingsRevision+'|'+state.tab;if(!force&&$('#diagnostic-components').dataset.revision===key)return;$('#diagnostic-components').innerHTML=connectionFieldsHTML(req,'identity');$('#diagnostic-observability').innerHTML=connectionFieldsHTML(req,'sources');for(const host of [$('#diagnostic-components'),$('#diagnostic-observability')]){restoreConnectionPanels(host);bindConnectionFields(host,req,bindingsChanged);}$('#diagnostic-components').dataset.revision=key;}
function bindingsChanged(rebuild){if(state.report||state.job)reset();if(rebuild){diagnosticBindingsRevision++;renderDiagnosticBindings(true);}render();}
function renderCommandConnections(c){const host=$('#command-connections'),req=connectionRequirements(c);if(!host)return;host.innerHTML=connectionFieldsHTML(req);const context=host.querySelector('[data-shared-context]');if(context)context.id='console-context';restoreConnectionPanels(host);host.dataset.metadataAuthority=metadataAuthority();bindConnectionFields(host,req,rebuild=>{if(req.metadata&&host.dataset.metadataAuthority!==metadataAuthority()){host.dataset.metadataAuthority=metadataAuthority();for(const key of commandSharedFields(c)){const input=$(`[data-shared-key="${key}"]`);if(input)input.value=commonValue(c,key);}for(const key of commandSharedFields(c))refreshDependentFields(c,key);for(const input of document.querySelectorAll('[data-command-flag]'))if(!reusableFlag(input.dataset.commandFlag)){if(input.type==='checkbox')input.checked=false;else input.value='';}}if(rebuild){diagnosticBindingsRevision++;renderCommandConnections(c);}updateCommandPreview(c);renderCommandOutput(c);});updateCommandRequirements(c,commandPayload(c).flags);}
function restoreDiagnosticBindings(b,request={}){initialBindings();if(request.source==='trace'||request.scenario)Object.assign(traceSelection(),{trace_id:request.trace_id||'',request_id:request.request_id||''});for(const role of ['client','server'])if(b[role]){const previous=diagnosticBindings[role],restored=structuredClone(b[role]);restored.metadata=previous.metadata||{};if(request.source==='metrics')for(const key of ['trace','trace_endpoint','trace_service'])restored[key]=previous[key];if(request.source==='trace')for(const key of ['metrics','metrics_endpoint','metric_labels','path'])restored[key]=previous[key];diagnosticBindings[role]=restored;if(restored.metrics_endpoint)restored.metrics='__manual';if(restored.trace_endpoint)restored.trace='__manual';}diagnosticBindingProfiles={};diagnosticBindingsRevision++;saveDiagnosticBindings();renderDiagnosticBindings(true);}

function adoptSharedContext(name){if(sharedContext()===name)return;saveDiagnosticBindings();const tenantScope={};for(const key of ['log_namespace','log_id','reader_name'])tenantScope[key]=initialBindings().client[key];const serverScope={};for(const key of ['node_id','path'])serverScope[key]=initialBindings().server[key];connectionContext=name;const defaults=state.info.context_defaults?.[name]||{};diagnosticBindings=structuredClone({client:defaults.client||emptyComponent(),server:defaults.server||emptyComponent()});for(const role of ['client','server']){diagnosticBindings[role]??=emptyComponent();diagnosticBindings[role].metadata??={};}Object.assign(diagnosticBindings.client,tenantScope);Object.assign(diagnosticBindings.server,serverScope);diagnosticBindings.client.metadata=clientMetaDraft;diagnosticBindingsRevision++;saveDiagnosticBindings();}
function adoptSharedMetadata(meta){if(!meta)return;initialBindings();clientMetaDraft={etcd:meta.etcd,prefix:meta.prefix,bucket:meta.bucket||'',root_path:meta.root_path||'',client_meta:meta.client_meta||''};diagnosticBindings.client.metadata=clientMetaDraft;diagnosticBindingsRevision++;saveDiagnosticBindings();}

function commandTimeHTML(){const label=(id,title)=>`<label>${title}<select data-shared-time="${id}">${[...$('#'+id).options].map(o=>`<option value="${esc(o.value)}" ${o.value===$('#'+id).value?'selected':''}>${esc(o.textContent)}</option>`).join('')}</select></label>`;const input=(id,title,type)=>`<label>${title}<input type="${type}" step="${type==='datetime-local'?'1':'0.01'}" data-shared-time="${id}" value="${esc($('#'+id).value)}"></label>`;return label('period','Incident interval')+label('zone','Time zone')+($('#period').value==='custom'?input('from','From','datetime-local')+input('to','To','datetime-local'):$('#period').value==='lookback'?input('lookback','Look back (minutes)','number')+input('anchor','End at (empty = now)','datetime-local'):'');}
function renderCommandTime(c){const host=$('#command-incident-window');if(!host)return;host.innerHTML=commandTimeHTML();for(const field of host.querySelectorAll('[data-shared-time]')){const update=()=>{$('#'+field.dataset.sharedTime).value=field.value;if(field.tagName==='SELECT')renderCommandTime(c);updateCommandPreview(c);};field.oninput=update;field.onchange=update;}}

// Preflight is explicit and source-local; it never runs a diagnosis.
const evidenceConnectionChecks=new Map();
function connectionProbeRequest(source,req){
 const bindings={};
 for(const role of Object.keys(req.roles)){if(!req.roles[role].sources.has(source))continue;const v=initialBindings()[role],s={context:sharedContext()};
 if(source==='metrics'){if(v.metrics==='__manual')s.metrics_endpoint=v.metrics_endpoint||'';else s.metrics=v.metrics||'';s.metric_labels=v.metric_labels||{};}
 else{if(v.trace==='__manual')s.trace_endpoint=v.trace_endpoint||'';else s.trace=v.trace||'';s.trace_service=v.trace_service||'';}
 bindings[role]=s;
 }
 return {source,bindings};
}
function connectionProbeKey(q){return sharedContext()+'|'+JSON.stringify(q);}
function updateConnectionFeedback(host,source,req){
 const button=host.querySelector(`[data-check-connection="${source}"]`),feedback=host.querySelector(`[data-connection-feedback="${source}"]`);if(!button||!feedback)return;
 const entry=evidenceConnectionChecks.get(connectionProbeKey(connectionProbeRequest(source,req)));
 button.disabled=!!entry?.busy;button.innerHTML=entry?.busy?'<i class="spinner"></i> Checking…':'Check connection';
 feedback.className='connection-check-feedback';
 if(!entry){feedback.textContent='';return;}
 if(entry.busy){feedback.textContent='Checking query API access…';return;}
 const results=entry.response?.results||[],failed=!!entry.error||results.some(r=>r.status==='failed');feedback.classList.add(failed?'failed':results.some(r=>r.status==='warning')?'warning':'ready');
 feedback.innerHTML=entry.error?esc(entry.error):results.map(r=>`<span class="connection-check-row"><strong>${esc(componentName(r.component))} · ${r.status==='ready'?'API accessible':r.status==='warning'?'Review service':'Connection failed'}</strong><br>${esc(r.message)}<span class="probe-next">${esc(r.next||'')}</span>${r.http_status?`<small>HTTP ${esc(r.http_status)}</small>`:''}<time>${esc(stamp(r.checked_at))} · ${esc(r.elapsed_ms)} ms</time></span>`).join('')+(entry.response?.history_warning?`<span>${esc(entry.response.history_warning)}</span>`:'');
}
async function checkEvidenceConnection(host,source,req){
 const q=connectionProbeRequest(source,req),key=connectionProbeKey(q);if(evidenceConnectionChecks.get(key)?.busy)return;
 const entry={busy:true};evidenceConnectionChecks.set(key,entry);updateConnectionFeedback(host,source,req);
 try{entry.response=await api('/api/workspace/connections/check',q);}catch(e){entry.error=e.message;}finally{entry.busy=false;updateConnectionFeedback(host,source,req);}
}

// Diagnostic form validation checks inputs only; evidence assessment remains in Go.
let diagnosticValidationAttempted=false,diagnosticValidationSelection='';
function diagnosticField(key,role){return document.querySelector('#diagnostics-view '+(role?`[data-binding-role="${role}"][data-binding-key="${key}"]`:`[data-shared-evidence="${key}"]`));}
function fieldCaption(input){
 const field=input.closest('label,.evidence-address,.field');if(!field)return null;
 let caption=field.querySelector('.scope-field-label')||(field.classList.contains('field')?field.querySelector('label'):null);
 if(!caption){caption=document.createElement('span');caption.className='scope-field-label';const text=[...field.childNodes].find(node=>node.nodeType===Node.TEXT_NODE&&node.textContent.trim());if(text){text.before(caption);caption.append(text);}else field.prepend(caption);}
 caption.classList.add('scope-field-label');return caption;
}
function setFieldRequirement(input,required,text='Required',alternative=false){
 if(!input)return;const caption=fieldCaption(input);if(!caption)return;
 const signature=required+'|'+text+'|'+alternative;if(caption.dataset.requirement===signature)return;
 for(const mark of caption.querySelectorAll('.command-required-badge,.required-mark,.optional-mark'))mark.remove();
 caption.insertAdjacentHTML('beforeend',required?`<em class="required-mark" ${alternative?'data-source-alternative':''}>${esc(text)}</em>`:'<span class="optional-mark">Optional</span>');caption.dataset.requirement=signature;
}
function diagnosticFormIssues(defs){
 const req=requirementsForChecks(defs),issues=new Map(),global=[];
 const add=(input,message)=>{if(input)issues.set(input,message);else global.push(message);};
 if(state.info.diagnostics_error)global.push(state.info.diagnostics_error);
 if(!defs.length)global.push('Choose a check or scenario.');
 const configured=defs.filter(checkSourceConfigured),sources=requiredEvidenceSources(req);
 for(const source of sources){const address=sharedEvidenceAddress(req,source);if(address.mixed)add(diagnosticField(source),'Enter one '+source+' address.');else if(address.value&&!normalizedSourceURL(address.value))add(diagnosticField(source),'Use an HTTP(S) query API address without embedded credentials or query parameters.');}
 if(defs.length&&!configured.length)for(const source of sources)add(diagnosticField(source),sources.length>1?'Configure at least one Prometheus or Trace source.':'Configure the required query API address.');
 for(const def of configured){const role=checkComponent(def),v=initialBindings()[role];
  for(const key of def.requires?.parameters||[]){if(['window'].includes(key))continue;if(!String(v[key]||'').trim())add(diagnosticField(key,role),'Enter '+key+'.');}
 }
 for(const [role,need]of Object.entries(req.roles)){const v=initialBindings()[role],manualMetrics=need.wpMetrics&&v.metrics==='__manual'&&!!v.metrics_endpoint?.trim(),manualTrace=need.trace&&v.trace==='__manual'&&!!v.trace_endpoint?.trim();
  if(manualTrace&&!v.trace_service?.trim())add(diagnosticField('trace_service',role),'Enter the Trace service.');
  const labels=document.querySelector(`#diagnostics-view [data-binding-role="${role}"][data-binding-labels]`);if(need.wpMetrics&&(v._labels_error||manualMetrics&&!Object.keys(v.metric_labels||{}).length))add(labels,'Enter population labels as a JSON object of strings.');
 }
 try{timeSelection();}catch(e){const ids=$('#period').value==='custom'?['from','to']:$('#period').value==='lookback'?['lookback',...($('#anchor').value?['anchor']:[])]:[];if(ids.length)for(const id of ids)add($('#'+id),e.message);else global.push(e.message);}
 return {req,configured,issues,global};
}
function diagnosticParameterExample(input){
 if(input.hasAttribute('data-binding-labels'))return ['Metric labels','Exact population selectors for this Prometheus address, as a JSON object of strings.','{"namespace":"wp-prod"}'];
 if(['from','to','anchor'].includes(input.id))return ['Incident time','Use the selected browser-local or UTC time zone. From must be earlier than To; the interval must be in the past.','2026-11-09T10:00:00'];
 if(input.id==='lookback')return ['Lookback','Positive incident duration in minutes, ending at the optional End at time or now.','45'];
 return commandParameterExample({name:'WAL diagnostics'},input);
}
function updateDiagnosticValidation(defs=configuredDefinitions()){
 const selection=[$('#mode').value,state.scenario,$('#atomic-check').value].join('|');if(selection!==diagnosticValidationSelection){diagnosticValidationSelection=selection;diagnosticValidationAttempted=false;}
 const result=diagnosticFormIssues(defs),{req,configured,issues}=result;
 for(const input of document.querySelectorAll('#diagnostic-observability [data-binding-key]')){const role=input.dataset.bindingRole,key=input.dataset.bindingKey,v=initialBindings()[role],need=req.roles[role];if(!need)continue;
  if(key==='node_id')setFieldRequirement(input,configured.some(d=>checkComponent(d)===role&&d.requires?.parameters.includes('node_id')));
  if(key==='trace_service')setFieldRequirement(input,!!(need.trace&&v.trace==='__manual'&&v.trace_endpoint?.trim()));
 }
 for(const source of requiredEvidenceSources(req)){const input=diagnosticField(source);if(!input)continue;const alternative=requiredEvidenceSources(req).length>1&&req.definitions.length>1;setFieldRequirement(input,true,alternative?'At least one':'Required',alternative);input.closest('.evidence-address').querySelectorAll('[data-workflow-help]').forEach(icon=>icon.remove());}
 for(const [role,need]of Object.entries(req.roles)){const v=initialBindings()[role],labels=document.querySelector(`#diagnostics-view [data-binding-role="${role}"][data-binding-labels]`);if(labels)setFieldRequirement(labels,!!(need.wpMetrics&&v.metrics==='__manual'&&v.metrics_endpoint?.trim()));}
 const windowSection=$('#period').closest('.workflow-step');for(const id of ['from','to','lookback'])setFieldRequirement($('#'+id),$('#period').value==='custom'?['from','to'].includes(id):$('#period').value==='lookback'&&id==='lookback');
 for(const root of [$('#diagnostic-components'),$('#diagnostic-observability'),windowSection]){root.classList.add('required-fields');decorateRequiredFields(root,{attempted:diagnosticValidationAttempted,issues,example:diagnosticParameterExample});}
 const notice=$('#diagnostic-setup');notice.closest('.workflow-step')?.querySelector('p.requirement')?.remove();notice.className='diagnostic-validation-feedback';if(notice.parentElement!==$('#run').parentElement)$('#run').parentElement.append(notice);
 notice.hidden=!diagnosticValidationAttempted||!result.global.length&&!issues.size;notice.textContent=result.global[0]||(issues.size?'Complete the highlighted inputs.':'');
 return result;
}
function validateDiagnosticInputs(defs=configuredDefinitions()){
 updateDiagnosticValidation(defs);diagnosticValidationAttempted=true;const result=updateDiagnosticValidation(defs);
 if(result.global.length||result.issues.size){focusMissingInput($('#diagnostics-view'));return false;}return true;
}
