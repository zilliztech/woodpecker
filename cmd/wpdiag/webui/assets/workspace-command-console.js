/* A browser frontend to catalog-validated wp subprocesses. Shared values are scoped by authority. */
'use strict';
let commandPreferences=null,commandDrafts={},commandValidationAttempted=false,commandValidationMissing=[];
const managedConnectionFlags=new Set(['trace-id','request-id','context','endpoint','node-admin-url','config','kubectl','etcd','meta-prefix','standalone','client','server','bindings-file','client-meta']);
const runtimeFlagNames=new Set(['output','timeout','concurrency','strict','no-color']);
function commandDefinitionByName(name){return state.commands.commands.find(c=>c.name===name);}
function metadataAuthority(){const m=sharedMetadata();return m.etcd&&m.prefix?m.etcd+'\0'+m.prefix:'unbound';}
function commandConnection(c){return c.name==='diagnose'&&preferences().diagnoseMode==='catalog'?'local':c.connection;}
function bindingDraft(kind){const p=preferences();p.bindings??={};p.bindings[kind]??={profile:'',overrides:{}};return p.bindings[kind];}
function bindingProfile(kind){return state.info.connection_profiles?.[bindingDraft(kind).profile]?.[kind]||{};}
function bindingValue(kind,key){const b=bindingDraft(kind);return b.overrides[b.profile]?.[key]??bindingProfile(kind)[key]??'';}
function bindingAuthority(kind){const b=bindingDraft(kind);return kind==='etcd'?b.profile+'|'+bindingValue(kind,'endpoints'):b.profile+'|'+bindingValue(kind,'kubeconfig')+'|'+bindingValue(kind,'kube_context');}
function commandContext(c){const connection=commandConnection(c);return connection==='kubernetes'?bindingDraft('k8s').profile:connection==='etcd'?sharedContext():['service','mixed','metadata'].includes(connection)||c.name==='ctx view'?sharedContext():'';}
function commandScope(c){const connection=commandConnection(c);return connection==='metadata'?metadataAuthority():connection==='etcd'?sharedContext()+'|'+sharedMetadata().etcd:connection==='kubernetes'?bindingAuthority('k8s'):connection==='telemetry'?JSON.stringify({bindings:diagnosticRequestBindings(commandCheckDefinitions(c)),trace:diagnosticTraceParameters(commandCheckDefinitions(c))}):['service','mixed'].includes(connection)?commandContext(c)+(connection==='mixed'?'|'+metadataAuthority():''):state.info.config_path;}
function commandDraftKey(c){return c.name;}
function commandRunKey(c,operation=state.tab==='runbooks'?{book:state.runbook,index:operationSteps[state.runbook]||0}:null){return c.name+'|'+commandScope(c)+(operation?'|operation:'+operation.book+':'+operation.index:'');}
function preferences(){
 if(commandPreferences)return commandPreferences;
 let saved={};try{saved=JSON.parse(sessionStorage.getItem('wp-command-ui:'+state.info.config_path)||'{}');}catch{}
 commandPreferences={values:{},runtime:saved.runtime||{output:'json',timeout:'30s',concurrency:'8',strict:false,budget:'120',window:'15m'},panels:saved.panels||{}};
 return commandPreferences;
}
function savePreferences(){try{const p=preferences();sessionStorage.setItem('wp-command-ui:'+state.info.config_path,JSON.stringify({runtime:p.runtime,panels:p.panels}));}catch{}}
function rememberCommandPanels(){
 const p=preferences();p.panels??={};
 for(const panel of document.querySelectorAll('#command-config details[data-command-panel]'))p.panels[panel.dataset.commandPanel]=panel.open;
 savePreferences();
}
function commandPanelOpen(key,fallback=false){const panels=preferences().panels||{};return (Object.hasOwn(panels,key)?panels[key]:fallback)?'open':'';}
function canonicalParameter(c,name,flag=false){
 if(!flag){if(name==='node')return 'node';if(name==='node-or-pod')return 'pod';if(name==='logName')return 'logName';if(name==='logId')return 'logID';if(name==='segmentId')return 'segmentID';return '';}
 if(['log','log-id'].includes(name))return 'logID';if(['seg','segment-id'].includes(name))return 'segmentID';if(name==='log-name')return 'logName';
 if(['bucket','root-path','root'].includes(name)&&c.flags.some(f=>f.name==='bucket'))return name==='bucket'?'bucket':'rootPath';
 if(c.connection==='kubernetes'&&['namespace','wp-cluster'].includes(name))return name==='namespace'?'namespace':'wpCluster';
 return '';
}
function commonValue(c,key){const p=preferences();if(Object.hasOwn(p.values,key))return String(p.values[key]);if(key==='bucket')return sharedMetadata().bucket||'';if(key==='rootPath')return sharedMetadata().root_path||'';if(commandConnection(c)==='kubernetes'&&['namespace','wpCluster'].includes(key))return String(bindingProfile('k8s')[key==='namespace'?'namespace':'cluster']||'');return '';}
function setCommonValue(c,key,value){preferences().values[key]=value;}
function uiArguments(c){if(c.name==='diagnose'&&commandConnection(c)==='local')return [];return c.name==='log skip-range remove'&&preferences().removeMode==='id'?[{name:'segmentId',optional:false}]:c.arguments;}
function commandSharedFields(c){if(c.name==='diagnose'&&commandConnection(c)==='local')return [];const keys=new Set();for(const a of uiArguments(c)){const key=canonicalParameter(c,a.name);if(key)keys.add(key);}for(const f of c.flags){const key=canonicalParameter(c,f.name,true);if(key)keys.add(key);}if(c.name==='log skip-range remove'&&preferences().removeMode!=='id')keys.delete('logID');return ['namespace','wpCluster','pod','node','logName','logID','segmentID','bucket','rootPath'].filter(key=>keys.has(key));}
// The catalog supplies Cobra-required flags. These additional requirements are
// validated in the native RunE implementations, including preview/apply branches.
const commandRuntimeRequired={
 'ops show':['op-id'],'metrics top':['by'],'logging set-level':['level'],
 'logstore fence-quorum':['reason'],'log skip-range add':['from-entry','to-entry','reason'],
 'log skip-range remove':['from-entry','to-entry']
};
function commandFlagRequired(c,f,flags={}){
 if(f.required||(commandRuntimeRequired[c.name]||[]).includes(f.name))return true;
 if(c.name==='metrics report'&&f.name==='scenario')return !flags.list;
 if(flags.apply&&['log truncate','meta log patch','meta segment patch'].includes(c.name))return ['expected-revision','reason','backup',...(c.name.startsWith('meta ')?['offline']:[])].includes(f.name);
 return false;
}
function commandRequirement(kind,key,required){return `<span class="command-param-requirement" data-requirement-kind="${kind}" data-requirement-key="${esc(key)}">${required?'<em class="required-mark">Required</em>':'<span class="optional-mark">Optional</span>'}</span>`;}
function commandParameterExample(c,input){
 const key=input.dataset.sharedKey||input.dataset.bindingKey||input.dataset.commandFlag||input.dataset.sharedEvidence||'';
 const fields={
  node:['Node','Discovered node ID in the selected WP service. Use the node identity reported by node list.','woodpecker-0'],
  pod:['Pod / node ordinal','A pod name or node ordinal in the selected Kubernetes namespace.','woodpecker-0'],
  logName:['Log name','Logical log name in the selected client metadata namespace. This is not the numeric Log ID.','my-channel'],
  logID:['Log ID','Numeric identity of the log, from its metadata or loaded writer.','42'],
  segmentID:['Segment ID','Numeric segment identity within the selected log.','7'],
  bucket:['Bucket','Object-storage bucket containing this physical storage instance. Provide it together with the storage root path.','example-bucket'],
  rootPath:['Storage root path','Storage root inside the bucket, independent of the etcd metadata prefix.','tenant/wal'],
  'metadata.etcd':['Etcd endpoints','Client metadata etcd endpoint(s), separated by commas.','http://127.0.0.1:19379'],
  'metadata.prefix':['Full metadata prefix','Complete metadata namespace configured by this WP client, including its woodpecker prefix.','wp-prod/woodpecker'],
  'op-id':['Operation ID','Copy an actual operation ID returned by ops list for this node.','append-op-123'],
  'from-entry':['First entry','Inclusive start of the exact verified range. Use a nonnegative entry ID.','50'],
  'to-entry':['Last entry','Inclusive end of the range; it must be at least the first entry.','50'],
  reason:['Reason',c.name==='log skip-range add'?'Record why the loss of these verified unreadable entries is accepted.':'Record the reason for this specific intervention.','Reviewed recovery for log 42, segment 7'],
  'expected-last-entry-id':['Expected last entry ID','Use the exact LastEntryId from coordinator metadata. Use -1 only for an empty segment.','99'],
  'expected-revision':['Expected revision',c.name==='log truncate'?'Use the LOG before.revision returned by the latest preview, not the target segment revision.':'Use the TARGET RECORD before.revision returned by the latest preview.','174'],
  'patch-file':['Patch file','Local JSON file containing only the supported correction fields. Relative paths use the directory containing cli.yaml.','./segment-repair.json'],
  'output-file':['Snapshot file','New metadata snapshot file on the WebUI host. Existing files are not overwritten.','./metadata-snapshot.json'],
  backup:['Backup file','New pre-change backup file on the WebUI host, required when applying this operation.','./metadata-before-change.json'],
  offline:['Offline acknowledgement','Acknowledge that the relevant writers and auditors are stopped and will stay stopped during the metadata correction.','Enable only after stopping the relevant writers and auditors.'],
  replicas:['Replica count','Desired Kubernetes replica count.','3'],
  by:['Metric name','Exact Prometheus metric family exposed by the node, used to rank nodes.','go_goroutines'],
  level:['Log level','Runtime logging level to apply to the selected node(s).','debug'],
  scenario:['Metrics scenario','Native runtime metrics scenario; use metrics report --list to see supported names.','stuck-write'],
  metrics:['Prometheus address','Prometheus query API origin, reachable by the process running wp webui.','http://127.0.0.1:19099'],
  trace:['Trace address','Jaeger query API origin, reachable by the process running wp webui.','http://127.0.0.1:16686'],
  deployment:['Deployment name','Logical deployment identity for this evidence source.','wp-prod'],
  trace_service:['Trace service','Exact service name retained by the selected Jaeger source.','woodpecker-0'],
  node_id:['Node ID','Exact node identity label for the selected evidence source.','woodpecker-0'],
 };
 if(fields[key])return fields[key];
 if(input.hasAttribute('data-shared-context'))return ['Server context','Saved server context in cli.yaml. It selects the WP Admin entry point and node URL mappings.','wp-prod'];
 if(input.hasAttribute('data-command-argument')){
  const arg=uiArguments(c)[Number(input.dataset.commandArgument)],name=arg?.name||'Argument';
  if(c.name==='diag metrics check')return ['Metrics check','Canonical key from the metrics evidence list.','writer-latency'];
  if(c.name==='diag trace check')return ['Trace check','Canonical key from the trace evidence list.','server-append-duration'];
  if(c.name==='diag scenario')return ['Scenario','A diagnostic scenario key, or all to collect the supported checks.','slow-writes'];
  const examples={name:'wp-prod',ID:'12',key:'/example/key','metric-name':'go_goroutines'};
  return [name,'Required positional argument for wp '+c.name+'.',examples[name]||name];
 }
 return ['Parameter','Required input for wp '+c.name+'.',input.placeholder||'Use a value from the selected target.'];
}
function updateCommandRequirements(c,flags){
 for(const marker of document.querySelectorAll('#command-config [data-requirement-kind]')){
  const kind=marker.dataset.requirementKind,key=marker.dataset.requirementKey;
  const required=kind==='shared'?requiredParameter(c,key):kind==='flag'?commandFlagRequired(c,c.flags.find(f=>f.name===key)||{name:key},flags):!uiArguments(c)[Number(key)]?.optional;
  const html=required?'<em class="required-mark">Required</em>':'<span class="optional-mark">Optional</span>';
  if(marker.dataset.required!==String(required)){marker.innerHTML=html;marker.dataset.required=String(required);}
 }
 const options=$('#command-config .command-options');if(options){const count=options.querySelectorAll('[data-requirement-kind=flag] .required-mark').length,total=options.querySelectorAll('[data-command-flag]').length;options.querySelector('summary').innerHTML='Command options ('+total+')'+(count?' <span class="command-required-count">'+count+' required</span>':'');}
 decorateRequiredFields($('#command-config'),{attempted:commandValidationAttempted,example:input=>commandParameterExample(c,input)});
}
function requiredParameter(c,key){if(['bucket','rootPath'].includes(key)&&(['segment probe','segment inspect','log scan'].includes(c.name)||commonValue(c,key==='bucket'?'rootPath':'bucket')))return true;return uiArguments(c).some(a=>!a.optional&&canonicalParameter(c,a.name)===key)||c.flags.some(f=>f.required&&canonicalParameter(c,f.name,true)===key)||c.name==='log skip-range remove'&&preferences().removeMode==='id'&&key==='logID';}
function parameterParent(c,key){const keys=commandSharedFields(c);if(key==='segmentID')return keys.includes('logName')?'logName':keys.includes('logID')?'logID':'';if(key==='logID'&&commandConnection(c)==='service'&&keys.includes('node'))return 'node';if(key==='wpCluster'&&keys.includes('namespace'))return 'namespace';if(key==='pod'&&keys.includes('wpCluster'))return 'wpCluster';return '';}
function refreshDependentFields(c,changed){const dependent=new Set([changed]);for(const key of commandSharedFields(c)){const parent=parameterParent(c,key),input=$(`[data-shared-key="${key}"]`);if(!input)continue;if(parent&&dependent.has(parent)){input.value=commonValue(c,key);dependent.add(key);}const connection=commandConnection(c),unbound=['service','mixed'].includes(connection)&&!commandContext(c)||['metadata','mixed'].includes(connection)&&(!sharedMetadata().etcd||!sharedMetadata().prefix);input.disabled=unbound||!!parent&&parent!=='namespace'&&!commonValue(c,parent);input.title=unbound?'Select the required connection first':input.disabled?'Select '+parent+' first':'';}}
function dependencySummary(c){const label={local:'No remote connection',service:'WP Admin context',metadata:'Client etcd + full metadata prefix',etcd:'Etcd endpoints; no metadata prefix',mixed:'WP Admin context + client metadata namespace',kubernetes:'Kubernetes connection / kubectl defaults',telemetry:'Client / Server sources + scope + incident interval'};const connection=commandConnection(c);return label[connection]||connection;}
function reusableFlag(name){return !/(password|token|secret|credential|access-key|cert|key$)/.test(name)&&!['yes','apply','force','execute','offline','reason','expected-revision','expected-last-entry-id','backup','patch-file','output-file'].includes(name);}
function runtimeFlag(c,name){return c.flags.some(f=>f.name===name&&f.global);}
function selectControl(id,rows,current,empty){if(!rows.length)return `<span class="field-hint">${empty}</span>`;if(rows.length===1&&current===rows[0].id)return `<b class="console-connection-name">${esc(rows[0].label)}</b>`;return `<select id="${id}"><option value="">${empty}</option>${rows.map(x=>`<option value="${esc(x.id)}" ${x.id===current?'selected':''}>${esc(x.label)}</option>`).join('')}</select>`;}
// Select the actual CLI tokens, using the command catalog as the only hierarchy.
let commandChooserInitialized=false,commandChooserPath=[],commandChooserName='';
function ensureCommandPathPicker(){
 if(!$('#command-path-controls'))$('#command-controls>.workflow-step').innerHTML='<div class="panel-head"><div class="panel-title"><span class="workflow-number">1</span>Choose a command</div></div><div class="command-path-picker"><div class="command-path-row"><code class="command-path-prefix">wp</code><div id="command-path-controls"></div></div><div class="command-path-summary"><code id="selected-command-label"></code><span id="command-count"></span></div><div class="visual-shortcuts"><button data-tab="cluster">Cluster view ↗</button><button data-tab="metadata">Metadata ↗</button></div></div>';
 if(!$('#command-selection-prompt')){const panel=document.createElement('section');panel.id='command-selection-prompt';panel.className='surface output-surface command-selection-prompt';panel.hidden=true;$('#command-console').before(panel);}
}
function commandPathChildren(prefix){return [...new Set(state.commands.commands.map(c=>c.name.split(' ')).filter(parts=>parts.length>prefix.length&&prefix.every((token,i)=>parts[i]===token)).map(parts=>parts[prefix.length]))].sort();}
function chooseCommandToken(level,value){
 const prefix=commandChooserPath.slice(0,level),choices=commandPathChildren(prefix);if(value&&!choices.includes(value))return;
 const old=commandDefinitionByName(state.commandName);if(old&&$('#command-config')?.children.length){saveCommandFields(old);rememberCommandPanels();}
 commandChooserPath=value?[...prefix,value]:prefix;
 const name=commandChooserPath.join(' ');state.commandName=commandDefinitionByName(name)?name:'';commandChooserName=state.commandName;state.commandEditorKey='';render();const next=$('#command-path-level-'+(level+1))||$('#command-path-level-'+level);next?.focus({preventScroll:true});
}
function renderCommandDirectory(){
 if(!state.commands)return;ensureCommandPathPicker();
 if(!commandChooserInitialized){commandChooserInitialized=true;if(!state.commandName)state.commandName=state.info.available_contexts?.length?'cluster info':'version';}
 if(state.commandName&&state.commandName!==commandChooserName){commandChooserPath=state.commandName.split(' ');commandChooserName=state.commandName;}
 const fields=[];let prefix=[];
 for(let level=0;;level++){
  const choices=commandPathChildren(prefix);if(!choices.length)break;
  const value=commandChooserPath[level]||'',label=['Group','Command','Action'][level]||'Action '+level;
  fields.push(`<label class="command-path-field"><span>${label}</span><select id="command-path-level-${level}" data-command-level="${level}" aria-label="${label}"><option value="">Choose ${label.toLowerCase()}</option>${choices.map(token=>`<option value="${esc(token)}" ${value===token?'selected':''}>${esc(token)}</option>`).join('')}</select></label>`);
  if(!value)break;prefix.push(value);
 }
 const html=fields.join(''),controls=$('#command-path-controls');if(controls.dataset.content!==html){controls.innerHTML=html;controls.dataset.content=html;for(const select of controls.querySelectorAll('[data-command-level]'))select.onchange=()=>chooseCommandToken(Number(select.dataset.commandLevel),select.value);}
 controls.dataset.levels=String(fields.length);
 $('#selected-command-label').textContent='wp'+(commandChooserPath.length?' '+commandChooserPath.join(' '):'')+(state.commandName?'':' …');
 $('#command-count').textContent=state.commands.commands.length+' commands';
 const c=commandDefinitionByName(state.commandName),pending=$('#command-selection-prompt');pending.hidden=!!c||state.tab!=='commands'||!!state.tool;
 if(c)renderSelectedCommand();else{
  $('#command-config').hidden=true;$('#command-console').hidden=true;pending.innerHTML='<div class="panel-head"><div class="panel-title">Choose a command</div></div><p>Select the next command part on the left to configure its parameters.</p>';renderCommandHistory();
 }
}
// Operations and Command Explorer share one editor, validator and command runner.
function renderSelectedCommand(){
 const c=commandDefinitionByName(state.commandName);if(!c)return;
 $('#command-config').hidden=false;$('#command-console').hidden=false;if($('#command-selection-prompt'))$('#command-selection-prompt').hidden=true;
 const key=c.name+'|'+commandContext(c)+'|'+(commandConnection(c)==='telemetry'?requirementKey(connectionRequirements(c)):commandScope(c))+'|'+diagnosticBindingsRevision;
 if(state.commandEditorKey!==key){state.commandEditorKey=key;renderCommandEditor(c);}
 renderCommandOutput(c);
}
function showCommand(name){if(!commandDefinitionByName(name))return;const old=commandDefinitionByName(state.commandName);if(old&&$('#command-config')?.children.length){saveCommandFields(old);rememberCommandPanels();}state.commandName=name;state.commandEditorKey='';state.tool='';state.tab='commands';render();}
function renderCommandEditor(c){
 commandValidationAttempted=false;commandValidationMissing=[];
 rememberCommandPanels();
 const draft=commandDrafts[commandDraftKey(c)]||{arguments:{},flags:{}},p=preferences(),shared=commandSharedFields(c),args=uiArguments(c),connection=commandConnection(c);
 const labels={node:'Node',pod:'Pod / node ordinal',logName:'Log name',logID:'Log ID',segmentID:'Segment ID',bucket:'Bucket',rootPath:'Storage root path',namespace:'Kubernetes namespace',wpCluster:'WP cluster resource'};
 const hints={node:'Node in the selected WP service.',pod:'Pod name or node ordinal in this namespace / resource.',logName:'Name in the selected client metadata namespace.',logID:connection==='service'?'Numeric ID of a writer loaded on this node.':'Numeric ID in the selected metadata namespace.',segmentID:'Segment within the selected Log.',bucket:['segment probe','segment inspect','log scan'].includes(c.name)?'Required explicit storage instance for this physical check.':'Optional instance filter; provide together with root path.',rootPath:'Storage root paired with bucket. This is independent of the etcd metadata prefix.',namespace:'Empty uses kubectl’s configured namespace.',wpCluster:'Woodpecker resource name; empty uses the CLI default.'};
 const service=['service','mixed'].includes(connection),metadata=['metadata','mixed'].includes(connection),telemetry=connection==='telemetry',raw=connection==='etcd',kube=connection==='kubernetes';
 const meta=state.info.session.metadata,choices=state.info.metadata_authorities;
 const currentMeta=choices.find(x=>x.id===meta?.authority_id)||choices.find(x=>x.etcd===meta?.etcd&&x.prefix===meta?.prefix);
 const optionFlags=(c.name==='diagnose'&&connection==='local'?[]:c.flags).filter(f=>!managedConnectionFlags.has(f.name)&&!canonicalParameter(c,f.name,true)&&!(f.global&&runtimeFlagNames.has(f.name))&&!(['target','from','to','window'].includes(f.name)&&telemetry)&&!(kube&&['kube-context','kubeconfig'].includes(f.name))&&!(c.name==='diagnose'&&f.name==='list'));
 const profiles=Object.entries(state.info.connection_profiles||{}).filter(([,v])=>raw?v.etcd?.endpoints?.length:v.k8s&&Object.values(v.k8s).some(Boolean));
 const profileSelect=kind=>`<label>Saved ${raw?'etcd':'Kubernetes'} defaults (optional)<select id="console-profile"><option value="">${raw?'Enter endpoints directly':'Use kubectl defaults'}</option>${profiles.map(([id])=>`<option value="${esc(id)}" ${bindingDraft(kind).profile===id?'selected':''}>${esc(id)}</option>`).join('')}</select></label>`;
 const title=(n,text)=>`<h3 class="config-section-title"><span class="workflow-number">${n}</span>${text}</h3>`;
 $('#command-config').innerHTML=`<div class="console-heading"><div><h2>wp ${esc(c.name)}</h2><span class="field-hint">${esc(c.description.split('\n')[0])}</span></div><span class="console-effect">${esc(c.effect)}</span></div><div class="console-form">
 ${c.name==='diagnose'?`<label class="command-mode">Operation mode<select id="console-operation-mode"><option value="run">Run live runtime diagnosis</option><option value="catalog" ${p.diagnoseMode==='catalog'?'selected':''}>List available checks (local)</option></select></label>`:''}
 ${c.name==='log skip-range remove'?`<label class="command-mode">Identify the Log by<select id="console-remove-mode"><option value="name">Log name</option><option value="id" ${p.removeMode==='id'?'selected':''}>Numeric Log ID</option></select></label>`:''}
 <section class="command-config-section">${title(2,'Connections')}<div class="console-bindings">
 ${(service||metadata||telemetry||raw)?'<div id="command-connections" class="shared-connections"></div>':''}
 ${kube?`${profileSelect('k8s')}<label>Kubeconfig file (optional)<input id="console-kubeconfig" value="${esc(bindingValue('k8s','kubeconfig'))}" placeholder="kubectl environment / default file"></label><label>Kubernetes context (optional)<input id="console-kube-context" value="${esc(bindingValue('k8s','kube_context'))}" placeholder="kubectl current-context"></label><p class="field-hint">Selects Kubernetes access only. WP Admin context is independent.</p>`:''}
 ${!service&&!metadata&&!telemetry&&!raw&&!kube?'<p class="connection-free">No remote connection required.</p>':'<p class="connection-note">Selected configuration · reachability is checked when the command runs.</p>'}
 </div></section>
 <section class="command-config-section">${title(3,'Object scope')}
 ${shared.length?`<div class="console-shared"><div class="console-fields">${shared.map(key=>`<label><span class="scope-field-label">${labels[key]} ${commandRequirement('shared',key,requiredParameter(c,key))}</span><input data-shared-key="${key}" data-recent-field="${({logName:'log_name',logID:'log_id',segmentID:'segment_id',node:'node_id',bucket:'metadata.bucket',rootPath:'metadata.root_path'})[key]||key}" autocomplete="off" value="${esc(commonValue(c,key))}" placeholder="${labels[key]}">${hints[key]?`<small class="field-hint">${esc(hints[key])}</small>`:''}</label>`).join('')}</div><p class="field-hint">Current inputs stay in the form when switching compatible commands. Suggestions never fill another field.</p></div>`:''}
 ${args.some(a=>!canonicalParameter(c,a.name))?`<div class="console-fields command-specific">${args.map((a,i)=>canonicalParameter(c,a.name)?'':`<label><span class="scope-field-label">${esc(a.name)} ${commandRequirement('argument',String(i),!a.optional)}</span><input data-command-argument="${i}" value="${esc(draft.arguments[a.name]||'')}" placeholder="${esc(a.name)}"></label>`).join('')}</div>`:''}${!shared.length&&!args.length?'<p class="field-hint">No object selection required for this command.</p>':''}</section>
 <section class="command-config-section">${title(4,'Run parameters')}
 ${telemetry?'<div id="command-incident-window" class="shared-time-fields"></div>':''}
 <details class="command-options" data-command-panel="options" ${commandPanelOpen('options',optionFlags.some(f=>commandFlagRequired(c,f,draft.flags)))}><summary>Command options (${optionFlags.length})${optionFlags.some(f=>commandFlagRequired(c,f,draft.flags))?' · required inputs':''}</summary><div>${optionFlags.map(f=>{const value=draft.flags[f.name],bool=f.type==='bool';return `<label><span><span class="scope-field-label">--${esc(f.name)} ${commandRequirement('flag',f.name,commandFlagRequired(c,f,draft.flags))}</span><small>${esc(f.description)}</small></span><input data-command-flag="${esc(f.name)}" ${bool?`type="checkbox" ${value===true||value===undefined&&f.default==='true'?'checked':''}`:`type="${/password|token|secret|access-key/.test(f.name)?'password':'text'}" value="${esc(value??'')}" placeholder="${esc(f.default||f.type)}"`}></label>`;}).join('')||'<p>No additional options.</p>'}</div></details>
 <details class="console-runtime" data-command-panel="runtime" ${commandPanelOpen('runtime')}><summary>Execution settings</summary><div class="console-fields">${runtimeFlag(c,'timeout')?`<label>Request timeout<input id="console-timeout" value="${esc(p.runtime.timeout||'30s')}"></label>`:''}${runtimeFlag(c,'concurrency')?`<label>Concurrency<input id="console-concurrency" type="number" min="1" value="${esc(p.runtime.concurrency||'8')}"></label>`:''}<label>Execution budget (seconds)<input id="console-budget" type="number" min="1" max="1800" value="${esc(p.runtime.budget||'120')}"></label>${runtimeFlag(c,'output')?`<label>Output<select id="console-output">${['json','table','wide','yaml'].map(x=>`<option ${p.runtime.output===x?'selected':''}>${x}</option>`).join('')}</select></label>`:''}${runtimeFlag(c,'strict')?`<label class="console-checkbox"><input id="console-strict" type="checkbox" ${p.runtime.strict?'checked':''}> Strict fan-out</label>`:''}</div></details></section>
 <div id="console-scope-review" class="scope-review"></div><div class="console-actions"><button id="execute-command" class="primary">▶ Run</button><button id="stop-command" hidden>Stop</button><button id="copy-command">Copy CLI</button>${telemetry?'<button id="download-command-bindings" class="text-link">Download bindings JSON</button>':''}${c.visual?'<button id="open-command-view" class="text-link">Open related view ↗</button>':''}<a href="${esc(c.reference)}" target="_blank" rel="noopener noreferrer">Help ↗</a><span id="console-command-state"></span></div>
 <div id="console-form-error" class="form-error" role="alert" hidden></div><details class="console-preview" data-command-panel="preview" ${commandPanelOpen('preview')}><summary>CLI / execution arguments</summary><pre class="report" id="command-preview"></pre><span class="field-hint">Relative file paths use the directory containing cli.yaml. Action flags are not carried between commands.</span></details></div>`;
 $('#command-console').innerHTML=`<div class="panel-head"><div class="panel-title">Command execution</div><code class="execution-command">wp ${esc(c.name)}</code></div><div id="command-execution"></div><div class="console-output-head"><b>Result</b><div><button data-command-output="formatted" class="selected">Formatted</button><button data-command-output="stdout">stdout</button><button data-command-output="stderr">stderr</button></div><span id="command-exit"></span></div><div id="command-result"></div><div class="console-input" id="command-input-row" hidden><label for="command-stdin">CLI input</label><input id="command-stdin" placeholder="Reply to a CLI prompt"><button id="command-send-input">Send</button></div>`;
 const save=event=>{const input=event?.target;if(input?.dataset.sharedKey){setCommonValue(c,input.dataset.sharedKey,input.value.trim());refreshDependentFields(c,input.dataset.sharedKey);}saveCommandFields(c);updateCommandPreview(c);};
 for(const panel of document.querySelectorAll('#command-config details[data-command-panel]'))panel.ontoggle=()=>{if(panel.isConnected)rememberCommandPanels();};
 for(const input of document.querySelectorAll('#command-config input,#command-config select')){if(input.id==='command-stdin'||['console-context','console-meta','console-target','console-profile','console-operation-mode','console-remove-mode','console-kubeconfig','console-kube-context','console-etcd-endpoints'].includes(input.id))continue;input.oninput=save;input.onchange=event=>{save(event);if(telemetry&&input.hasAttribute('data-command-argument'))renderCommandConnections(c);};}
 async function changeConnection(body){state.commandSwitching=true;updateCommandPreview(c);try{await select(body);}catch(e){error(e);}finally{state.commandSwitching=false;state.commandEditorKey='';render();}}
 const rebuild=()=>{state.commandEditorKey='';render();};
 if($('#console-operation-mode'))$('#console-operation-mode').onchange=()=>{p.diagnoseMode=$('#console-operation-mode').value;savePreferences();rebuild();};
 if($('#console-remove-mode'))$('#console-remove-mode').onchange=()=>{p.removeMode=$('#console-remove-mode').value;savePreferences();rebuild();};

 if($('#console-meta'))$('#console-meta').onchange=()=>changeConnection($('#console-meta').value?{action:'connect-meta',authority_id:$('#console-meta').value,log_name:''}:{action:'disconnect-meta'});
 if($('#console-connect-meta'))$('#console-connect-meta').onclick=()=>changeConnection({action:'connect-meta',authority_id:choices[0].id,log_name:''});
 if($('#console-target'))$('#console-target').onchange=()=>changeConnection({action:'use-target',target:$('#console-target').value});
 if($('#console-profile'))$('#console-profile').onchange=()=>{bindingDraft(raw?'etcd':'k8s').profile=$('#console-profile').value;savePreferences();rebuild();};
 for(const [id,kind,key]of [['console-etcd-endpoints','etcd','endpoints'],['console-kubeconfig','k8s','kubeconfig'],['console-kube-context','k8s','kube_context']])if($('#'+id))$('#'+id).onchange=()=>{const b=bindingDraft(kind);b.overrides[b.profile]??={};b.overrides[b.profile][key]=$('#'+id).value.trim();savePreferences();rebuild();};
 if($('#download-command-bindings'))$('#download-command-bindings').onclick=()=>download(JSON.stringify(diagnosticRequestBindings(commandCheckDefinitions(c)),null,2)+'\n','wp-diagnostic-bindings.json');
 $('#execute-command').onclick=()=>{const key=commandRunKey(c);executeCommand(c).catch(e=>{if(state.commandName===c.name&&commandRunKey(c)===key)consoleError(e);});};
 $('#stop-command').onclick=()=>{const run=state.commandRuns[commandRunKey(c)];if(run)api('/api/runs/'+run.id+'/cancel',{}).catch(consoleError);};
 $('#copy-command').onclick=async()=>{try{await navigator.clipboard.writeText($('#command-preview').textContent);$('#copy-command').textContent='Copied';}catch{consoleError(Error('Select and copy the CLI preview.'));}};
 if($('#open-command-view'))$('#open-command-view').onclick=()=>openCommandView(c);
 $('#command-send-input').onclick=()=>sendCommandInput(c).catch(consoleError);
 $('#command-stdin').onkeydown=e=>{if(e.key==='Enter'){e.preventDefault();sendCommandInput(c).catch(consoleError);}};
 if(service||metadata||telemetry||raw)renderCommandConnections(c);
 if(telemetry)renderCommandTime(c);
 if(telemetry){const input=$('[data-command-argument="0"]');if(input){const list=document.createElement('datalist');list.id='command-check-names';const values=c.name==='diag scenario'?state.catalog.scenarios.map(v=>v.id):state.catalog.checks.filter(v=>v.source===(c.name==='diag metrics check'?'metrics':'trace')).map(v=>v.name);list.innerHTML=values.map(v=>`<option value="${esc(v)}"></option>`).join('');input.setAttribute('list',list.id);input.after(list);}}
 for(const key of shared)refreshDependentFields(c,key);
 updateCommandPreview(c);
}
function consoleError(e){$('#console-form-error').classList.remove('form-validation-error');$('#console-form-error').hidden=false;$('#console-form-error').textContent=e.message||String(e);}
function saveCommandFields(c){
 const draft={arguments:{},flags:{}},p=preferences();
 for(const input of document.querySelectorAll('[data-shared-key]'))setCommonValue(c,input.dataset.sharedKey,input.value.trim());
 for(const [i,a]of uiArguments(c).entries()){const input=$(`[data-command-argument="${i}"]`);if(input)draft.arguments[a.name]=input.value;}
 for(const input of document.querySelectorAll('[data-command-flag]'))if(reusableFlag(input.dataset.commandFlag))draft.flags[input.dataset.commandFlag]=input.type==='checkbox'?input.checked:input.value;
 commandDrafts[commandDraftKey(c)]=draft;
 for(const key of ['timeout','concurrency','budget','output','window'])if($('#console-'+key))p.runtime[key]=$('#console-'+key).value;
 if($('#console-strict'))p.runtime.strict=$('#console-strict').checked;savePreferences();
}
function commandPayload(c){
 const q={name:c.name,arguments:[],flags:{},context:commandContext(c),use_metadata:false,budget_ms:Number(preferences().runtime.budget||120)*1000};
 const runtime=preferences().runtime;
 for(const key of ['timeout','concurrency','output'])if(runtimeFlag(c,key)&&runtime[key]!==undefined&&runtime[key]!=='')q.flags[key]=String(runtime[key]);
 if(runtimeFlag(c,'strict'))q.flags.strict=!!runtime.strict;
 if(runtimeFlag(c,'no-color'))q.flags['no-color']=true;
 for(const [i,a]of uiArguments(c).entries()){
  const key=canonicalParameter(c,a.name),value=(key?$(`[data-shared-key="${key}"]`)?.value:$(`[data-command-argument="${i}"]`)?.value)||'';
  if(value.trim())q.arguments.push(...(a.name.includes('...')?value.trim().split(/\s+/):[value.trim()]));else if(!a.optional)q.arguments.push('');
 }
 for(const f of c.flags){const key=canonicalParameter(c,f.name,true);if(key){const value=$(`[data-shared-key="${key}"]`)?.value;if(value!==undefined&&value!==''&&commandSharedFields(c).includes(key))q.flags[f.name]=value;}}
 for(const input of document.querySelectorAll('[data-command-flag]')){
  const f=c.flags.find(x=>x.name===input.dataset.commandFlag);if(!f)continue;
  if(input.type==='checkbox'){if(input.checked||f.default==='true')q.flags[f.name]=input.checked;}
  else if(input.value!=='')q.flags[f.name]=f.type==='stringArray'||f.type==='stringSlice'?input.value.split('\n').filter(Boolean):input.value;
 }
 if(commandConnection(c)==='telemetry'){q.bindings=diagnosticRequestBindings(commandCheckDefinitions(c));for(const [key,value]of Object.entries(diagnosticTraceParameters(commandCheckDefinitions(c))))q.flags[key.replaceAll('_','-')]=value;try{const window=timeSelection();q.flags.from=window.from;q.flags.to=window.to;}catch{}}
 if(['metadata','mixed','etcd'].includes(commandConnection(c))&&c.flags.some(f=>f.name==='etcd')){q.metadata={...sharedMetadata(),label:'Explicit form parameters',log_name:''};delete q.metadata.bucket;delete q.metadata.root_path;}
 if(c.name==='diagnose'&&preferences().diagnoseMode==='catalog')q.flags.list=true;
 if(commandConnection(c)==='etcd'){const endpoints=$('#console-etcd-endpoints')?.value.trim();if(endpoints)q.flags.etcd=endpoints;if(!q.context)q.flags.standalone=true;}
 if(commandConnection(c)==='kubernetes'){for(const key of ['kubeconfig','kube-context']){const value=$('#console-'+key)?.value.trim();if(value)q.flags[key]=value;}}
 return q;
}
function updateCommandPreview(c){
 const q=commandPayload(c),argv=['wp'];updateCommandRequirements(c,q.flags);if(q.context)argv.push('--context',q.context);argv.push(...c.name.split(' '));
 const flags={...q.flags};if(q.metadata){flags.etcd=q.metadata.etcd;flags['meta-prefix']=q.metadata.prefix;if(c.flags.some(f=>f.name==='standalone'))flags.standalone=!q.metadata.client_meta;if(q.metadata.client_meta)flags['client-meta']=q.metadata.client_meta;}if(q.bindings)flags['bindings-file']='./wp-diagnostic-bindings.json';if(q.use_metadata){flags.etcd=state.info.session.metadata.etcd;flags['meta-prefix']=state.info.session.metadata.prefix;if(c.flags.some(f=>f.name==='standalone'))flags.standalone=true;}
 for(const [name,value]of Object.entries(flags)){for(const v of Array.isArray(value)?value:[value])argv.push('--'+name+'='+String(v));}
 if(q.arguments.length){argv.push('--');for(const [i,a]of q.arguments.entries())argv.push(a||'<'+c.arguments[i].name+'>');}
 $('#command-preview').textContent='WOODPECKER_CLI_CONFIG='+shellArgument(state.info.config_path)+' '+argv.map(shellArgument).join(' ');
 const run=state.commandRuns[commandRunKey(c)],running=run&&!run.done;
 const connection=commandConnection(c),missing=[];
 if(['service','mixed'].includes(connection)&&!(state.info.available_contexts||[]).includes(q.context))missing.push('WP Admin context');
 if(['metadata','mixed'].includes(connection)&&(!q.metadata?.etcd||!q.metadata?.prefix))missing.push('client metadata endpoints / prefix');
 if(connection==='etcd'&&!q.metadata?.etcd)missing.push('etcd endpoints');
 if(connection==='telemetry'){if(!commandCheckDefinitions(c).length)missing.push('Choose a known check or scenario');try{timeSelection();}catch(e){missing.push(e.message);}const message=bindingsError(connectionRequirements(c));if(message)missing.push(message);}
 for(const key of commandSharedFields(c))if(requiredParameter(c,key)&&!$(`[data-shared-key="${key}"]`)?.value.trim())missing.push(key);
 if(commandSharedFields(c).includes('bucket')){const bucket=$('[data-shared-key=bucket]')?.value.trim(),root=$('[data-shared-key=rootPath]')?.value.trim();if(!!bucket!==!!root)missing.push(bucket?'rootPath':'bucket');}
 for(const [i,a]of uiArguments(c).entries())if(!a.optional&&!q.arguments[i])missing.push(a.name);
 for(const f of c.flags)if(commandFlagRequired(c,f,q.flags)&&(q.flags[f.name]===undefined||q.flags[f.name]===''||f.type==='bool'&&!q.flags[f.name]))missing.push('--'+f.name);
 commandValidationMissing=[...new Set(missing)];
 $('#execute-command').disabled=c.execution!=='process'||!!running||state.commandSwitching;
 if(commandValidationAttempted&&$('#console-form-error').classList.contains('form-validation-error')){const panel=$('#console-form-error');panel.hidden=!missing.length;panel.textContent=missing.length?'Complete the required inputs: '+commandValidationMissing.join(', '):'';}
 $('#execute-command').textContent=running?'Running…':'▶ Run';$('#stop-command').hidden=!running;$('#stop-command').title=c.effect.startsWith('Changes')?'Stop waiting; already applied changes are not rolled back.':'Stop the running command.';
 $('#console-command-state').textContent=c.execution==='terminal'?'Use this interactive command in a terminal.':c.execution!=='process'?'Not implemented.':missing.length?(commandValidationAttempted?'Missing required inputs':'Complete the required inputs'):running?'Execution in progress':'Ready to run';
 const scope=commandSharedFields(c).map(key=>{const value=$(`[data-shared-key="${key}"]`)?.value;return value?key+'='+value:'';}).filter(Boolean);
 const binding=connection==='mixed'?[q.context,sharedMetadata().prefix].filter(Boolean).join(' + '):connection==='metadata'?sharedMetadata().prefix:connection==='etcd'?q.metadata?.etcd:connection==='kubernetes'?($('#console-kube-context')?.value||'kubectl current-context'):connection==='telemetry'?[...new Set(commandCheckDefinitions(c).map(d=>componentName(checkComponent(d))+' '+d.source+': '+(initialBindings()[checkComponent(d)][d.source]||'not configured')))].join('; '):q.context;
 $('#console-scope-review').innerHTML='<b>Run scope</b><span>'+esc([binding,...scope].filter(Boolean).join(' → ')||'Local command')+'</span>';

}
async function executeCommand(c){
 if($('#execute-command').disabled)return;saveCommandFields(c);commandValidationAttempted=true;updateCommandPreview(c);
 if(commandValidationMissing.length){
  const panel=$('#console-form-error');panel.classList.add('form-validation-error');panel.hidden=false;panel.textContent='Complete the required inputs: '+commandValidationMissing.join(', ');
  focusMissingInput($('#command-config'));
  return;
 }
 $('#console-form-error').classList.remove('form-validation-error');$('#console-form-error').hidden=true;
 const key=commandRunKey(c),q=commandPayload(c);
 const execution={operation:state.tab==='runbooks'?{book:state.runbook,index:operationSteps[state.runbook]||0}:null};
 state.commandRuns[key]={...execution,id:'',done:false,result:{status:'starting',name:c.name,stdout:'',stderr:''}};updateCommandPreview(c);renderCommandOutput(c);
 try{
  const job=await api('/api/ops/commands',q);state.commandRuns[key]={...execution,id:job.id,done:false,result:job.command};
  for(;;){const current=await api('/api/runs/'+job.id);state.commandRuns[key]={...execution,id:job.id,done:current.report_ready,result:current.command};if(state.commandName===c.name&&commandRunKey(c)===key){renderCommandOutput(c);updateCommandPreview(c);}if(current.report_ready){if(commandConnection(c)==='telemetry'){try{rememberEvidenceConnections(JSON.parse(current.command.stdout));}catch{}}rememberCommandResult(c,state.commandRuns[key]);if(state.tab==='runbooks')renderOperationExecution();break;}await new Promise(r=>setTimeout(r,350));}
 }catch(e){state.commandRuns[key].done=true;state.commandRuns[key].result.status='failed';state.commandRuns[key].result.stderr=e.message||String(e);if(state.commandName===c.name&&commandRunKey(c)===key){renderCommandOutput(c);updateCommandPreview(c);consoleError(e);}if(state.tab==='runbooks')renderOperationExecution();throw e;}
}
function compactJSON(value){const text=typeof value==='string'?value:JSON.stringify(value);return text&&text.length>220?text.slice(0,220)+'…':String(text??'');}
function formattedCommandResult(stdout){
 let value;try{value=JSON.parse(stdout);}catch{return `<pre class="command-terminal">${esc(stdout||'No stdout.')}</pre>`;}
 const rows=Array.isArray(value)?value:null;
 if(rows&&rows.length&&rows.every(x=>x&&typeof x==='object'&&!Array.isArray(x))){const keys=[...new Set(rows.flatMap(x=>Object.keys(x)))].slice(0,10);return `<div class="console-table-wrap"><table><thead><tr>${keys.map(k=>`<th>${esc(k)}</th>`).join('')}</tr></thead><tbody>${rows.slice(0,100).map(row=>`<tr>${keys.map(k=>`<td title="${esc(compactJSON(row[k]))}">${esc(compactJSON(row[k]))}</td>`).join('')}</tr>`).join('')}</tbody></table></div><p class="field-hint">${rows.length} rows · showing up to 100 rows and 10 columns. stdout retains the original output.</p>`;}
 if(value&&typeof value==='object'&&!Array.isArray(value))return `<div class="console-table-wrap"><table><tbody>${Object.entries(value).slice(0,40).map(([key,v])=>`<tr><th>${esc(key)}</th><td>${esc(compactJSON(v))}</td></tr>`).join('')}</tbody></table></div><p class="field-hint">Top-level fields. Open stdout for the complete JSON.</p>`;
 return `<pre class="command-terminal">${esc(JSON.stringify(value,null,2))}</pre>`;
}
let commandOutputMode='formatted';
function renderCommandOutput(c){
 if(!$('#command-result'))return;
 const run=state.commandRuns[commandRunKey(c)],r=run?.result;
 renderNativeExecution(run);renderCommandHistory();
 $('#command-exit').textContent=r?(r.status+(r.exit_code!==undefined?' · exit '+r.exit_code:'')):'Not run';
 $('#command-exit').className=r&&r.exit_code!==undefined&&r.exit_code!==0?'command-failed':'';
 $('#command-input-row').hidden=!r?.input_open||!!run.done;
 $('#command-result').innerHTML=r?(commandOutputMode==='stderr'?`<pre class="command-terminal stderr">${esc(r.stderr||'No stderr.')}</pre>`:commandOutputMode==='stdout'?`<pre class="command-terminal">${esc(r.stdout||'No stdout yet.')}</pre>`:formattedCommandResult(r.stdout)):'<div class="console-no-result">Run the command to inspect its exit status, stdout and stderr.</div>';
 if(r?.stderr&&commandOutputMode==='formatted')$('#command-result').innerHTML+=`<details class="console-stderr" ${!run.done||r.exit_code!==0?'open':''}><summary>stderr</summary><pre class="command-terminal stderr">${esc(r.stderr)}</pre></details>`;
 if(r?.output_limited)$('#command-result').innerHTML+='<p class="error-box">Output limit reached; the command was stopped.</p>';
 if(r?.finished_at)$('#command-result').innerHTML+=`<div class="console-run-info">${esc(r.context||'No WP service context')} · ${esc(stamp(r.started_at))} → ${esc(stamp(r.finished_at))}<br>Working directory: ${esc(r.working_directory)}</div>`;
 for(const button of document.querySelectorAll('[data-command-output]'))button.classList.toggle('selected',button.dataset.commandOutput===commandOutputMode);
 if(state.tab==='runbooks')renderOperationExecution();
}
async function sendCommandInput(c){const run=state.commandRuns[commandRunKey(c)];if(!run?.id||run.done)return;await api('/api/ops/commands/'+run.id+'/input',{text:$('#command-stdin').value});$('#command-stdin').value='';}
document.addEventListener('click',e=>{const tab=e.target.closest('[data-command-output]');if(tab){commandOutputMode=tab.dataset.commandOutput;renderCommandOutput(commandDefinitionByName(state.commandName));}});
(async()=>{renderOverview();try{await refresh();populateAtomic();const now=new Date(Date.now()-1000);$('#from').value=inputTime(new Date(+now-15*60000));$('#to').value=inputTime(now);render();}catch(e){error(e);}})();
