/* Guided operational scenarios. Reuses native command execution; no diagnostic rules. */
'use strict';
// Scenario content follows the public troubleshooting guide. It does not infer causes.
const operationsDocs='https://zilliztech.github.io/woodpecker/docs.html#operations-scenarios-';
const runbooks=[
 {id:'write-stall',title:'Investigate stalled writes',tag:'WRITE',summary:'Find where append progress stops.',doc:'write',scenario:'stuck-writes',steps:[
  {title:'Nodes & reachability',phase:'Investigate',question:'Are appends being submitted, and can the replicas be reached?',commands:['node list','node healthz'],look:'Record application append attempts, errors and deadlines. Check the relevant nodes, not only the Admin seed.',outcomes:[['No append attempts','Check the workload first; idle input does not establish a write stall.'],['Replica unreachable','Check node mappings, port forwards and availability; then assess the remaining quorum.']],verify:'Application append attempts and the affected nodes are identified.'},
  {title:'In-flight requests',phase:'Investigate',question:'Which append or flush request persists across samples?',commands:['ops list','ops show','ops stats'],look:'Filter by the actual numeric Log ID and Segment ID. Compare the same operation ID and age on every quorum member; use --longer-than 30000 for a 30-second lead.',outcomes:[['Same operation persists','Open its details to locate the stage; check registry eviction counts too.'],['No matching operation','It may have finished, been evicted or never reached this node. Check peers and client submission.']],verify:'A repeated operation is identified, or the gap before the server is recorded.'},
  {title:'Writer buffers & flush queues',phase:'Investigate',question:'Is the loaded writer buffering work or waiting for flush?',commands:['logstore segments','logstore segment-show','logstore buffer','logstore flush-queue'],look:'Observe the affected writer while appends are outstanding. Re-sample buffer bytes and queue depth; these are occupancies, not latency measurements.',outcomes:[['Queue grows; flush remains pending','Correlate operation details with storage I/O and backend errors.'],['Writer absent or queue empty','Confirm the current owner and segment. Loaded writers are not a list of all stored data.']],verify:'The affected writer and repeated buffer / queue samples are captured.'},
  {title:'Quorum-confirmed tail (LAC)',phase:'Confirm',question:'Does the quorum-confirmed tail advance under write load?',commands:['logstore lac','meta segment show'],look:'Run LAC twice for the same log and segment. Compare per-replica responses with the confirmed metadata boundary and observation times.',outcomes:[['LAC advances','The segment is confirming writes; investigate the client deadline or another stage.'],['LAC stays fixed with pending appends','Correlate with the repeated operation and queue evidence before choosing recovery.']],verify:'Two comparable LAC samples and the outstanding application writes are recorded.'},
  {title:'Flush / fence writer',phase:'Act',question:'Which intervention is justified by the collected evidence?',commands:['logstore force-flush','logstore fence-quorum'],risk:'These commands can change writer state. Force-flush requests sync on a loaded writer. Quorum fencing interrupts writes and relies on the application reopening or rolling the writer.',look:'Resolve confirmed network or storage failures first. Choose only the recovery that matches the observed blocker; this step is optional.',outcomes:[['Flush path needs intervention','Review the exact node, Log ID and Segment ID before requesting a flush.'],['Old writer must be fenced','Preview selected quorum members and impact. Supply a reason; commit only after reviewing the CLI acceptance.']],verify:'The intended intervention and its outcome are recorded; RPC success alone is not recovery.'},
  {title:'Recheck append progress',phase:'Verify',question:'Do application appends succeed and durable progress resume?',commands:['ops list','logstore lac','log scan'],look:'Check fresh application append results, repeat LAC and inspect outstanding operations. If the writer rolled, use the actual new segment.',outcomes:[['Appends succeed; LAC advances','Confirm old operations and queues clear under workload.'],['CRC / read errors appear','Switch to Corruption & skip-range recovery to inspect actual data.']],verify:'New application appends succeed, the durable tail advances and the original blocker clears.'}
 ]},
 {id:'read-stall',title:'Investigate stalled reads',tag:'READ',summary:'Compare the reader, confirmed tail and actual reads.',doc:'read',scenario:'stuck-reads',steps:[
  {title:'Reader positions',phase:'Investigate',question:'Is the same reader advancing, caught up or no longer reporting?',commands:['log readers'],look:'Record the Reader identity and position twice, allowing for its report interval (the guide uses 35 seconds). Keep the application read attempt or checkpoint alongside both samples.',outcomes:[['Position moves','The reader reports progress; compare it with the expected application checkpoint.'],['Position stays fixed','Compare with the confirmed tail next. An unchanged position alone is not a stall.']],verify:'The same Reader identity, both positions and their observation times are recorded.'},
  {title:'Metadata & confirmed tail',phase:'Confirm',question:'Is there confirmed unread data ahead of the reader?',commands:['meta log show','meta segment list','meta check','logstore lac','log scan'],look:'Compare the reader position with segment state, confirmed entry boundaries and LAC. Use scan for physical coverage; metadata checks alone do not read payloads.',outcomes:[['Reader is at the confirmed tail','It may be caught up. Confirm whether new writes are expected.'],['Confirmed backlog exists','Probe the exact next entry; distinguish a read failure from an unreachable replica.']],verify:'The next required entry and its expected boundary are identified.'},
  {title:'Read the blocked entry',phase:'Confirm',question:'Can any usable replica serve the next required entry?',commands:['segment probe'],look:'Set --from-entry to the suspected position and an appropriate --max-entries. Inspect every replica result; a bounded probe covers only its requested range.',outcomes:[['One replica serves the entry','Preserve that copy and inspect client failover or the unhealthy replicas.'],['Timeout / 404 / ambiguous location','Restore access or explicit bucket / root path; unreadability has not been established.'],['All usable alternatives fail','Inspect exact damage and the recoverable tail before considering any skip.']],verify:'Per-replica read outcomes are captured for the exact required position.'},
  {title:'Inspect blocks & requests',phase:'Investigate',question:'Is this a damaged record or a long-running read request?',commands:['segment inspect','ops list','ops show'],look:'Inspect can find records after a damaged block. In ops list, filter the real log / segment and logstore.get_batch_entries; inspect returned operation IDs.',outcomes:[['Checksum / record failure','Record exact bad entries and the healthy tail, then open the corruption workflow.'],['Long-running read, no proven damage','Inspect the request stage and client / server transport before any data-loss action.']],verify:'Physical damage, access failure and an outstanding request are distinguished.'},
  {title:'Recheck reader progress',phase:'Verify',question:'Does the original reader pass the blocked position?',commands:['log readers','segment probe'],look:'After the targeted recovery, re-sample the same reader and confirm application-level read success. A CLI probe does not advance the application reader.',outcomes:[['Reader and checkpoint advance','Confirm progress through the expected healthy tail.'],['Probe succeeds; reader remains blocked','Inspect client retry, reporting and application processing.']],verify:'The original Reader and application checkpoint advance through the required range.'}
 ]},
 {id:'segment-review',title:'Corruption & skip-range recovery',tag:'DATA',summary:'Confirm damage before accepting an exact unreadable range.',doc:'corruption',steps:[
  {title:'Export & inspect metadata',phase:'Investigate',question:'Can we reproduce the incident without losing the original evidence?',commands:['meta export','meta log show','meta segment show','meta check'],look:'Record the checkpoint, log / segment identity and version. Export metadata to a new file, and separately back up original replica files / objects through the deployment procedure.',outcomes:[['Metadata check is valid','This validates constraints only; it does not prove the payload is readable.'],['Metadata is inconsistent','Correct it only with independent validation and acknowledged-write history; use the documented metadata repair branch.']],verify:'Original payloads are preserved separately from the metadata snapshot and diagnostic outputs.'},
  {title:'Scan segment coverage',phase:'Investigate',question:'Does observed coverage match the finalized metadata boundary?',commands:['log scan','meta segment show'],look:'Start with quick scan, then raw scan for the suspected segment using --from-segment / --to-segment. Raw scan verifies local block checksums.',outcomes:[['short','Compare expected and observed ranges, then probe every usable alternative.'],['unknown / unreachable / cap reached','Fix access, location or survey bounds; missing coverage is not proven.'],['compacted / reclaimed','Probe shared objects where retained; intentionally reclaimed data is not automatically corruption.']],verify:'Expected boundaries and observed coverage are recorded with incomplete replies kept visible.'},
  {title:'Probe & inspect replicas',phase:'Confirm',question:'Is there a healthy copy, and what can be read after the fault?',commands:['segment probe','segment inspect'],look:'Probe the suspected entry and inspect beyond the damaged block. For object_storage, multiple node replies may read the same shared copy.',outcomes:[['Any healthy copy exists','Preserve it and recover unhealthy replicas through the supported deployment path. Do not skip readable data.'],['All usable alternatives fail at exact entries','Record the inclusive bad range and verified healthy tail. Timeouts or incomplete probes do not meet this condition.']],verify:'Every usable alternative and the exact bad range have been checked; healthy data is preserved.'},
  {title:'Preview / declare skip range',phase:'Act',question:'Has the loss of this exact inclusive range been explicitly accepted?',commands:['log skip-range list','log skip-range add'],risk:'Skip-range accepts data loss; it does not repair files or compaction. Use only for an exactly verified unreadable range after all usable alternatives fail.',look:'Review existing declarations first. Fill the confirmed segment, --from-entry, --to-entry and a loss-acceptance reason. Run without --yes to preview the CLI checks and impact.',outcomes:[['Preview / exit 7','No declaration was committed. Review the output before explicitly accepting.'],['Declaration committed','Readers may skip the inclusive range. Record accepted entry IDs; removing the declaration does not restore their data.']],verify:'The committed declaration matches the exact accepted loss, or the action remains an uncommitted preview.'},
  {title:'Recheck reader progress',phase:'Verify',question:'Did the original reader cross the range and reach the healthy tail?',commands:['log skip-range list','log readers','segment probe'],look:'Keep the same stalled reader running and allow refresh/report timing. Probe retained healthy entries after the bad range; verify actual application reads and its durable checkpoint.',outcomes:[['Same reader crosses the range','Record returned entry IDs, the accepted loss and the application checkpoint.'],['Only the Admin command succeeded','Recovery is not established until the application reader progresses.']],verify:'The same reader and application checkpoint pass the skipped range and reach the verified healthy tail.'},
  {title:'Retention (optional)',phase:'Optional',question:'Is a durable application checkpoint beyond the damaged segment established?',commands:['meta segment show','log truncate','log scan','marking list'],risk:'Truncation advances retention and can lead to physical deletion. It is a separate operation, not an automatic follow-up to skip-range.',look:'Only after application recovery and a verified checkpoint, review the normal retention preview. Apply requires the log revision, reason and a new backup. Observe auditor transitions separately.',outcomes:[['No verified checkpoint','Stop here; do not advance retention merely to clear an error.'],['Frontier applied','Observe Truncated / reclaimed and asynchronous cleanup. Do not require reading intentionally retired data.']],verify:'If retention was intended, its checkpoint basis and later auditor / cleanup state are recorded.'}
 ]}
];
const operationSteps={},operationCommands={},operationExpanded={};
function expandedOperationSteps(book){return operationExpanded[book]??=( {[operationSteps[book]||0]:true} );}
let operationViewKey='',operationHomes=null;
const connectionNames={service:'WP Admin context',metadata:'Client etcd + metadata prefix',etcd:'Etcd endpoints',telemetry:'Metrics / Trace target + incident interval',kubernetes:'kubectl / Kubernetes configuration',local:'Local CLI configuration',mixed:'Command-specific metadata / Admin / storage inputs'};
function navigate(tab){
 const previous=state.tab+'|'+state.tool;
 if(tab==='cluster'||tab==='metadata'){state.tool=tab;state.tab='commands';}
 else{state.tab=tab;if(tab==='commands')state.tool='';}
 $('#request-error').hidden=true;render();
 if(previous!==state.tab+'|'+state.tool)window.scrollTo({top:0,behavior:'auto'});
 if(state.tab==='diagnostics')loadRunHistory();
}
// The editor is mounted in Operations rather than duplicated: connections, validation,
// subprocess execution and result history remain owned by the existing command console.
function operationMounts(home=false){
 if(!operationHomes){operationHomes={};for(const id of ['command-config','command-console','command-history']){const node=$('#'+id),marker=document.createElement('span');marker.hidden=true;node.before(marker);operationHomes[id]=marker;}}
 for(const [id,mount]of Object.entries({'command-config':'operation-config-mount','command-console':'operation-output-mount','command-history':'operation-history-mount'})){
  const node=$('#'+id);if(home){if(node.previousSibling!==operationHomes[id])operationHomes[id].after(node);}else if(node.parentElement!==$('#'+mount))$('#'+mount).append(node);
 }
}
function rememberOperationInputs(){
 if(state.commands&&state.commandName&&$('#command-config')?.children.length){const c=commandDefinitionByName(state.commandName);if(c){saveCommandFields(c);rememberCommandPanels();}}
}
function renderOperations(){
 const source=state.runbookSource;
 $('#runbook-return').hidden=!source||state.tab==='runbooks';
 $('#runbook-return').innerHTML=source?`<button class="text-link" data-return-runbook>← Back to Operations</button><span>${esc(source.title)} · Step ${source.index+1}</span>`:'';
 if(state.tab==='runbooks')renderRunbook();else{if(operationHomes)operationMounts(true);if(operationViewKey)state.commandEditorKey='';renderCommandDirectory();operationViewKey='';}
}
function renderRunbook(){
 if(!state.commands)return;
 const selected=runbooks.find(b=>b.id===state.runbook)||runbooks[0],index=operationSteps[selected.id]||0,step=selected.steps[index],key=selected.id+'|'+index,expanded=expandedOperationSteps(selected.id);
 const header=$('#runbooks-view .question-header');header.querySelector('h1').textContent='Operational Scenarios';header.querySelector('p').textContent='Select a step, set its parameters and run it. Execution results appear on the right.';
 $('#runbooks-view .input-region strong').textContent='Steps & parameters';$('#runbooks-view .input-region p').textContent='Choose what to run. Only the selected command executes.';
 const nav=$('#runbook-step-nav');nav.previousElementSibling.querySelector('.panel-title').innerHTML='<span class="workflow-number">2</span>Steps';
 nav.nextElementSibling.hidden=true;
 $('#runbook-picker').innerHTML=`<select id="operation-scenario-select" aria-label="Operational scenario">${runbooks.map(b=>`<option value="${b.id}" ${b.id===selected.id?'selected':''}>${esc(b.title)}</option>`).join('')}</select>`;
 $('#operation-scenario-select').onchange=e=>{rememberOperationInputs();state.runbook=e.target.value;state.runbookSource=null;render();};
 if(operationViewKey!==key){
  if(operationHomes)operationMounts(true);
  nav.innerHTML=selected.steps.map((row,i)=>{const current=i===index,chosen=operationCommands[selected.id+'|'+i]||row.commands[0];return `<section class="operation-step-item ${current?'active':''}"><button class="operation-step-nav ${current?'active':''}" data-toggle-operation-step="${i}" aria-expanded="${!!expanded[i]}" aria-controls="operation-step-editor-${i}" aria-current="${current?'step':'false'}"><span>${i+1}</span><div><b>${esc(row.title)}</b><small data-operation-step-state="${i}">Not run</small></div><span class="operation-step-chevron" aria-hidden="true">${expanded[i]?'⌄':'›'}</span></button><div id="operation-step-editor-${i}" class="operation-step-editor" ${expanded[i]?'':'hidden'}><section class="operation-command-plan"><div class="operation-command-heading"><b>Operation</b><button class="text-link" data-operation-help="${i}">Step help ↗</button></div><div ${current?'id="operation-command-picker"':'class="operation-command-options"'}>${current?'':row.commands.map(command=>`<button class="operation-command ${command===chosen?'selected':''}" data-operation-command="${esc(command)}" data-operation-command-step="${i}" aria-pressed="${command===chosen}"><code>${esc(command)}</code></button>`).join('')}</div>${current?'<p id="operation-command-needs"></p><p id="operation-action-notice" hidden></p>':'<button class="text-link operation-configure-step" data-review-step="'+i+'">Set parameters &amp; view results →</button>'}</section>${current?`<div id="operation-config-mount"></div><div class="operation-left-actions">${selected.scenario?'<button class="text-link" data-operation-diagnostics>Historical checks ↗</button>':''}${index+1<selected.steps.length?`<button data-review-step="${index+1}">Next step →</button>`:''}</div>`:''}</div></section>`;}).join('');
  $('#runbook-content').innerHTML=`<div class="workspace-region-heading output-region"><span class="region-tag">OUTPUT</span><div><strong>Step execution &amp; results</strong><p>${esc(selected.title)}</p></div></div><section class="operation-current-run"><header class="operation-result-heading"><div><span class="operation-phase">Step ${index+1} of ${selected.steps.length}</span><h2>${esc(step.title)}</h2></div><span id="operation-result-state" class="operation-result-state">Not run</span></header><div id="operation-output-mount"></div></section><div id="operation-history-mount"></div>`;
  operationViewKey=key;state.commandEditorKey='';
 }
 operationMounts();
 const name=operationCommands[key]||step.commands[0];state.commandName=name;
 $('#operation-command-picker').innerHTML=step.commands.map(command=>`<button class="operation-command ${name===command?'selected':''}" data-operation-command="${esc(command)}" data-operation-command-step="${index}" aria-pressed="${name===command}"><code>${esc(command)}</code></button>`).join('');
 const c=commandDefinitionByName(name);$('#operation-command-needs').textContent=c?'Needs: '+dependencySummary(c):'Command unavailable in this build.';
 const impact={'log skip-range add':'Accepts loss of the declared entries. Preview first; skipping does not repair files.','logstore force-flush':'Requests a sync on the selected loaded writer.','logstore fence-quorum':'Fences the selected quorum and can interrupt writes. Review the preview before accepting.','log truncate':'Advances retention and can lead to data deletion. Verify the application checkpoint first.'};
 const notice=$('#operation-action-notice');notice.hidden=!impact[name];notice.textContent=impact[name]||'';
 renderSelectedCommand();
 // Interactive CLI replies are inputs too; keep them with the execution controls.
 const reply=$('#command-input-row'),form=$('#command-config .console-form');if(reply&&form&&reply.parentElement!==form)form.append(reply);
 updateOperationExpansions();renderOperationExecution();
}
function updateOperationExpansions(){
 const expanded=expandedOperationSteps(state.runbook);
 for(const button of document.querySelectorAll('[data-toggle-operation-step]')){const index=Number(button.dataset.toggleOperationStep),open=!!expanded[index];button.setAttribute('aria-expanded',String(open));$('#operation-step-editor-'+index).hidden=!open;button.querySelector('.operation-step-chevron').textContent=open?'⌄':'›';}
}
function toggleOperationStep(index){
 const book=runbooks.find(b=>b.id===state.runbook)||runbooks[0];if(!book.steps[index])return;
 const expanded=expandedOperationSteps(book.id);expanded[index]=!expanded[index];
 // Folding never changes selection, remounts an editor or affects a running request.
 updateOperationExpansions();
}
function renderOperationExecution(){
 if(state.tab!=='runbooks'||!$('#operation-result-state'))return;
 const selected=runbooks.find(b=>b.id===state.runbook)||runbooks[0],index=operationSteps[selected.id]||0;
 for(const [i,step]of selected.steps.entries()){
  const runs=step.commands.map(name=>{const c=commandDefinitionByName(name);return c?state.commandRuns[commandRunKey(c,{book:selected.id,index:i})]:null;}).filter(Boolean);
  const running=runs.some(run=>!run.done),failed=runs.some(run=>run.done&&(run.result.status==='failed'||run.result.exit_code!==undefined&&run.result.exit_code!==0)),done=runs.filter(run=>run.done).length;
  const el=$(`[data-operation-step-state="${i}"]`);if(el){el.textContent=running?'Running…':failed?'Review output':done?done+' / '+step.commands.length+' operations executed':'Not run';el.dataset.state=running?'running':failed?'attention':done?'executed':'pending';}
 }
 const c=commandDefinitionByName(state.commandName),run=c?state.commandRuns[commandRunKey(c)]:null,r=run?.result,label=$('#operation-result-state');
 label.textContent=!run?'Not run':!run.done?'Running…':r.exit_code!==undefined?'Finished · exit '+r.exit_code:r.status==='failed'?'Request failed':'Finished';
 label.dataset.state=!run?'pending':!run.done?'running':r.status==='failed'||r.exit_code!==undefined&&r.exit_code!==0?'attention':'executed';
 if(run?.id)label.title='Run ID: '+run.id;else label.removeAttribute('title');
}
function showOperationHelp(index=operationSteps[state.runbook]||0){
 const book=runbooks.find(b=>b.id===state.runbook)||runbooks[0],step=book.steps[index];
 dialog(step.title,book.title+' · Step '+(index+1),`<section class="inspection-section"><h3>${esc(step.question)}</h3><p>${esc(step.look)}</p></section>${step.risk?`<section class="inspection-section"><h3>Impact</h3><p>${esc(step.risk)}</p></section>`:''}<section class="inspection-section"><h3>Interpret the output</h3>${step.outcomes.map(([when,then])=>`<h4>${esc(when)}</h4><p>${esc(then)}</p>`).join('')}</section><section class="inspection-section"><h3>Verify at the application</h3><p>${esc(step.verify)}</p></section><a href="${operationsDocs+book.doc}" target="_blank" rel="noopener noreferrer">Full scenario guide ↗</a>`);
}
function selectOperationStep(index){
 const book=runbooks.find(b=>b.id===state.runbook)||runbooks[0];if(!book.steps[index])return;
 rememberOperationInputs();expandedOperationSteps(book.id)[index]=true;operationSteps[book.id]=index;render();
}
function openRunbookStep(index){selectOperationStep(index);}
function openOperationDiagnostics(){
 const book=runbooks.find(b=>b.id===state.runbook),index=operationSteps[book.id]||0;if(!book.scenario)return;
 if(state.busy){error(Error('Wait for the active diagnosis to finish, or stop it before choosing another scenario.'));return;}
 rememberOperationInputs();state.runbookSource={id:book.id,title:book.title,index};state.scenario=book.scenario;$('#mode').value='scenario';state.tab='diagnostics';reset();
}
function shellArgument(value){return /^[A-Za-z0-9_./:@=+-]+$/.test(value)?value:"'"+value.replace(/'/g,"'\"'\"'")+"'";}
function openCommandView(c){
 const first=uiArguments(c)[0],key=first?canonicalParameter(c,first.name):'';
 const arg=state.tab==='commands'&&!state.tool&&state.commandName===c.name?commandPayload(c).arguments[0]:key?commonValue(c,key):'';
 closeDialog();
 if(c.visual==='diagnostics'){navigate('diagnostics');if(c.name==='diag catalog')catalog();}
 else navigate(c.visual);
 if(c.visual==='metadata'&&arg){$('#meta-log').value=arg;metadataControls();}
}

document.addEventListener('click',e=>{
 const category=e.target.closest('[data-command-category]');if(category){state.commandCategory=category.dataset.commandCategory;renderCommandDirectory();return;}
 const command=e.target.closest('[data-command]');if(command){showCommand(command.dataset.command);return;}
 const book=e.target.closest('[data-runbook]');if(book){rememberOperationInputs();state.runbook=book.dataset.runbook;state.runbookSource=null;render();return;}
 const fold=e.target.closest('[data-toggle-operation-step]');if(fold){toggleOperationStep(Number(fold.dataset.toggleOperationStep));return;}
 const review=e.target.closest('[data-review-step]');if(review){selectOperationStep(Number(review.dataset.reviewStep));return;}
 const op=e.target.closest('[data-operation-command]');if(op){rememberOperationInputs();const index=Number(op.dataset.operationCommandStep),key=state.runbook+'|'+index;operationCommands[key]=op.dataset.operationCommand;expandedOperationSteps(state.runbook)[index]=true;operationSteps[state.runbook]=index;state.commandEditorKey='';render();return;}
 if(e.target.closest('[data-operation-diagnostics]')){openOperationDiagnostics();return;}
 const help=e.target.closest('[data-operation-help]');if(help){showOperationHelp(Number(help.dataset.operationHelp));return;}
 const step=e.target.closest('[data-open-step]');if(step){openRunbookStep(Number(step.dataset.openStep));return;}
 if(e.target.closest('[data-return-runbook]')){state.tab='runbooks';state.runbookSource=null;render();return;}
 if(e.target.closest('[data-back-commands]')){navigate('commands');}
});
