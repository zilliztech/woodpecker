// Browser acceptance for the real local WebUI/API. No npm dependencies.
import {spawn} from 'node:child_process';
import {mkdtemp,readFile,writeFile,rm,readdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import assert from 'node:assert/strict';
import {join} from 'node:path';

const [url,output]=process.argv.slice(2);
assert(url&&output,'Usage: wal_browser.mjs URL OUTPUT');
const chromePath=process.env.CHROME_BIN||['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome','/usr/bin/google-chrome','/usr/bin/google-chrome-stable','/usr/bin/chromium','/usr/bin/chromium-browser'].find(existsSync);
assert(chromePath,'Chrome is required; set CHROME_BIN');
const profile=await mkdtemp('/tmp/wpdiag-webui-chrome-');
const chrome=spawn(chromePath,['--headless=new','--disable-gpu','--no-first-run','--no-default-browser-check','--no-sandbox','--remote-debugging-port=0','--user-data-dir='+profile,'about:blank'],{stdio:['ignore','ignore','pipe']});
let chromeStderr='',chromeError;
chrome.stderr.on('data',data=>{chromeStderr=(chromeStderr+data.toString()).slice(-32768);});
chrome.on('error',error=>{chromeError=error;});
const chromeClosed=new Promise(resolve=>chrome.once('close',resolve));
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
let ws,call,evaluate;
const errors=[],checks=[];
try{
 let port;
 for(let i=0;i<100;i++){
  if(chromeError)throw Error('Chrome launch failed: '+chromeError.message);
  if(chrome.exitCode!==null||chrome.signalCode!==null)throw Error('Chrome exited before startup: '+chromeStderr);
  try{port=(await readFile(join(profile,'DevToolsActivePort'),'utf8')).split('\n')[0];break;}catch{await pause(100);}
 }
 assert(port,'Chrome did not start within 10 seconds: '+chromeStderr);
 const target=await(await fetch('http://127.0.0.1:'+port+'/json/new?about:blank',{method:'PUT'})).json();
 ws=new WebSocket(target.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{ws.onopen=resolve;ws.onerror=reject;});
 let seq=0;const pending=new Map();
 ws.onmessage=event=>{const m=JSON.parse(event.data);if(m.id){const p=pending.get(m.id);if(p){pending.delete(m.id);clearTimeout(p.timer);m.error?p.reject(Error(JSON.stringify(m.error))):p.resolve(m.result);}}else if(m.method==='Runtime.exceptionThrown')errors.push(m.params.exceptionDetails.text);};
 call=(method,params={})=>new Promise((resolve,reject)=>{const id=++seq;const timer=setTimeout(()=>{pending.delete(id);reject(Error('Timeout '+method));},20000);pending.set(id,{resolve,reject,timer});ws.send(JSON.stringify({id,method,params}));});
 evaluate=async expression=>{const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true,userGesture:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value;};
 const wait=async(expression,message,timeout=10000)=>{const deadline=Date.now()+timeout;while(Date.now()<deadline){if(await evaluate(expression))return;await pause(80);}throw Error(message+'\n'+await evaluate('document.body.innerText.slice(-5000)'));};

 const click=sel=>evaluate(`document.querySelector(${JSON.stringify(sel)}).click()`);
 const shot=async name=>{const s=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});await writeFile(join(output,name+'.png'),Buffer.from(s.data,'base64'));};
 const set=async(sel,value)=>evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(sel)});e.value=${JSON.stringify(value)};e.dispatchEvent(new Event('input',{bubbles:true}));e.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 await call('Page.enable');await call('Runtime.enable');await call('Network.enable');
 await call('Browser.setDownloadBehavior',{behavior:'allow',downloadPath:output});
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});
 let workspaceGate;
 ws.addEventListener('message',event=>{const message=JSON.parse(event.data);if(message.method==='Fetch.requestPaused')workspaceGate=message.params.requestId;});
 await call('Fetch.enable',{patterns:[{urlPattern:'*/api/workspace',requestStage:'Request'}]});
 await call('Page.navigate',{url});await call('Page.bringToFront');
 await wait('document.querySelectorAll("#overview-modules .module-card").length===3','capability cards render before configuration');
 assert.equal(await evaluate('state.info'),null,'overview renders before the blocked configuration response');
 assert(await evaluate('document.querySelector("#overview-modules").innerText.includes("Loading configuration")'));
 assert(workspaceGate,'configuration request is held until cards are visible');
 await call('Fetch.continueRequest',{requestId:workspaceGate});await call('Fetch.disable');


 await wait('typeof state!=="undefined"&&state.info!==null&&state.catalog!==null','workspace ready');

 assert.equal(await evaluate('state.info.session.context'),'fixture-a');
 assert.equal(await evaluate('state.catalog.checks.length'),55);
 assert.deepEqual(await evaluate('sharedMetadata()'),{});
 assert(!await evaluate('JSON.stringify(state.info.context_defaults).includes("woodpecker")'));
 assert.equal(await evaluate('Object.keys(state.info.client_meta_profiles).length'),0);
 await click('.workspace-tabs [data-tab="diagnostics"]');
 assert(!await evaluate('!!document.querySelector(`#diagnostics-view [data-binding-key="metadata.bucket"]`)'));
 const assertContextDetails=async()=>{
  const result=await evaluate(`(()=>{const panel=document.querySelector('[data-binding-panel="server-context-details"]');panel.open=true;const links=[...panel.querySelectorAll('.context-connections>div')].filter(r=>['Prometheus','Trace','Grafana'].includes(r.querySelector('dt').textContent)).map(r=>({name:r.querySelector('dt').textContent,href:r.querySelector('a')?.href,target:r.querySelector('a')?.target,rel:r.querySelector('a')?.rel}));const map=panel.querySelector('.host-map-scroll');map.scrollLeft=map.scrollWidth;return {links,mapCount:map.querySelectorAll('tr').length,nowrap:[...map.querySelectorAll('th,td')].every(e=>getComputedStyle(e).whiteSpace==='nowrap'),pageFits:document.documentElement.scrollWidth<=innerWidth+1};})()`);
  assert.deepEqual(result.links.map(l=>l.name),['Prometheus','Trace','Grafana']);
  assert(result.links.every(l=>l.href?.startsWith('http')&&l.target==='_blank'&&l.rel.includes('noopener')));
  assert(result.mapCount>0&&result.nowrap&&result.pageFits,'compact context details: '+JSON.stringify(result));
 };
 // Explicit source preflight is independent of incident-scope fields.
 await evaluate('document.querySelector(`[data-binding-panel="context-evidence"]`).open=true');
 for(const source of ['metrics','trace']){
  assert(await evaluate(`(()=>{const b=document.querySelector('[data-check-connection="${source}"]');b.click();return b.disabled&&b.textContent.includes('Checking');})()`),'preflight starts with visible progress');
  await wait(`document.querySelector('[data-connection-feedback="${source}"]').textContent.includes('API accessible')`,'successful '+source+' connection check');
 }
 await assertContextDetails();
 await call('Emulation.setDeviceMetricsOverride',{width:390,height:700,deviceScaleFactor:1,mobile:false});await assertContextDetails();
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});
 await evaluate('document.querySelector(`[data-binding-panel="server-context-details"]`).open=false');
 const assertFullFile=async(kind)=>{
  const response=await evaluate(`api(${JSON.stringify(kind==='yaml'?'/api/workspace/config':'/api/workspace/candidates/config')})`);
  const expected=kind==='yaml'?response.yaml:response.content;
  assert.equal(await evaluate('document.querySelector(".config-source-yaml").textContent'),expected,'complete file contents match the API exactly');
  assert.equal(await evaluate('getComputedStyle(document.querySelector(".config-source-yaml")).whiteSpace'),'pre');
  const scrolling=await evaluate(`(()=>{const e=document.querySelector('.config-source-yaml'),r=e.getBoundingClientRect();e.scrollTop=e.scrollHeight;e.scrollLeft=e.scrollWidth;const horizontal=e.scrollLeft;e.scrollLeft=0;const text=e.firstChild,last=text.data.trimEnd().length;const range=document.createRange();range.setStart(text,Math.max(0,last-1));range.setEnd(text,last);const end=range.getBoundingClientRect(),footer=document.querySelector('.drawer-bottom').getBoundingClientRect();return {scrolls:e.scrollHeight>e.clientHeight,horizontal,top:e.scrollTop,atEnd:Math.abs(e.scrollHeight-e.clientHeight-e.scrollTop)<=2,lastLineVisible:end.top>=r.top-1&&end.bottom<=r.bottom+1,footerVisible:footer.bottom<=innerHeight+1,width:document.documentElement.scrollWidth};})()`);
  assert(scrolling.scrolls&&scrolling.top>0&&scrolling.atEnd&&scrolling.lastLineVisible&&scrolling.footerVisible,kind+' scroll to complete last line: '+JSON.stringify(scrolling));
  assert(scrolling.horizontal>0,kind+' long lines scroll horizontally');
  assert(scrolling.width<=await evaluate('innerWidth')+1,'no page overflow');
 };
 await click('#diagnostics-view .server-context-step [data-view-config]');await wait('!!document.querySelector(".config-source-yaml")','config view');
 assert(await evaluate('document.querySelector(".config-source-yaml").textContent.startsWith("# Preserve source comments and ordering")'));
 assert(await evaluate('document.querySelector(".drawer").getBoundingClientRect().width>=1000'),'source drawer is wider');
 await assertFullFile('yaml');await shot('cli-source-end');
 await call('Emulation.setDeviceMetricsOverride',{width:390,height:700,deviceScaleFactor:1,mobile:false});await assertFullFile('yaml');await shot('cli-source-end-mobile');
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});await evaluate('closeDialog()');
 await click('[data-view-recent-config]');await wait('!!document.querySelector(".recent-config-json")','recent input config');
 assert(await evaluate('document.querySelector(".config-file-location code").textContent.endsWith("/recent-inputs.json")'));
 assert(await evaluate('JSON.parse(document.querySelector(".recent-config-json").textContent)["metadata.etcd"].includes("http://previous-etcd:2379")'));
 await assertFullFile('json');await shot('recent-input-config-end');
 await call('Emulation.setDeviceMetricsOverride',{width:390,height:700,deviceScaleFactor:1,mobile:false});await assertFullFile('json');await shot('recent-input-config-end-mobile');
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});await evaluate('closeDialog()');

 // Rule viewers expose full source files, or a clearly labeled built-in template.
 for(const source of ['metrics','trace']) {
  await click('[data-view-rules="'+source+'"]');await wait('!!document.querySelector(".config-source-yaml")','rule file viewer');
  assert(await evaluate('document.querySelector(".config-file-location code").textContent.endsWith("diag-'+source+'.yaml")'));
  assert(await evaluate('document.querySelector(".config-source-yaml").textContent.includes("schema_version: wp.diag.rules/v1")'));
  assert(await evaluate('document.querySelector(".config-file-note").textContent.includes("File does not exist")'));
  await evaluate('closeDialog()');
 }
 // The visible selector key is the same canonical key used in rule files and CLI.
 for(const source of ['metrics','trace']){
  await set('#mode',source);
  const keys=await evaluate('[...document.querySelector("#atomic-check").options].map(o=>({value:o.value,text:o.textContent}))');
  assert(keys.length>0&&keys.every(k=>k.value===k.text&&/^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/.test(k.value)));
  assert(await evaluate('document.querySelector("#atomic-check-description").textContent.length>0'));
  const key=keys[0].value;
  await click('[data-view-rules="'+source+'"]');await wait('!!document.querySelector(".config-source-yaml")','canonical rule keys');
  assert(await evaluate('document.querySelector(".config-source-yaml").textContent.includes('+JSON.stringify(key+':')+')'));
  await evaluate('closeDialog()');
 }
 await set('#mode','scenario');
 await set('#scenario','slow-writes');await set('#period','5');
 await set('[data-binding-role="server"][data-binding-key="node_id"]','node-1');
 await set('[data-binding-role="client"][data-binding-key="log_namespace"]','tenant');
 await set('[data-binding-role="client"][data-binding-key="log_id"]','1');
 assert(!await evaluate('document.querySelector("#run").disabled'));
 await click('#run');await wait('!!state.report','scenario',40000);
 assert(await evaluate(`(()=>{const group=document.querySelector('.current-diagnostic-run');return ['.execution-plan','#diagnostic-result','#diagnostic-details','.result-actions','#investigation'].every(sel=>group.contains(document.querySelector(sel)))&&!group.contains(document.querySelector('#diagnostic-history'));})()`),'one run groups plan, results and analysis; history remains separate');
 await evaluate('document.querySelector(".current-diagnostic-run").scrollIntoView({block:"start",behavior:"instant"})');await shot('grouped-diagnostic-run');

 assert(await evaluate('state.report.summary.metrics.abnormal>0'));
 const run=await evaluate('state.job.id');
 await set('[data-binding-role="client"][data-binding-key="log_namespace"]','new-explicit-namespace');
 await evaluate('restore('+JSON.stringify(run)+')');
 assert.equal(await evaluate('initialBindings().client.log_namespace'),'new-explicit-namespace','view must not overwrite current inputs');
 await evaluate('restore('+JSON.stringify(run)+',"result",true)');
 assert.equal(await evaluate('initialBindings().client.log_namespace'),'tenant','only explicit reuse restores request inputs');
 // Source candidates reflect server-side successful queries, never typed drafts.
 await set('#mode','metrics');await set('#atomic-check','writer-throughput');
 const initial=await evaluate('api("/api/workspace/candidates")');
 const endpoint=initial.values.metrics_endpoint[0].value;
 await evaluate('setSharedEvidenceAddress("metrics",'+JSON.stringify(endpoint+'/failed')+');diagnosticBindingsRevision++;renderDiagnosticBindings(true);render()');
 await click('#run');await wait('!!state.report&&!state.busy','failed source',40000);
 assert(await evaluate('!!document.querySelector(`[data-workflow-help="incomplete"]`)'),'summary explains incomplete collection');
 await evaluate(`document.querySelector('[data-workflow-help="incomplete"]').scrollIntoView({block:'center',behavior:'instant'})`);
 await pause(160);
 const hoverPoint=await evaluate(`(()=>{const r=document.querySelector('[data-workflow-help="incomplete"]').getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};})()`);
 await call('Input.dispatchMouseEvent',{type:'mouseMoved',...hoverPoint});
 await wait('!document.querySelector("#workflow-help-popup").hidden','hover explanation');
 assert(await evaluate('document.querySelector("#workflow-help-popup").textContent.includes("not a confirmed Woodpecker fault")'));

 await evaluate('document.querySelector(`[data-workflow-help="unassessed"]`).focus()');
 await wait('!document.querySelector("#workflow-help-popup").hidden','status explanation');
 assert(await evaluate('document.querySelector("#workflow-help-popup").textContent.includes("not mutually exclusive")'));
 await shot('collection-status-explanation');
 await evaluate('closeWorkflowHelp();document.querySelector("#diagnostic-details").open=true');
 assert(await evaluate('!!document.querySelector(`#result-detail-list [data-workflow-help="source_unavailable"]`)'));
 await click('[data-check-connection="metrics"]');
 await wait('document.querySelector(`[data-connection-feedback="metrics"]`).textContent.includes("Connection failed")','source preflight failure');
 assert(await evaluate('document.querySelector(`[data-connection-feedback="metrics"]`).textContent.includes("HTTP 503")'));
 assert(!await evaluate('api("/api/workspace/candidates").then(x=>(x.values.metrics_endpoint||[]).some(v=>v.value.endsWith("/failed")))'));
 await evaluate('setSharedEvidenceAddress("metrics",'+JSON.stringify(endpoint+'/empty')+');diagnosticBindingsRevision++;renderDiagnosticBindings(true);render()');
 await click('#run');await wait('!!state.report&&!state.busy','empty successful source',40000);
 assert(await evaluate('api("/api/workspace/candidates").then(x=>x.values.metrics_endpoint.some(v=>v.value.endsWith("/empty")))'));
 await evaluate('document.querySelector("[data-binding-panel=context-evidence]").open=true;document.querySelector("[data-shared-evidence=metrics]").focus();document.querySelector("[data-shared-evidence=metrics]").click()');
 await pause(200);await evaluate('document.querySelector("[data-shared-evidence=metrics]").blur();document.querySelector("[data-shared-evidence=metrics]").scrollIntoView({block:"center",behavior:"instant"});document.querySelector("[data-shared-evidence=metrics]").focus();document.querySelector("[data-shared-evidence=metrics]").click()');
 await wait('!!document.querySelector(".recent-values [role=option]")','source suggestions');
 const sourceBefore=await evaluate('document.querySelector("[data-shared-evidence=metrics]").value');
 await click('.recent-values .recent-value-remove');await pause(150);
 assert.equal(await evaluate('document.querySelector("[data-shared-evidence=metrics]").value'),sourceBefore,'delete does not edit active input');
 // Shared metadata remains temporary. Each candidate fills exactly one key.
 await click('.workspace-tabs [data-tab="commands"]');
 await evaluate('state.commandName="meta log list";state.commandEditorKey="";render()');
 await set('[data-binding-key="metadata.etcd"]','http://manual-etcd:2379');
 await set('[data-binding-key="metadata.prefix"]','typed/wp');
 await evaluate('document.querySelector(`[data-binding-key="metadata.etcd"]`).focus();document.querySelector(`[data-binding-key="metadata.etcd"]`).click()');
 await wait('!!document.querySelector(".recent-values [role=option]")','independent endpoint candidates');
 await click('.recent-values .recent-value-text');
 assert.equal(await evaluate('sharedMetadata().prefix'),'typed/wp','endpoint candidate does not select prefix');
 const explicit=await evaluate('sharedMetadata().etcd');
 await evaluate('switchSharedContext("fixture-b")');
 assert.equal(await evaluate('sharedMetadata().etcd'),explicit);
 assert.equal(await evaluate('sharedMetadata().prefix'),'typed/wp');
 // Compatible physical parameters remain current values, with no authority-indexed restoration.
 await evaluate('state.commandName="segment probe";state.commandEditorKey="";render()');
 await set('[data-shared-key="bucket"]','typed-bucket');await set('[data-shared-key="rootPath"]','typed-root');
 await evaluate('state.commandName="segment inspect";state.commandEditorKey="";render()');
 assert.equal(await evaluate('document.querySelector("[data-shared-key=bucket]").value'),'typed-bucket');
 // Command Explorer executes the native diagnostic command with a separate
 // bindings JSON; no client/server profiles are written into cli.yaml.
 await evaluate('showCommand("diag metrics check")');
 await set('[data-command-argument="0"]','writer-throughput');
 await click('#execute-command');
 await wait('Object.values(state.commandRuns).some(r=>r.done&&r.result.name==="diag metrics check")','native diagnostic command',30000);
 const command=await evaluate('Object.values(state.commandRuns).find(r=>r.done&&r.result.name==="diag metrics check").result');
 assert.equal(command.exit_code,0,command.stderr);
 assert.equal(JSON.parse(command.stdout).schema_version,'wp.diag/v1');
 await shot('independent-client-inputs');
 await call('Page.reload');await wait('typeof state!=="undefined"&&!!state.info','reload');
 assert.deepEqual(await evaluate('sharedMetadata()'),{},'temporary client inputs are not restored from storage');
 assert.equal(await evaluate('initialBindings().client.log_namespace||""'),'');
 await click('.workspace-tabs [data-tab="diagnostics"]');await shot('context-only-diagnostics');
 for(const width of [1440,900,390]) { await call('Emulation.setDeviceMetricsOverride',{width,height:1050,deviceScaleFactor:1,mobile:false});await pause(80);assert(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'overflow '+width); }
 // A broken rules file blocks diagnosis without blocking unrelated operations.
 await evaluate(`(async()=>{const original=api;api=async(path,...args)=>{if(path==='/api/diag/catalog')throw Error('Invalid diagnostic rules fixture');return original(path,...args)};try{await refresh();populateAtomic();render();}finally{api=original}})()`);
 assert.equal(await evaluate('state.info.diagnostics_error'),'Invalid diagnostic rules fixture');
 assert(await evaluate('state.commands.commands.length>0'));
 await click('.workspace-tabs [data-tab="commands"]');
 assert(await evaluate('!document.querySelector("#commands-view").hidden'));
 assert.equal(errors.length,0,JSON.stringify(errors));
 console.log(JSON.stringify({passed:true,complete_source_files:true,scroll_to_end:true,independent_candidates:true,success_only:true,history_view_preserves_inputs:true,explicit_reuse:true,temporary_inputs:true,js_errors:errors}));
}finally{ws?.close();chrome.kill();await chromeClosed;await rm(profile,{recursive:true,force:true});}
