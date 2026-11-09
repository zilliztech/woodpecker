// Operations interaction acceptance. Native execution is intercepted with a fixture; no cluster operations or model calls.
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

 const click=sel=>evaluate(`(()=>{const selector=${JSON.stringify(sel)},scoped=selector.startsWith('[data-operation-command=')||selector==='[data-operation-help]'?'#operation-step-editor-'+(operationSteps[state.runbook]||0)+' '+selector:selector;document.querySelector(scoped).click();})()`);
 const shot=async name=>{const s=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:false});await writeFile(join(output,name+'.png'),Buffer.from(s.data,'base64'));};
 const set=async(sel,value)=>evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(sel)});e.value=${JSON.stringify(value)};e.dispatchEvent(new Event('input',{bubbles:true}));e.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 await call('Page.enable');await call('Runtime.enable');await call('Network.enable');await call('Page.bringToFront');await call('Emulation.setFocusEmulationEnabled',{enabled:true});
 await call('Browser.setDownloadBehavior',{behavior:'allow',downloadPath:output});
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});
 await call('Page.navigate',{url});

 await wait('typeof state!=="undefined"&&state.info!==null&&state.catalog!==null','workspace ready');







 await evaluate(`(()=>{window.diagnosticSubmissionRequests=0;const original=api;api=async(path,body)=>{if(path==='/api/diag/runs'){diagnosticSubmissionRequests++;throw Error('Field validation must not submit a diagnosis');}return original(path,body);};})()`);
 await click('.workspace-tabs [data-tab="diagnostics"]');
 await set('#mode','scenario');await set('#scenario','slow-writes');
 const node='[data-binding-role="server"][data-binding-key="node_id"]',prom='[data-shared-evidence="metrics"]',trace='[data-shared-evidence="trace"]';
 const addresses=await evaluate(`({metrics:document.querySelector(${JSON.stringify(prom)}).value,trace:document.querySelector(${JSON.stringify(trace)}).value})`);
 assert(addresses.metrics&&addresses.trace,'configured local sources');
 await set(node,'');
 assert.equal(await evaluate(`document.querySelector(${JSON.stringify(node)}).getAttribute('aria-required')`),'true');
 assert.equal(await evaluate('document.querySelectorAll("#diagnostics-view .command-field-missing").length'),0,'initial form is neutral');
 assert(await evaluate('document.querySelector("#diagnostic-setup").hidden'));
 assert(!await evaluate('document.querySelector("#diagnostic-observability").parentElement.textContent.includes("Fields follow the selected checks")'),'old explanatory banner removed');
 assert(!await evaluate('document.querySelector("#run").disabled'),'Run can validate incomplete inputs');
 await shot('diagnostics-neutral');
 await click('#run');
 assert.equal(await evaluate('diagnosticSubmissionRequests'),0);
 assert.equal(await evaluate(`document.querySelector(${JSON.stringify(node)}).getAttribute('aria-invalid')`),'true');
 assert.equal(await evaluate('document.querySelectorAll("#diagnostic-components [aria-invalid=true]").length'),0,'filled sources remain neutral');
 assert(await evaluate('document.querySelector("#diagnostic-setup").closest(".workflow-launch")!==null'),'feedback is at Run, not Check parameters');
 assert(!await evaluate('document.querySelector("#diagnostic-setup").textContent.includes("Required:")'));
 await shot('diagnostics-node-required-after-run');
 await set(node,'fixture-node');
 assert.equal(await evaluate(`document.querySelector(${JSON.stringify(node)}).getAttribute('aria-invalid')`),null);
 assert(await evaluate('document.querySelector("#diagnostic-setup").hidden'));
 assert(await evaluate('validateDiagnosticInputs()'),'complete scenario inputs');
 // Trace-only scenarios must not require parameters of unavailable Metrics checks.
 await set(prom,'');await set(node,'');
 assert.equal(await evaluate(`document.querySelector(${JSON.stringify(node)}).getAttribute('aria-required')`),null);
 assert.equal(await evaluate(`document.querySelector(${JSON.stringify(node)}).getAttribute('aria-invalid')`),null);
 assert(await evaluate('validateDiagnosticInputs()'),'Trace-only scenario is allowed');
 await shot('diagnostics-trace-only');
 // No source: show the alternative-source requirement at the API inputs.
 await set(trace,'');await click('#run');
 assert.equal(await evaluate('diagnosticSubmissionRequests'),0);
 for(const selector of [prom,trace])assert.equal(await evaluate(`document.querySelector(${JSON.stringify(selector)}).getAttribute('aria-invalid')`),'true');
 assert.equal(await evaluate(`document.querySelector(${JSON.stringify(node)}).getAttribute('aria-invalid')`),null,'no irrelevant Node warning');
 await shot('diagnostics-missing-sources');
 await set(prom,addresses.metrics);await set(trace,addresses.trace);
 await set('#mode','metrics');await set('#atomic-check','writer-throughput');
 assert.equal(await evaluate('document.querySelectorAll("#diagnostics-view .command-field-missing").length'),0,'new check starts neutral');
 await set('#period','custom');await set('#from','');await set('#to','');
 assert.equal(await evaluate('document.querySelector("#from").getAttribute("aria-invalid")'),null,'time inputs start neutral');
 await click('#run');
 for(const id of ['from','to'])assert.equal(await evaluate(`document.querySelector('#${id}').getAttribute('aria-invalid')`),'true');
 assert.equal(await evaluate('diagnosticSubmissionRequests'),0);
 await shot('diagnostics-incident-time-validation');
 await evaluate(`(()=>{document.querySelector('#from').value=inputTime(new Date(Date.now()-900000));document.querySelector('#to').value=inputTime(new Date(Date.now()-60000));document.querySelector('#from').dispatchEvent(new Event('change',{bubbles:true}));document.querySelector('#to').dispatchEvent(new Event('change',{bubbles:true}));})()`);
 assert(await evaluate('validateDiagnosticInputs()'),'valid fixed incident window');
 // Manual source population fields and invalid JSON are validated next to their inputs.
 await set(prom,'http://127.0.0.1:18090');
 await evaluate(`(()=>{initialBindings().client.deployment='';initialBindings().client.metric_labels={};renderDiagnosticBindings(true);render();})()`);
 await click('#run');
 assert.equal(await evaluate('document.querySelector("#diagnostic-observability [data-binding-key=deployment]").getAttribute("aria-invalid")'),'true');
 assert.equal(await evaluate('document.querySelector("#diagnostic-observability [data-binding-labels]").getAttribute("aria-invalid")'),'true');
 await shot('diagnostics-manual-source-validation');
 assert.equal(await evaluate('document.querySelector("#milvus")'),null);
 assert(await evaluate('state.catalog.checks.every(c=>!c.name.includes("milvus"))'));
 await set(prom,addresses.metrics);await set('#mode','scenario');await set('#scenario','slow-writes');
 await set(node,'');
 const help=await evaluate(`(()=>{const input=document.querySelector(${JSON.stringify(node)}),help=input.closest('label').querySelector('.parameter-help');help.scrollIntoView({block:'center',behavior:'instant'});return !!help;})()`);assert(help);
 await pause(80);await evaluate(`document.querySelector(${JSON.stringify(node)}).closest('label').querySelector('.parameter-help').focus({preventScroll:true})`);
 await wait('!!document.querySelector("#workflow-help-popup")&&!document.querySelector("#workflow-help-popup").hidden','Node ID example');
 assert(await evaluate('document.querySelector("#workflow-help-popup").textContent.includes("woodpecker-0")'));
 await shot('diagnostics-node-example');await evaluate('closeWorkflowHelp()');
 // Shared decorator still renders the same neutral Required label in Command Explorer.
 await evaluate("showCommand('node healthz')");
 assert.equal(await evaluate('document.querySelectorAll("#command-config .command-field-missing").length'),0);
 const colors=await evaluate(`({diagnostic:getComputedStyle(document.querySelector('#diagnostic-observability [data-binding-key=node_id]').closest('label').querySelector('.required-mark')).color,command:getComputedStyle(document.querySelector('[data-shared-key=node]').closest('label').querySelector('.required-mark')).color})`);
 assert.equal(colors.diagnostic,colors.command);
 await click('.workspace-tabs [data-tab="diagnostics"]');
 for(const width of [1440,900,390]){await call('Emulation.setDeviceMetricsOverride',{width,height:1050,deviceScaleFactor:1,mobile:false});await evaluate('window.scrollTo(0,0)');await pause(80);assert(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'overflow '+width);await shot('diagnostics-'+width);}
 assert.equal(await evaluate('diagnosticSubmissionRequests'),0);
 console.log(JSON.stringify({passed:true,field_validation:true,scenario_and_atomic:true,trace_only:true,source_and_time_fields:true,shared_style:true,viewports:[1440,900,390],diagnostic_queries:0,model_calls:0}));
 assert.equal(errors.length,0,JSON.stringify(errors));
}finally{ws?.close();chrome.kill();await chromeClosed;await rm(profile,{recursive:true,force:true});}
