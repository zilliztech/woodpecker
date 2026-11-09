// Browser acceptance for the real local WebUI/API. No npm dependencies.
import {spawn} from 'node:child_process';
import {mkdtemp,readFile,writeFile,rm,readdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import assert from 'node:assert/strict';
import {join} from 'node:path';

const [url,output,variant]=process.argv.slice(2);
assert(url&&output&&variant,'Usage: workspace_setup_browser.mjs URL OUTPUT VARIANT');
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
 await call('Page.navigate',{url});

 await wait('typeof state!=="undefined"&&state.info!==null&&state.catalog!==null','workspace ready');

 assert.equal(await evaluate('state.tab'),'overview');
 assert.equal(await evaluate('document.querySelectorAll("[data-module]").length'),3,variant+': '+JSON.stringify(errors)+' '+await evaluate('document.body.innerText.slice(-2000)'));
 await shot(variant+'-overview');await click('[data-tab="diagnostics"]');
 const visible=sel=>evaluate(`document.querySelector(${JSON.stringify(sel)}).getClientRects().length>0`);
 assert.deepEqual(await evaluate('Array.from(document.querySelectorAll(".workspace-tabs [data-tab]")).map(b=>b.dataset.tab)'),['overview','diagnostics','runbooks','commands','settings']);
 assert(!await visible('#wp-context'));assert(!await visible('#meta-target'));assert(!await evaluate('!!document.querySelector("#instance")'));
 if(variant==='single'||variant==='no-admin'||variant==='history'){
  assert(await visible('#diagnostic-components'));
  assert(!await evaluate('document.querySelector("#run").disabled'));
  await set('#mode','metrics');await set('#atomic-check','writer-throughput');await click('#run');await wait('!!state.report','telemetry without a service context',30000);
  assert.equal(await evaluate('state.report.status'),'completed');
 }else{
  assert(await evaluate('document.querySelector("#run").disabled'));
  assert(await visible('#diagnostic-setup'));
  await click('#catalog');await wait('!!document.querySelector("[data-check]")','offline catalog');await click('#close');
 }
 await shot(variant+'-diagnostics');
 await click('.workspace-tabs [data-tab="commands"]');await click('.visual-shortcuts [data-tab="cluster"]');
 if(variant==='single'||variant==='invalid-diag'||variant==='history'){
  assert(!await evaluate('document.querySelector("#refresh-cluster").disabled'));
  assert(!await visible('#wp-context'));
  assert(await visible('#wp-context-name'));
  await click('#refresh-cluster');await wait('document.querySelector("#cluster-result").innerText.includes("node-1")','Admin is independent',30000);
 }else assert(await evaluate('document.querySelector("#refresh-cluster").disabled'));
 await shot(variant+'-cluster');
 await click('.workspace-tabs [data-tab="commands"]');await click('.visual-shortcuts [data-tab="metadata"]');
 if(variant==='single'||variant==='no-admin'||variant==='history'||variant==='invalid-diag'){
  if(variant!=='history'){assert(!await visible('#meta-target'));}else{assert(await visible('#meta-target'));}
  assert.equal(await evaluate('state.info.session.metadata||null'),null);
  if(variant!=='history')
  assert(await visible('#meta-target-name'));
  if(variant==='history'){
   assert.equal(await evaluate('state.info.metadata_authorities.find(c=>c.id===document.querySelector("#meta-target").value).origin'),'history');
   assert.equal(await evaluate('document.querySelector("#meta-log").value'),'remembered-log');
   assert(await evaluate('document.querySelector("[data-meta=M03]").disabled'));
  }
  await click('#connect-meta');
 }else{
  assert(await evaluate('document.querySelector("#manual-meta").open'));
  await set('#meta-etcd','http://client-etcd.invalid:2379');await set('#meta-prefix','client/woodpecker');await click('#connect-manual');
 }
 await wait('!!state.info.session.metadata','explicit metadata without Admin or telemetry');
 await evaluate('document.querySelector("#meta-log").value="";metadataControls()');
 assert(!await evaluate('document.querySelector("[data-meta=M03]").disabled'));
 assert(await evaluate('document.querySelector("[data-meta=M01]").disabled'));
 await shot(variant+'-metadata');
 if(variant==='single'){
  await click('#manual-meta summary');
  await set('#meta-etcd','http://history-etcd:2379');await set('#meta-prefix','history-client/woodpecker');await set('#meta-log','remembered-log');await click('#connect-manual');
  await wait('state.info.session.metadata?.prefix==="history-client/woodpecker"','save manual connection');
  assert(await evaluate('state.info.metadata_history.some(c=>c.prefix==="history-client/woodpecker")'));
 }
 if(variant==='history'){
  const remembered=await evaluate('state.info.metadata_history.find(c=>c.prefix==="history-client/woodpecker").id');
  await click('#manage-meta-history');await wait('!!document.querySelector("[data-forget-meta]")','history manager');
  await shot('history-restored');await click(`[data-forget-meta="${remembered}"]`);
  await wait(`!state.info.metadata_history.some(c=>c.id===${JSON.stringify(remembered)})`,'forget local history');
  assert(await evaluate('!!state.info.session.metadata'),'forget does not disconnect');await click('#close');
 }

 if(variant==='no-admin'){
  await click('.workspace-tabs [data-tab="commands"]');await set('#command-search','k8s status');
  await click('[data-command="k8s status"]');assert(!await evaluate('document.querySelector("#execute-command").disabled'));
  await click('#execute-command');await wait('document.querySelector("#command-exit").textContent.includes("exit 0")','Kubernetes preview without a WP context',30000);
  assert(await evaluate('document.querySelector("#command-result").innerText.includes("kubectl")'));
 }
 await click('[data-tab="settings"]');assert(await visible('#settings-view'));
 assert.equal(errors.length,0,JSON.stringify(errors));
 console.log(JSON.stringify({variant,passed:true,js_errors:errors}));
}finally{ws?.close();chrome.kill();await chromeClosed;await rm(profile,{recursive:true,force:true});}
