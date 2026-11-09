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






 await evaluate(`(()=>{window.parameterExecutionRequests=0;const original=api;api=async(path,body)=>{if(path==='/api/ops/commands'){parameterExecutionRequests++;throw Error('Parameter review must not execute commands');}return original(path,body);};})()`);
 const required=sel=>evaluate(`document.querySelector(${JSON.stringify(sel)}).getAttribute('aria-required')==='true'`);
 const focusExample=async(sel,expected)=>{await evaluate(`(()=>{const input=document.querySelector(${JSON.stringify(sel)}),help=input.closest('label').querySelector('.parameter-help');if(!help)throw Error('No example for '+${JSON.stringify(sel)});for(let p=help.parentElement;p;p=p.parentElement)if(p.tagName==='DETAILS')p.open=true;help.scrollIntoView({block:'center',behavior:'instant'});})()`);await pause(80);await evaluate(`(()=>{const help=document.querySelector(${JSON.stringify(sel)}).closest('label').querySelector('.parameter-help');if(document.activeElement===help)help.blur();help.focus({preventScroll:true});})()`);await wait(`document.querySelector('#workflow-help-popup')&&!document.querySelector('#workflow-help-popup').hidden&&document.querySelector('#workflow-help-popup').textContent.includes(${JSON.stringify(expected)})`,'parameter example');};

 await click('.workspace-tabs [data-tab="runbooks"]');
 await click('#operation-step-editor-0 [data-operation-command="node healthz"]');
 assert(await required('[data-shared-key=node]'));
 await focusExample('[data-shared-key=node]','woodpecker-0');
 assert(await evaluate('document.querySelector("#workflow-help-popup").textContent.includes("EXAMPLE")'));
 assert(!await evaluate('document.querySelector("#workflow-help-popup").textContent.includes("NEXT STEP")'));
 await shot('operations-node-example');await evaluate('closeWorkflowHelp()');
 assert.equal(await evaluate('document.querySelectorAll("#command-config .command-field-missing").length'),0,'missing inputs are neutral before Run');
 const neutral=await evaluate('getComputedStyle(document.querySelector("[data-shared-key=node]").closest("label").querySelector(".required-mark")).color');
 await click('#execute-command');
 assert.equal(await evaluate('parameterExecutionRequests'),0,'incomplete inputs never reach the backend');
 assert.equal(await evaluate('document.querySelector("[data-shared-key=node]").getAttribute("aria-invalid")'),'true');
 assert.notEqual(await evaluate('getComputedStyle(document.querySelector("[data-shared-key=node]").closest("label").querySelector(".required-mark")).color'),neutral,'warning only after Run');
 assert.equal(await evaluate('document.querySelector("[data-shared-context]").getAttribute("aria-invalid")'),null,'filled required fields stay neutral');
 await shot('operations-missing-after-run');
 await set('[data-shared-key=node]','fixture-node');
 assert.equal(await evaluate('document.querySelectorAll("#command-config .command-field-missing").length'),0,'warnings clear on correction');
 assert(await evaluate('document.querySelector("#console-form-error").hidden'));

 // Hover uses the same popup and never changes a field value.
 const bounds=await evaluate(`(()=>{const input=document.querySelector('[data-shared-key=node]'),help=input.closest('label').querySelector('.parameter-help'),r=help.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2,value:input.value};})()`);
 await call('Input.dispatchMouseEvent',{type:'mouseMoved',x:bounds.x,y:bounds.y});
 await wait('!document.querySelector("#workflow-help-popup").hidden','hover opens example');
 assert.equal(await evaluate('document.querySelector("[data-shared-key=node]").value'),bounds.value);
 await call('Input.dispatchKeyEvent',{type:'keyDown',key:'Escape',code:'Escape',windowsVirtualKeyCode:27});await call('Input.dispatchKeyEvent',{type:'keyUp',key:'Escape',code:'Escape',windowsVirtualKeyCode:27});
 assert(await evaluate('document.querySelector("#workflow-help-popup").hidden'));
 await evaluate("showCommand('ops show')");
 await set('[data-shared-key=node]','fixture-node');await set('[data-command-flag="op-id"]','');
 assert(await required('[data-command-flag="op-id"]'),'RunE-required Operation ID is labelled required');
 assert(await evaluate('commandValidationMissing.length>0'));
 assert(!await evaluate('document.querySelector("#execute-command").disabled'),'Run remains available to validate the inputs');
 assert.equal(await evaluate('document.querySelectorAll("#command-config .command-field-missing").length'),0,'switching commands starts neutral');
 await click('#execute-command');
 assert.equal(await evaluate('document.querySelector("[data-command-flag=op-id]").getAttribute("aria-invalid")'),'true');
 assert.equal(await evaluate('parameterExecutionRequests'),0);
 await focusExample('[data-command-flag="op-id"]','append-op-123');
 await shot('explorer-operation-id');await evaluate('closeWorkflowHelp()');
 await set('[data-command-flag="op-id"]','append-op-123');
 assert(!await evaluate('document.querySelector("#execute-command").disabled'));
 await evaluate("showCommand('ops list')");
 assert(await required('[data-shared-key=node]'));
 assert(!await required('[data-shared-key=logID]'),'optional log filter stays optional');
 assert.equal(await evaluate('document.querySelector("[data-shared-key=logID]").closest("label").querySelectorAll(".parameter-help").length'),0);
 const palette=await evaluate(`(()=>{const a=document.querySelector('[data-shared-key=node]').closest('label').querySelector('.required-mark'),b=document.querySelector('[data-shared-key=logID]').closest('label').querySelector('.optional-mark');return {required:getComputedStyle(a).color,optional:getComputedStyle(b).color,bg:getComputedStyle(a).backgroundColor};})()`);
 assert.notEqual(palette.required,palette.optional);assert.equal(palette.bg,'rgba(0, 0, 0, 0)','default required label has no warning background');
 await evaluate("showCommand('logstore segment-show')");
 assert(await required('[data-shared-key=logID]'),'same field becomes required for another command');
 assert(await required('[data-shared-key=segmentID]'));
 await focusExample('[data-shared-key=logID]','42');await evaluate('closeWorkflowHelp()');
 await evaluate("showCommand('meta log show')");
 assert(await required('[data-binding-key="metadata.etcd"]'));assert(await required('[data-binding-key="metadata.prefix"]'));
 await focusExample('[data-binding-key="metadata.prefix"]','wp-prod/woodpecker');
 await shot('explorer-client-meta-example');await evaluate('closeWorkflowHelp()');
 await evaluate("showCommand('segment probe')");
 assert(await required('[data-shared-key=bucket]'));assert(await required('[data-shared-key=rootPath]'));
 await focusExample('[data-shared-key=rootPath]','tenant/wal');await evaluate('closeWorkflowHelp()');
 await evaluate("showCommand('log skip-range add')");
 for(const name of ['from-entry','to-entry','reason'])assert(await required('[data-command-flag="'+name+'"]'),name+' is required');
 await focusExample('[data-command-flag="from-entry"]','50');
 await shot('explorer-skip-range-example');await evaluate('closeWorkflowHelp()');
 // Apply-only inputs are optional in preview, and required only when apply is enabled.
 await evaluate("showCommand('meta segment patch')");
 assert(await required('[data-command-flag="patch-file"]'));
 assert(!await required('[data-command-flag="expected-revision"]'));
 await evaluate(`(()=>{const input=document.querySelector('[data-command-flag=apply]');input.checked=true;input.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 for(const name of ['expected-revision','reason','backup','offline'])assert(await required('[data-command-flag="'+name+'"]'),'apply requires '+name);
 await focusExample('[data-command-flag="expected-revision"]','174');
 assert(await evaluate('document.querySelector("#workflow-help-popup").textContent.includes("TARGET RECORD")'));
 await shot('explorer-apply-requirements');await evaluate('closeWorkflowHelp()');
 await evaluate(`(()=>{const input=document.querySelector('[data-command-flag=apply]');input.checked=false;input.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 for(const name of ['expected-revision','reason','backup','offline'])assert(!await required('[data-command-flag="'+name+'"]'),'preview does not require '+name);
 await evaluate("showCommand('metrics report')");
 assert(await required('[data-command-flag=scenario]'));
 await evaluate(`(()=>{const input=document.querySelector('[data-command-flag=list]');input.checked=true;input.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 assert(!await required('[data-command-flag=scenario]'),'listing scenarios needs no scenario selection');
 await evaluate("showCommand('k8s scale')");assert(await required('[data-command-flag=replicas]'));
 await focusExample('[data-command-flag=replicas]','3');await evaluate('closeWorkflowHelp()');
 await evaluate("showCommand('logstore fence-quorum')");assert(await required('[data-command-flag=reason]'));
 await evaluate("showCommand('etcd cat')");assert(await required('[data-command-argument="0"]'));await focusExample('[data-command-argument="0"]','/example/key');await evaluate('closeWorkflowHelp()');
 await evaluate("showCommand('node healthz')");
 for(const width of [1440,900,390]){
  await call('Emulation.setDeviceMetricsOverride',{width,height:1050,deviceScaleFactor:1,mobile:false});
  await focusExample('[data-shared-key=node]','woodpecker-0');
  const r=await evaluate(`(()=>{const r=document.querySelector('#workflow-help-popup').getBoundingClientRect();return {left:r.left,right:r.right,top:r.top,bottom:r.bottom};})()`);
  assert(r.left>=10&&r.right<=width-10&&r.top>=10&&r.bottom<=1040,'tooltip stays in viewport '+width+' '+JSON.stringify(r));
  assert(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'overflow '+width);
  await shot('parameter-help-'+width);await evaluate('closeWorkflowHelp()');
 }
 assert.equal(await evaluate('parameterExecutionRequests'),0);
 console.log(JSON.stringify({passed:true,operations:true,command_explorer:true,hover_focus_escape:true,required_optional_switching:true,conditional_apply:true,validation_on_submit:true,viewports:[1440,900,390],cluster_operations:0,model_calls:0}));
 assert.equal(errors.length,0,JSON.stringify(errors));
}finally{ws?.close();chrome.kill();await chromeClosed;await rm(profile,{recursive:true,force:true});}
