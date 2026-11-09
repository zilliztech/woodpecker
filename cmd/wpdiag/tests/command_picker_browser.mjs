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







 await evaluate(`(()=>{window.pickerExecutionRequests=0;const original=api;api=async(path,body)=>{if(path==='/api/ops/commands'){pickerExecutionRequests++;throw Error('Command selection must not execute');}return original(path,body);};})()`);
 await click('.workspace-tabs [data-tab="commands"]');
 assert.equal(await evaluate('document.querySelectorAll("#command-search,#command-chooser,#command-category-select").length'),0,'old directory UI is replaced');
 const groups=await evaluate('[...document.querySelector("#command-path-level-0").options].map(o=>o.value).filter(Boolean)');
 assert.deepEqual(groups,await evaluate('[...new Set(state.commands.commands.map(c=>c.name.split(" ")[0]))].sort()'));
 await shot('command-picker-default');
 await set('#command-path-level-0','meta');
 assert.equal(await evaluate('state.commandName'),'','a group is not an executable operation');
 assert(await evaluate('document.querySelector("#command-config").hidden&&document.querySelector("#command-console").hidden'));
 assert(!await evaluate('document.querySelector("#command-selection-prompt").hidden'));
 assert.equal(await evaluate('document.activeElement.id'),'command-path-level-1','focus moves to the next part');
 await set('#command-path-level-1','segment');
 assert.equal(await evaluate('document.querySelectorAll("[data-command-level]").length'),3);
 assert.equal(await evaluate('state.commandName'),'');
 await shot('command-picker-meta-pending');
 await set('#command-path-level-2','show');
 assert.equal(await evaluate('state.commandName'),'meta segment show');
 assert.equal(await evaluate('document.querySelector("#selected-command-label").textContent'),'wp meta segment show');
 assert(!await evaluate('document.querySelector("#command-config").hidden'));
 await shot('command-picker-meta-show');
 await set('#command-path-level-0','ops');await set('#command-path-level-1','list');
 await set('[data-shared-key=node]','fixture-node');await set('[data-shared-key=logID]','42');await set('[data-shared-key=segmentID]','7');
 await evaluate(`document.querySelector('.console-runtime').open=true;document.querySelector('.command-options').open=true;document.querySelector('.console-preview').open=true`);
 await set('#console-timeout','12s');
 await set('#command-path-level-0','logstore');await set('#command-path-level-1','segment-show');
 for(const [key,value]of [['node','fixture-node'],['logID','42'],['segmentID','7']])assert.equal(await evaluate(`document.querySelector('[data-shared-key="${key}"]').value`),value,'shared '+key);
 assert.equal(await evaluate('document.querySelector("#console-timeout").value'),'12s');
 assert(await evaluate('document.querySelector(".console-runtime").open&&document.querySelector(".command-options").open&&document.querySelector(".console-preview").open'),'panel preferences survive hierarchy navigation');
 await shot('command-picker-shared-parameters');
 await set('#command-path-level-0','node');await set('#command-path-level-1','cancel-decommission');
 await evaluate(`(()=>{const i=document.querySelector('[data-command-flag=yes]');i.checked=true;i.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 await set('#command-path-level-0','ops');await set('#command-path-level-1','list');await set('#command-path-level-0','node');await set('#command-path-level-1','cancel-decommission');
 assert(!await evaluate('document.querySelector("[data-command-flag=yes]").checked'),'acceptance is not copied between commands');
 await set('#command-path-level-0','version');assert.equal(await evaluate('state.commandName'),'version');
 assert.equal(await evaluate('document.querySelectorAll("[data-command-level]").length'),1,'standalone commands need one dropdown');
 assert(!await evaluate('document.querySelector("#execute-command").disabled'));
 await shot('command-picker-version');
 await set('#command-path-level-0','meta');await set('#command-path-level-1','segment');await set('#command-path-level-2','show');
 await set('#command-path-level-0','ops');assert.equal(await evaluate('document.querySelectorAll("[data-command-level]").length'),2,'changing parents removes stale levels');
 await set('#command-path-level-1','list');await set('#command-path-level-1','');
 assert.equal(await evaluate('state.commandName'),'','clearing a selection hides the stale editor');
 assert(await evaluate('document.querySelector("#command-config").hidden'));
 // Every catalog command is reachable using only the visible hierarchy.
 const count=await evaluate(`(()=>{let count=0;for(const c of state.commands.commands){const parts=c.name.split(' ');for(let i=0;i<parts.length;i++){const select=document.querySelector('#command-path-level-'+i);if(!select||![...select.options].some(o=>o.value===parts[i]))throw Error('Unreachable '+c.name+' part '+i);select.value=parts[i];select.dispatchEvent(new Event('change',{bubbles:true}));}if(state.commandName!==c.name||document.querySelector('#command-config').hidden)throw Error('Wrong selection '+c.name);count++;}return count;})()`);
 assert.equal(count,81);
 await evaluate("showCommand('meta quorum show')");
 assert.deepEqual(await evaluate('[...document.querySelectorAll("[data-command-level]")].map(s=>s.value)'),['meta','quorum','show'],'direct links synchronize the hierarchy');
 await click('.workspace-tabs [data-tab="runbooks"]');
 assert(await evaluate('!!document.querySelector("#operation-config-mount #command-config")&&!document.querySelector("#command-config").hidden'),'Operations still mounts the shared editor');
 await click('.workspace-tabs [data-tab="commands"]');
 assert(await evaluate('!!document.querySelector("#command-controls #command-config")'),'Explorer restores the editor');
 await set('#command-path-level-0','meta');await set('#command-path-level-1','segment');await set('#command-path-level-2','show');
 for(const width of [1440,900,390]){
  await call('Emulation.setDeviceMetricsOverride',{width,height:1050,deviceScaleFactor:1,mobile:false});
  await evaluate('document.querySelector("#command-controls").scrollTop=0;window.scrollTo(0,0)');await pause(80);
  assert(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'overflow '+width);
  await shot('command-picker-'+width);
 }
 assert.equal(await evaluate('pickerExecutionRequests'),0);
 console.log(JSON.stringify({passed:true,groups:groups.length,commands:count,max_levels:3,shared_values:true,panel_preferences:true,direct_links:true,operations_navigation:true,viewports:[1440,900,390],cluster_operations:0,model_calls:0}));
 assert.equal(errors.length,0,JSON.stringify(errors));
}finally{ws?.close();chrome.kill();await chromeClosed;await rm(profile,{recursive:true,force:true});}
