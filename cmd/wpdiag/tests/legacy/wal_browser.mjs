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
 const set=async(sel,value)=>evaluate(`(()=>{const e=document.querySelector(${JSON.stringify(sel)});e.value=${JSON.stringify(value)};e.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 await call('Page.enable');await call('Runtime.enable');await call('Network.enable');
 await call('Browser.setDownloadBehavior',{behavior:'allow',downloadPath:output});
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});
 await call('Page.navigate',{url});
 await wait('typeof state!=="undefined"&&state.catalog!==null','catalog loaded');
 assert.equal(await evaluate('document.querySelectorAll("[data-problem]").length'),7);
 assert.equal(await evaluate('state.catalog.catalog.evidence.length'),52);
 assert(!await evaluate('document.body.innerText.includes("SYNTHETIC")'));
 await click('#run');await wait('state.busy&&state.job!==null','run starts');
 assert(await evaluate('document.querySelector("#run").textContent.includes("Stop")'));
 await wait('!!state.report','run completes',40000);
 assert(await evaluate('state.report.evidence.some(e=>e.status==="abnormal")'),'fixture must yield real backend findings: '+JSON.stringify(await evaluate('state.report')));
 assert.equal(await evaluate('state.filter'),'abnormal');
 assert(await evaluate('state.report.observations.length>0'));
 await shot('01-write-findings');
 const modelText='## Finding\n**Latency is elevated.** [W01]\n\n## Evidence\n- P99 exceeds the reference. [W01]\n- Unrecognized reference stays text. [ZZ99]\n- <img src=x onerror=window.__agentInjected=true> [external](https://example.invalid)\n\n## Next steps\n1. Inspect the corresponding entry.';
 await evaluate(`state.analysis={status:'completed',text:${JSON.stringify(modelText)}};render()`);
 assert.equal(await evaluate('document.querySelectorAll(".agent-answer h3").length'),3);
 assert(await evaluate('document.querySelector(".agent-answer strong").textContent.includes("elevated")'));
 assert.equal(await evaluate('document.querySelectorAll(".agent-answer img,.agent-answer script,.agent-answer a").length'),0);
 assert(!await evaluate('!!window.__agentInjected'));
 assert(await evaluate('Array.from(document.querySelectorAll(".agent-answer [data-evidence]")).every(e=>e.dataset.evidence==="W01")'));
 await click('.agent-answer [data-evidence="W01"]');await wait('!!document.querySelector("#inspection-curve")','Agent reference opens retained evidence');await click('#close');
 await evaluate('state.analysis=null;render()');
 checks.push('readable AI sections, known evidence links, escaped model HTML and inert external URLs');

 const runID=await evaluate('state.job.id');const exact=await(await fetch(url+'/api/runs/'+runID+'/report')).text();
 await click('[data-action="export"]');
 let saved;for(let i=0;i<100;i++){saved=(await readdir(output)).find(n=>n.endsWith(runID+'.json'));if(saved)break;await pause(100);}
 assert(saved,'No JSON download');assert.equal(await readFile(join(output,saved),'utf8'),exact,'Export must preserve the stored report bytes');
 await click('[data-evidence="W01"]');
 await wait('!!document.querySelector("#inspection-curve")','recorded evidence inspector');
 for(const heading of ['Metric / source','Calculation','Result','Abnormal data','Time series']) assert(await evaluate(`document.querySelector('#drawer-body').innerText.includes(${JSON.stringify(heading)})`));
 for(const removed of ['Applied policy','Scope & prerequisites','Implementation','Source references']) assert(!await evaluate(`document.querySelector('#drawer-body').innerText.includes(${JSON.stringify(removed)})`));
 assert(await evaluate('document.querySelectorAll(".chart-point").length>0'),'actual P99 points');
 assert(await evaluate('document.querySelectorAll(".chart-point.matched").length>0'),'qualified sustained windows highlighted');
 await shot('02-concise-evidence');
 await call('Emulation.setDeviceMetricsOverride',{width:390,height:1050,deviceScaleFactor:1,mobile:false});
 assert(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'inspector overflow');await shot('02-concise-evidence-mobile');
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1050,deviceScaleFactor:1,mobile:false});
 await evaluate('document.querySelector(".inspection-source").open=true');
 assert(await evaluate('document.querySelectorAll("[data-observation]").length>0'));
 await click('[data-observation]');await wait('document.querySelector("#drawer-body").innerText.includes("prometheus_matrix")','original metric observation');
 assert(await evaluate('document.querySelector("#drawer-body").innerText.includes("Expression")||document.querySelector("#drawer-body").innerText.includes("expression")'));
 await shot('02-original-evidence');await click('#close');
 await click('[data-action="packet"]');await wait('document.querySelector("#drawer-body").innerText.includes("report_sha256")','agent packet');
 assert(await evaluate('document.querySelector("#drawer-body").innerText.includes("analyze_supplied_findings_only")'));
 await click('#close');
 assert(await evaluate('document.querySelector("[data-action=analyze]").disabled'),'Agent off by default');
 await click('#history');await wait('!!document.querySelector("[data-history]")','retained run');await click('[data-history]');
 assert(await evaluate('state.report!==null'));
 checks.push('real asynchronous checks, findings filter, exact source observations, Agent packet, disabled Agent, retained history');
 await click('[data-filter="all"]');await click('[data-evidence="W05"]');
 await wait('!!document.querySelector("#run-single-check")','single-check action');
 await click('#run-single-check');await wait('!!state.report&&state.report.check==="W05"','single check completes',30000);
 assert.deepEqual(await evaluate('state.report.evidence.map(e=>e.id).sort()'),['W04','W05']);
 assert.equal(await evaluate('document.querySelectorAll(".check-row").length'),2);
 assert(await evaluate('document.querySelector("#run").textContent.includes("Run scenario")'));
 await click('[data-evidence="W05"]');await wait('!!document.querySelector("#inspection-curve")','single check keeps measured curves');await click('#close');
 checks.push('individual check action uses shared backend dependencies and stored curve inspection');
 await click('[data-problem="read_stuck"]');await click('#run');await wait('state.busy&&state.job!==null','cancellable run starts');await click('#run');await wait('!!state.report','partial result retained');
 assert.equal(await evaluate('state.report.complete'),false);
 checks.push('cancellation preserves partial evidence and never claims completion');
 for(const id of ['read_slow','write_stuck','open_reader','open_writer']){await click('[data-problem="'+id+'"]');await click('#run');await wait('!!state.report','check '+id,30000);assert(await evaluate('state.report.evidence.length>0'));}
 await click('[data-problem="metadata"]');
 assert(await evaluate('document.querySelector("#metadata-time").hidden===false'));
 await click('#run');await wait('!!state.report','metadata missing authority');
 assert.equal(await evaluate('state.report.time_range.mode'),'snapshot');
 assert(await evaluate('state.report.evidence.every(e=>e.status==="unknown")'));
 await shot('03-metadata-boundary');
 await click('#catalog');assert.equal(await evaluate('document.querySelectorAll(".catalog-entry").length'),52);
 await evaluate('document.querySelector("#catalog-search").value="M04";document.querySelector("#catalog-search").dispatchEvent(new Event("input",{bubbles:true}))');
 assert.equal(await evaluate('Array.from(document.querySelectorAll(".catalog-entry")).filter(e=>!e.hidden).length'),1);
 await click('#close');
 for(const width of [1440,900,390]){await call('Emulation.setDeviceMetricsOverride',{width,height:1050,deviceScaleFactor:1,mobile:false});await pause(150);assert(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'overflow '+width);await shot('viewport-'+width);}
 checks.push('seven problem entries, current metadata boundary, searchable catalog and three viewport sizes');
 await call('Page.navigate',{url:new URL('/advanced',url).href});
 await wait('typeof state!=="undefined"&&state.catalog!==null','Advanced catalog');
 assert.equal(await evaluate('document.querySelectorAll("#history option").length'),1,'Advanced must not interpret WAL reports as old integrated reports');
 checks.push('Advanced history excludes incompatible WAL and analysis report kinds');
 assert.equal(errors.length,0,JSON.stringify(errors));
 assert(!await evaluate('/[\\u4e00-\\u9fff]/.test(document.body.innerText)'));
 console.log(JSON.stringify({passed:checks,js_errors:errors},null,2));
}finally{ws?.close();chrome.kill();await chromeClosed;await rm(profile,{recursive:true,force:true});}
