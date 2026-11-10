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
 await call('Page.navigate',{url});

 await wait('typeof state!=="undefined"&&state.info!==null&&state.catalog!==null','workspace ready');

 await wait('state.info.agent.code!=="agent_status_pending"','background Agent login check');
 assert(await evaluate('state.info.agent.ready'),'fake Agent ready');
 assert(!await evaluate('document.querySelector("[data-diagnostic-followup]")'));
 await click('.workspace-tabs [data-tab="diagnostics"]');await set('#scenario','slow-writes');await set('#period','5');await set('[data-binding-role="server"][data-binding-key="node_id"]','node-1');
 await click('#run');await wait('!!state.report','diagnostic run',40000);
 const runID=await evaluate('state.job.id'),exact=await evaluate('fetch("/api/runs/"+state.job.id+"/report").then(r=>r.text())');
 const from=await evaluate('state.report.request.from');
 assert(!await evaluate('document.querySelector("#analyze").disabled'));
 await click('#analyze');await wait('!!state.analysis&&state.analysis.status==="completed"','first analysis',30000);
 const firstID=await evaluate('state.analysisID');
 assert(await evaluate('state.analysis.text.includes("Initial assessment")'));
 assert.equal(await evaluate('runAnalyses(state.job.id).length'),1);
 await set('#analysis-question','What would confirm the suspected impact?');await click('#analyze');
 await wait('!!state.analysis&&state.analysisID!=='+JSON.stringify(firstID),'follow-up analysis',30000);
 const secondID=await evaluate('state.analysisID');
 assert(await evaluate('state.analysis.text.includes("Refined assessment")'));
 assert.equal(await evaluate('state.analysis.previous_analysis.id'),firstID);
 assert.equal(await evaluate('state.analysis.question'),'What would confirm the suspected impact?');
 assert.equal(await evaluate('runAnalyses(state.job.id).length'),2);
 await shot('10-agent-follow-up');
 await click('#history');await wait('document.querySelector("#diagnostic-history").open','open run history');
 assert(await evaluate('document.querySelector("#run-history-list").innerText.includes("2 analysis reports")'));
 await shot('11-analysis-history');
 await evaluate('document.querySelector("[data-run-record]").open=true');
 await click('[data-run-analyses]');await wait('!state.restoring&&state.analysisID==='+JSON.stringify(secondID),'view analyses');
 await set('#analysis-version',firstID);await wait('state.analysisID==='+JSON.stringify(firstID),'view original answer');
 assert(await evaluate('state.analysis.text.includes("Initial assessment")'));
 assert(!await evaluate('!!state.analysis.previous_analysis'));
 await click('#download-analysis');let saved;
 for(let i=0;i<100;i++){saved=(await readdir(output)).find(n=>n.endsWith(firstID+'.json'));if(saved)break;await pause(100);}
 assert(saved);assert.equal(JSON.parse(await readFile(join(output,saved),'utf8')).text,await evaluate('state.analysis.text'));
 assert.equal(await evaluate('fetch("/api/runs/"+state.job.id+"/report").then(r=>r.text())'),exact);
 // Reload retains the server-owned run and analyses, without invoking the Agent.
 await call('Page.reload');await wait('typeof state!=="undefined"&&state.info&&state.jobs.length>=3','restored session');
 await click('.workspace-tabs [data-tab="diagnostics"]');await click('#history');
 await wait('!!document.querySelector("[data-run-analyses]")','retained reports after reload');
 await evaluate('document.querySelector("[data-run-record]").open=true');
 await click('[data-run-analyses]');await wait('state.analysisID==='+JSON.stringify(secondID),'restore latest completed report');
 assert.equal(await evaluate('state.report.request.from'),from);
 assert.equal(await evaluate('document.querySelector("#period").value'),'15','viewing history preserves current incident window');
 for(const width of [1440,900,390]){await call('Emulation.setDeviceMetricsOverride',{width,height:1050,deviceScaleFactor:1,mobile:false});await pause(120);assert(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),'history overflow '+width);await shot('analysis-viewport-'+width);}
 assert.equal(errors.length,0,JSON.stringify(errors));
 console.log(JSON.stringify({passed:true,run_id:runID,analysis_reports:2,prior_context_preserved:true,original_evidence_unchanged:true,session_reload:true,js_errors:errors}));
}finally{ws?.close();chrome.kill();await chromeClosed;await rm(profile,{recursive:true,force:true});}
