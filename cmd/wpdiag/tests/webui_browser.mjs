// Browser acceptance for the real local WebUI/API. No npm dependencies.
import {spawn} from 'node:child_process';
import {mkdtemp,readFile,writeFile,rm,readdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import assert from 'node:assert/strict';
import {join} from 'node:path';

const [url,begin,end,output]=process.argv.slice(2);
assert(url&&output,'Usage: webui_browser.mjs URL FROM_SECONDS TO_SECONDS OUTPUT');
const chromePath=process.env.CHROME_BIN||['/Applications/Google Chrome.app/Contents/MacOS/Google Chrome','/usr/bin/google-chrome','/usr/bin/google-chrome-stable','/usr/bin/chromium','/usr/bin/chromium-browser'].find(existsSync);
assert(chromePath,'Chrome is required; set CHROME_BIN');
const profile=await mkdtemp('/tmp/wpdiag-webui-chrome-');
const chrome=spawn(chromePath,['--headless=new','--disable-gpu','--no-first-run','--no-default-browser-check','--no-sandbox','--remote-debugging-port=0','--user-data-dir='+profile,'about:blank'],{stdio:'ignore'});
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
let ws,call,evaluate;
const errors=[],checks=[];
try{
 let port;
 for(let i=0;i<100;i++){try{port=(await readFile(join(profile,'DevToolsActivePort'),'utf8')).split('\n')[0];break;}catch{await pause(100);}}
 assert(port,'Chrome did not start');
 const target=await(await fetch('http://127.0.0.1:'+port+'/json/new?about:blank',{method:'PUT'})).json();
 ws=new WebSocket(target.webSocketDebuggerUrl);await new Promise((resolve,reject)=>{ws.onopen=resolve;ws.onerror=reject;});
 let seq=0;const pending=new Map();
 ws.onmessage=event=>{const m=JSON.parse(event.data);if(m.id){const p=pending.get(m.id);if(p){pending.delete(m.id);clearTimeout(p.timer);m.error?p.reject(Error(JSON.stringify(m.error))):p.resolve(m.result);}}else if(m.method==='Runtime.exceptionThrown')errors.push(m.params.exceptionDetails.text);};
 call=(method,params={})=>new Promise((resolve,reject)=>{const id=++seq;const timer=setTimeout(()=>{pending.delete(id);reject(Error('Timeout '+method));},20000);pending.set(id,{resolve,reject,timer});ws.send(JSON.stringify({id,method,params}));});
 evaluate=async expression=>{const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true,userGesture:true});if(r.exceptionDetails)throw Error(JSON.stringify(r.exceptionDetails));return r.result.value;};
 const wait=async(expression,message,timeout=10000)=>{const deadline=Date.now()+timeout;while(Date.now()<deadline){if(await evaluate(expression))return;await pause(80);}throw Error(message+'\n'+await evaluate('document.body.innerText.slice(-5000)'));};
 const set=async(id,value)=>evaluate(`(()=>{const e=document.getElementById(${JSON.stringify(id)});e.value=${JSON.stringify(value)};e.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 const click=async(id)=>evaluate(`document.getElementById(${JSON.stringify(id)}).click()`);
 const check=async(id,value=true)=>{if(await evaluate(`document.getElementById(${JSON.stringify(id)}).checked`)!==value)await click(id);};
 const preview=async()=>{await click('preview');await wait("document.getElementById('form-status').textContent.includes('Plan ready')||document.getElementById('form-status').textContent.includes('cannot run')",'No completed plan');};
 const run=async()=>{assert(!await evaluate("document.getElementById('run').disabled"),'Run button should be available');const previous=await evaluate('state.selected');await click('run');await wait(`state.selected && state.selected!==${JSON.stringify(previous)} && state.raw!==null`,'No finished report');return evaluate('JSON.parse(state.raw)');};
 const historical=async()=>{await set('mode','historical');await set('from',new Date(Number(begin)*1000).toISOString().slice(0,19));await set('to',new Date(Number(end)*1000).toISOString().slice(0,19));};
 const chooseModule=async(id,flow='write')=>{await set('execution','modules');await set('flow',flow);for(const selected of await evaluate("checked('modules')"))await check('module-'+selected,false);await check('module-'+id);};
 const expectStatus=(report,status)=>assert.equal(report.status,status);
 await call('Page.enable');await call('Runtime.enable');await call('Accessibility.enable');
 await call('Browser.setDownloadBehavior',{behavior:'allow',downloadPath:output,eventsEnabled:true});
 await call('Page.navigate',{url});
 await wait("typeof state!=='undefined' && state.catalog!==null",'No module catalog');
 assert.equal(await evaluate("document.querySelectorAll('#modules input').length"),8);
 await set('connection-prometheus','fixture');await set('connection-trace','fixture');await historical();await preview();
 assert.equal(await evaluate("document.querySelectorAll('#plan-content .plan-entry').length"),4);
 assert(await evaluate("document.getElementById('plan-content').innerText.includes('wpcli: no connection configured')"));
 checks.push('catalog-driven selectors and local plan');
 let report=await run();expectStatus(report,'completed');
 assert.deepEqual(report.runs.map(r=>r.module_id),['milvus.append','milvus.persistence','trace','loki']);assert(report.summary.associations.length>0,'No matching retained trace/log association');
 assert(await evaluate("document.getElementById('report').innerText.includes('suspected_quota_admission_wait')"),'Slow quota must be visible');
 const ids=report.summary.findings.map(f=>f.id);
 const displayed=await evaluate("Array.from(document.querySelectorAll('[data-finding-id]')).map(e=>e.dataset.findingId)");
 assert(displayed.every(id=>ids.includes(id)),'UI changed original finding IDs');
 const raw=await evaluate('state.raw');
 assert(!raw.includes('sentinel-secret'));
 assert(!await evaluate("document.body.innerText.includes('127.0.0.1:')"),'Private endpoint leaked');
 checks.push('integrated write stages and suspect-step findings');
 // Navigate from a finding to a real original metric observation.
 await evaluate("document.querySelector('[data-module=\"milvus.append\"] .evidence-links button').click()");
 await wait("document.getElementById('evidence-body').textContent.includes('prometheus_matrix')",'No original observation');
 assert(await evaluate("document.getElementById('evidence-facts').innerText.includes('Query executed:Yes')"));
 assert(await evaluate("document.getElementById('evidence-body').textContent.includes('promql')"));
 await click('close-evidence');checks.push('finding-to-original-observation navigation and executed query');
 // Export must use the stored bytes, not JSON stringify of a browser-rounded report.
 const previousDownloads=new Set(await readdir(output));await click('download');let downloaded;
 for(let i=0;i<100;i++){const files=(await readdir(output)).filter(f=>f.endsWith('.json')&&f.startsWith('wpdiag-')&&!previousDownloads.has(f));if(files.length){downloaded=files[0];break;}await pause(50);}
 assert(downloaded,'No JSON export');assert.equal(await readFile(join(output,downloaded),'utf8'),raw);checks.push('byte-identical JSON export');
 // Inspect the exact trace/log association produced by the existing runner.
 await evaluate("[...document.querySelectorAll('#report .report-details')].find(d=>d.querySelector('summary')?.textContent.startsWith('Related evidence')).open=true");
 await evaluate("document.querySelector('#report .association .evidence-links button').click()");
 await wait("!document.getElementById('evidence-body').textContent.startsWith('Loading')",'No associated observation');
 await click('close-evidence');checks.push('matching observed trace/log association navigation');
 await chooseModule('trace','write');await set('mode','snapshot');await set('target-trace-trace_id','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');await preview();
 report=await run();expectStatus(report,'completed');assert.equal(report.runs[0].module_id,'trace');
 await evaluate("document.querySelector('[data-module=trace] .evidence-links button').click()");
 await wait("document.getElementById('evidence-body').textContent.includes('jaeger_trace')",'No selected trace evidence');
 assert(await evaluate("document.getElementById('evidence-facts').textContent.includes('Selected span')"));await click('close-evidence');checks.push('independent exact trace and original span inspection');
 await historical();await chooseModule('loki','write');await preview();report=await run();expectStatus(report,'completed');assert.equal(report.runs[0].module_id,'loki');
 await evaluate("document.querySelector('[data-module=loki] .evidence-links button').click()");
 await wait("document.getElementById('evidence-body').textContent.includes('loki_log_page')",'No retained log evidence');
 assert(await evaluate("document.getElementById('evidence-facts').textContent.includes('add entry failed')"));await click('close-evidence');checks.push('independent Loki diagnosis and retained excerpt inspection');
 await set('execution','integrated');
 // Partial sources retain success and per-source error information.
 await set('execution','integrated');await check('auto-sources',false);await check('source-prometheus');await check('source-trace');await set('connection-trace','unavailable');await preview();
 report=await run();expectStatus(report,'partial');assert(report.runs.some(r=>r.module_id==='trace'&&r.report.status==='disabled'));
 assert(await evaluate("document.getElementById('report').innerText.includes('connection_disabled')"));checks.push('partial selected source, original metric evidence retained');
 // Single-stage read investigations use the same runner and source profiles.
 await chooseModule('milvus.consume','read');await preview();report=await run();expectStatus(report,'completed');assert.equal(report.runs.length,1);assert.equal(report.runs[0].module_id,'milvus.consume');checks.push('individual consume-stage investigation');
 await chooseModule('milvus.query','read');await preview();report=await run();expectStatus(report,'completed');assert.equal(report.runs[0].module_id,'milvus.query');const retainedQuery=await evaluate('state.selected');checks.push('individual query-stage investigation');
 // An independent source-only WP read works without any Milvus-stage selection.
 await chooseModule('prometheus','read');await set('scope','wp');await preview();report=await run();expectStatus(report,'completed');assert.equal(report.runs[0].report.selection.scope,'wp');checks.push('independent WP source diagnosis');
 // Unavailable connection is a source error; no response body credential leaks.
 await set('scope','e2e');await set('connection-prometheus','denied');await preview();report=await run();expectStatus(report,'failed');assert(await evaluate("document.getElementById('report').innerText.includes('http_401')"));assert(!await evaluate("document.body.innerText.includes('sentinel-secret')"));checks.push('source error and private-value handling');
 await set('connection-prometheus','fixture');
 // Keep a selected module visible when a changed window is unsupported.
 await chooseModule('milvus.append','write');await set('mode','snapshot');await preview();
 assert(await evaluate("document.getElementById('run').disabled"));assert(await evaluate("document.getElementById('plan-content').innerText.includes('profile_not_supported')"));checks.push('unsupported selection explained before collection');
 // Native historical limitations remain visible in an explicit mixed selection.
 await historical();await set('execution','integrated');await set('scope','e2e');await check('auto-sources',false);await check('source-trace',false);await check('source-wpcli');await check('current-native');await preview();
 assert(await evaluate("document.getElementById('plan-content').innerText.includes('snapshot')"));assert(await evaluate("document.getElementById('plan-content').innerText.includes('current_context')"));
 assert(await evaluate("document.getElementById('plan-content').innerText.includes('connection_not_found')"));checks.push('historical interval and separately labeled current native plan');
 await check('source-wpcli',false);await check('current-native',false);
 // A live run can be cancelled through the UI, retaining the original final report.
 await chooseModule('milvus.append','write');await set('mode','live');await set('duration','30');await preview();
 const previous=await evaluate('state.selected');await click('run');await wait(`state.selected!==${JSON.stringify(previous)} && state.active && !document.getElementById('cancel').disabled`,'No active run');
 await wait("document.getElementById('progress').innerText.includes('started')",'No module progress');
 await click('cancel');await wait("state.raw!==null && JSON.parse(state.raw).status==='cancelled'",'No cancelled final report');checks.push('live progress and cancellation');
 // Budget expiration is distinguished from cancellation and health.
 await set('budget','0.02');await preview();report=await run();expectStatus(report,'invalid');checks.push('source-owned live-window budget validation');
 await historical();await set('connection-prometheus','slow');await preview();report=await run();expectStatus(report,'budget_exceeded');checks.push('time budget exhaustion state');
 await set('connection-prometheus','fixture');await set('budget','60');await historical();await preview();report=await run();expectStatus(report,'completed');
 const count=await evaluate("document.getElementById('history').options.length");assert(count<=9);assert(count>2);
 const earlier=retainedQuery;await set('history',earlier);await wait(`state.selected===${JSON.stringify(earlier)} && state.raw!==null`,'No retained historical report');checks.push('bounded history and preserved prior reports');
 // Presentation fixture: preserve conflicting source facts and render log-like
 // HTML as text. This creates no production rule and never fabricates a run.
 await evaluate(`(()=>{const fixture=JSON.parse(state.raw);const run=fixture.runs[0];const fs=run.report.assessments?.[0]?.findings||run.report.results[0].findings;const f=fs.find(f=>f.supporting_observations?.length);if(!f)throw Error('No fixture support');f.contradicting_observations=[f.supporting_observations[0]];f.summary='<img src=x onerror=alert(1)> literal retained event';state.report=fixture;renderReport(fixture);})()`);
 assert(await evaluate("document.getElementById('report').textContent.includes('Contradicting observations')"));assert(await evaluate("document.getElementById('report').textContent.includes('<img src=x onerror=alert(1)>')"));assert.equal(await evaluate("document.querySelectorAll('#report img').length"),0);checks.push('conflicting evidence preserved and event text escaped');await evaluate('state.report=JSON.parse(state.raw);renderReport(state.report)');
 // All form controls have accessible names; verify real Chrome AX output.
 const ax=await call('Accessibility.getFullAXTree');const unnamed=ax.nodes.filter(n=>!n.ignored&&['textbox','combobox','checkbox','button'].includes(n.role?.value)&&!n.name?.value);assert.equal(unnamed.length,0,'Unnamed controls: '+JSON.stringify(unnamed));checks.push('accessible form/control names and native evidence dialog');
 for(const theme of ['light','dark'])for(const width of [1440,390]){
  await call('Emulation.setDeviceMetricsOverride',{width,height:1000,deviceScaleFactor:1,mobile:width<500});
  await evaluate(`document.documentElement.dataset.theme='${theme}';window.scrollTo(0,0)`);await pause(80);
  const dimensions=await evaluate('({viewport:innerWidth,content:document.documentElement.scrollWidth})');assert(dimensions.content<=dimensions.viewport+1,'Horizontal overflow: '+JSON.stringify({theme,width,...dimensions}));
  const screenshot=await call('Page.captureScreenshot',{format:'png'});await writeFile(join(output,`webui-${theme}-${width}.png`),Buffer.from(screenshot.data,'base64'));
 }
 checks.push('desktop/mobile light/dark layouts');
 assert.deepEqual(errors,[]);console.log(JSON.stringify({passed:true,count:checks.length,checks,uncaught_exceptions:0}));
}catch(error){
 if(call){try{const screen=await call('Page.captureScreenshot',{format:'png'});await writeFile(join(output,'failure.png'),Buffer.from(screen.data,'base64'));await writeFile(join(output,'failure.txt'),await evaluate('document.body.innerText'));await writeFile(join(output,'failure-report.json'),await evaluate("typeof state!=='undefined' && state.raw ? state.raw : '{}'"));}catch{}}
 throw error;
}finally{
 if(ws)ws.close();chrome.kill('SIGTERM');await pause(200);await rm(profile,{recursive:true,force:true,maxRetries:5,retryDelay:100});
}
