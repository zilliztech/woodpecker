// Real deployed WebUI acceptance. Backend responses are not mocked here.
import {spawn} from 'node:child_process';
import {mkdtemp, readFile, writeFile, rm, readdir} from 'node:fs/promises';
import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';

const [url, input, output] = process.argv.slice(2);
assert(url && input && output, 'Usage: webui_browser.mjs URL INPUT.json OUTPUT');
const manifest = JSON.parse(await readFile(input, 'utf8'));
const chromePath = process.env.CHROME_BIN || [
 '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', '/usr/bin/google-chrome',
 '/usr/bin/google-chrome-stable', '/usr/bin/chromium', '/usr/bin/chromium-browser',
].find(existsSync);
assert(chromePath, 'Chrome is required; set CHROME_BIN');
const profile = await mkdtemp(join(output, 'chrome-profile-'));
const chrome = spawn(chromePath, ['--headless=new', '--disable-gpu', '--no-first-run', '--no-default-browser-check',
 '--no-sandbox', '--remote-debugging-port=0', '--user-data-dir=' + profile, 'about:blank'], {stdio:['ignore','ignore','pipe']});
let stderr = '', launchError, ws, call, evaluate;
chrome.stderr.on('data', b => { stderr = (stderr + b.toString()).slice(-32768); });
chrome.on('error', e => { launchError = e; });
const closed = new Promise(resolve => chrome.once('close', resolve));
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));
const errors = [], runs = [], checks = [];
const hash = raw => createHash('sha256').update(raw).digest('hex');
const stats = async () => (await fetch(manifest.probe + '/stats')).json();
try {
 let port;
 for (let i=0; i<100; i++) {
  if (launchError) throw launchError;
  assert(chrome.exitCode === null && chrome.signalCode === null, 'Chrome exited before startup');
  try { port = (await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]; break; }
  catch { await pause(100); }
 }
 assert(port, 'Chrome startup deadline exceeded');
 const target = await (await fetch('http://127.0.0.1:' + port + '/json/new?about:blank', {method:'PUT'})).json();
 ws = new WebSocket(target.webSocketDebuggerUrl);
 await new Promise((resolve,reject) => { ws.onopen=resolve; ws.onerror=reject; });
 let seq=0;
 const pending = new Map();
 ws.onmessage = event => {
  const m = JSON.parse(event.data);
  if (m.id) { const p=pending.get(m.id); if(p) { pending.delete(m.id); clearTimeout(p.timer); m.error?p.reject(Error(JSON.stringify(m.error))):p.resolve(m.result); } }
  else if (m.method === 'Runtime.exceptionThrown') errors.push(m.params.exceptionDetails.text);
 };
 call = (method,params={}) => new Promise((resolve,reject) => {
  const id=++seq, timer=setTimeout(()=>{pending.delete(id); reject(Error('CDP timeout: '+method));},15000);
  pending.set(id,{resolve,reject,timer}); ws.send(JSON.stringify({id,method,params}));
 });
 evaluate = async expression => {
  const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true,userGesture:true});
  if(r.exceptionDetails) throw Error(JSON.stringify(r.exceptionDetails));
  return r.result.value;
 };
 const wait = async (expression,message,timeout=12000) => {
  const deadline=Date.now()+timeout;
  while(Date.now()<deadline) { if(await evaluate(expression)) return; await pause(80); }
  throw Error(message+'\n'+await evaluate('document.body.innerText.slice(-3000)'));
 };
 const until = async (predicate,message,timeout=5000) => {
  const deadline=Date.now()+timeout;
  while(Date.now()<deadline) { if(await predicate()) return; await pause(50); }
  throw Error(message);
 };
 const set = (id,value) => evaluate(`(()=>{const e=document.getElementById(${JSON.stringify(id)});if(!e)throw Error('Missing field');e.value=${JSON.stringify(value)};e.dispatchEvent(new Event('change',{bubbles:true}));})()`);
 const click = id => evaluate(`document.getElementById(${JSON.stringify(id)}).click()`);
 const check = async (id,value=true) => { if(await evaluate(`document.getElementById(${JSON.stringify(id)}).checked`)!==value) await click(id); };
 const historical = async () => {
  await set('mode','historical');
  // The shipped datetime inputs have a one-second step. The incident is
  // comfortably inside the fixture's initial scrape/export grace interval.
  await set('from',new Date(manifest.from).toISOString().slice(0,19));
  await set('to',new Date(manifest.to).toISOString().slice(0,19));
 };
 const modules = async (ids,scope='wp',flow='write') => {
  await set('execution','modules'); await set('scope',scope); await set('flow',flow);
  for(const id of await evaluate("checked('modules')")) await check('module-'+id,false);
  for(const id of ids) await check('module-'+id);
 };
 const integrated = async (selected,scope='wp') => {
  await set('execution','integrated'); await set('scope',scope); await set('flow','write'); await check('auto-sources',false);
  for(const source of ['wpcli','prometheus','trace','loki']) await check('source-'+source,selected.includes(source));
 };
 const preview = async () => {
  const before=(await stats()).length;
  await click('preview');
  await wait("document.getElementById('form-status').textContent.includes('Plan ready')||document.getElementById('form-status').textContent.includes('cannot run')",'Preview did not complete');
  assert.equal((await stats()).length,before,'Preview contacted a backend');
 };
 let currentRequest;
 const start = async () => {
  assert(!await evaluate("document.getElementById('run').disabled"),'Run unavailable');
  currentRequest=await evaluate('state.planned');
  const previous=await evaluate('state.selected'); await click('run');
  await wait(`state.selected && state.selected!==${JSON.stringify(previous)}`,'Run did not start');
 };
 const capture = async name => {
  await wait('state.raw!==null && !state.active','No final stored report',15000);
  const raw=await evaluate('state.raw'), report=JSON.parse(raw);
  const row={case:name,handle:await evaluate('state.selected'),status:report.status,report:name+'.json',request:name+'.request.json',plan:name+'.plan.json',sha256:hash(raw),
   rendered_ids:await evaluate("Array.from(document.querySelectorAll('[data-finding-id]')).map(n=>n.dataset.findingId)"),evidence:[]};
  await writeFile(join(output,row.report),raw);
  await writeFile(join(output,row.request),JSON.stringify(currentRequest,null,2));
  const job=await evaluate("api('/api/runs/'+encodeURIComponent(state.selected)).then(r=>r.value)");
  row.progress_events=job.events;
  await writeFile(join(output,row.plan),JSON.stringify(job.plan,null,2));
  const ids=new Set((report.summary?.findings||[]).map(f=>f.id));
  assert(row.rendered_ids.every(id=>ids.has(id)),'UI changed finding IDs');
  runs.push(row);
  return {row,report,raw};
 };
 const run = async name => { await preview(); await start(); return capture(name); };
 const inspect = async (row,report,module,kind) => {
  const child=report.runs.find(c=>c.module_id===module);
  const observations=child.report.results.flatMap(r=>r.observations||[]);
  const obs=observations.find(o=>o.kind===kind && o.status==='observed'); assert(obs,'No actual '+kind);
  const selector=`button[data-module-id=${JSON.stringify(module)}][data-observation-id=${JSON.stringify(obs.id)}]`;
  assert(await evaluate(`!!document.querySelector(${JSON.stringify(selector)})`),'Original observation has no navigation button');
  await evaluate(`document.querySelector(${JSON.stringify(selector)}).click()`);
  await wait(`document.getElementById('evidence-title').textContent===${JSON.stringify(kind)}`,'Original evidence not shown');
  const raw=await evaluate("document.getElementById('evidence-body').textContent");
  assert.equal(JSON.parse(raw).id,obs.id);
  if(obs.query) assert(await evaluate("document.getElementById('evidence-facts').textContent.includes('Query executed:Yes')"));
  if(kind==='jaeger_trace') assert(await evaluate("document.getElementById('evidence-facts').textContent.includes('Selected span')"));
  const file=row.case+'.'+module+'.evidence.json'; await writeFile(join(output,file),raw);
  row.evidence.push({module,id:obs.id,file}); await click('close-evidence');
 };

 await call('Page.enable'); await call('Runtime.enable');
 const browserVersion=(await call('Browser.getVersion')).product;
 await call('Browser.setDownloadBehavior',{behavior:'allow',downloadPath:output,eventsEnabled:true});
 await call('Page.navigate',{url});
 await wait("typeof state!=='undefined' && state.catalog!==null",'No actual catalog');
 assert.equal(await evaluate("document.querySelectorAll('#modules input').length"),8);
 for(const source of ['prometheus','trace','loki']) await set('connection-'+source,'acceptance');
 assert(!JSON.stringify(await evaluate('state.catalog')).includes('http://127.0.0.1:'));
 checks.push('actual shipped catalog; named connections expose no endpoints');

 await modules(['prometheus']); await historical();
 await set('target-prometheus-deployment',manifest.deployment);
 let metric=await run('metrics-write'); assert.equal(metric.report.status,'completed');
 await inspect(metric.row,metric.report,'prometheus','prometheus_matrix');
 assert(await evaluate("document.getElementById('report').textContent.includes('unknown')"));
 checks.push('real metrics investigation; executed query, original observation and unknown population');
 const oldDownloads=new Set(await readdir(output)); await click('download'); let download;
 await until(async()=>{download=(await readdir(output)).find(f=>f.startsWith('wpdiag-')&&f.endsWith('.json')&&!oldDownloads.has(f));return download;},'No downloaded JSON');
 assert.equal(await readFile(join(output,download),'utf8'),metric.raw);
 checks.push('byte-identical browser JSON download');
 await modules(['prometheus'],'wp','read'); const read=await run('metrics-read'); assert.equal(read.report.status,'completed');

 await modules(['milvus.append'],'e2e','write');
 const missing=await run('append-missing-producer');
 assert.equal(missing.report.runs[0].module_id,'milvus.append');
 assert(JSON.stringify(missing.report).includes('inconclusive'));
 checks.push('independent append stage with absent Milvus producer remains inconclusive');

 await integrated(['prometheus','trace','loki']);
 await set('target-trace-trace_id',manifest.associated_trace_id);
 const multi=await run('multi-source'); assert.equal(multi.report.status,'completed');
 assert(multi.report.summary.associations.some(a=>a.identity===manifest.associated_trace_id));
 await inspect(multi.row,multi.report,'trace','jaeger_trace');
 await inspect(multi.row,multi.report,'loki','loki_log_page');
 assert(await evaluate("document.getElementById('report').textContent.includes('add entry failed')"));
 assert(await evaluate("document.getElementById('report').textContent.includes('not executed')"));
 checks.push('actual trace/log suspicion after data recovery; original event/span navigation');
 for(const theme of ['light','dark']) for(const width of [1440,390]) {
  await call('Emulation.setDeviceMetricsOverride',{width,height:1000,deviceScaleFactor:1,mobile:width<500});
  await evaluate(`document.documentElement.dataset.theme='${theme}'`);
  const screen=await call('Page.captureScreenshot',{format:'png'});
  await writeFile(join(output,`deployed-${theme}-${width}.png`),Buffer.from(screen.data,'base64'));
 }
 await call('Emulation.clearDeviceMetricsOverride');
 // Refresh inspects a retained report; it must not start another collection.
 const retained=multi.row.handle, beforeRefresh=(await stats()).length;
 await call('Page.reload'); await wait("typeof state!=='undefined' && state.catalog!==null",'Reload failed');
 await wait(`!!Array.from(document.getElementById('history').options).find(o=>o.value===${JSON.stringify(retained)})`,'Retained report absent');
 await set('history',retained); await wait('state.raw!==null','No retained JSON');
 assert.equal(await evaluate('state.raw'),multi.raw); assert.equal((await stats()).length,beforeRefresh);
 checks.push('reload retains exact in-session result without recollecting');
 for(const source of ['prometheus','trace','loki']) await set('connection-'+source,'acceptance');
 await historical(); await integrated(['trace','loki'],'e2e');
 await set('target-trace-trace_id',manifest.associated_trace_id);
 const e2e=await run('e2e-partial'); assert(JSON.stringify(e2e.report).includes('partial_observed_path'));
 checks.push('WP-only traces retain missing Milvus E2E coverage');
 await modules(['trace'],'wp','read'); await set('target-trace-trace_id',manifest.read_trace_id);
 const traceRead=await run('read-trace'); assert.equal(traceRead.report.status,'completed');
 await integrated(['trace','loki']); await set('target-trace-trace_id',manifest.associated_trace_id);
 await set('connection-loki','unreachable'); assert.equal((await run('selected-unreachable')).report.status,'partial');
 await set('connection-loki','disabled'); assert.equal((await run('selected-disabled')).report.status,'partial');
 await set('connection-loki','unreachable'); await integrated(['trace']);
 assert.equal((await run('unselected-unreachable')).report.status,'completed');
 checks.push('selected failed/disabled sources retained; unselected failure has no dependency');

 await modules(['prometheus']); await set('connection-prometheus','deny');
 const denied=await run('transport-denied'); assert.equal(denied.report.status,'failed');
 assert(JSON.stringify(denied.report).includes('http_401'));
 checks.push('explicit synthetic transport 401; raw backend body withheld');
 await set('connection-prometheus','acceptance'); await set('mode','snapshot');
 const unsupportedBefore=(await stats()).length; await preview();
 assert(await evaluate("document.getElementById('run').disabled"));
 assert(await evaluate("document.getElementById('plan-content').textContent.includes('profile_not_supported')"));
 assert.equal((await stats()).length,unsupportedBefore);
 checks.push('unsupported selection rejected before I/O');

 await historical(); await set('output-limit',String(4096/1048576));
 const limited=await run('output-limit'); assert.equal(limited.report.status,'budget_exceeded');
 assert(Buffer.byteLength(limited.raw)<=4096); await set('output-limit','16');
 await modules(['trace']); await set('target-trace-trace_id',manifest.associated_trace_id);
 await set('input-limit',String(1024/1048576));
 const inputLimited=await run('input-limit'); assert.equal(inputLimited.report.status,'budget_exceeded');
 await set('input-limit','8'); checks.push('actual report/input byte limits stay explicit');

 await modules(['prometheus']); await set('mode','live'); await set('duration','30');
 await preview(); const liveBefore=(await stats()).length; await start();
 await wait("state.active && !document.getElementById('cancel').disabled",'No active live run');
  await wait("document.getElementById('progress').textContent.includes('started')",'No module-start progress');
 await click('cancel'); const live=await capture('cancel-live'); assert.equal(live.report.status,'cancelled');
 assert.equal((await stats()).length,liveBefore);
 checks.push('live-window cancellation exits before query');

 await historical(); await set('connection-prometheus','hold'); await set('budget','15'); await preview();
 const heldBefore=(await stats()).length; await start();
 await until(async()=> (await stats()).slice(heldBefore).some(r=>r.mode==='hold'&&r.backend_finished_at),'No real backend query before cancellation');
 assert(await evaluate('state.active'),'Run ended before cancellation');
 const cancelTime=Date.now(); await click('cancel');
 const inflight=await capture('cancel-inflight'); assert.equal(inflight.report.status,'cancelled');
 await until(async()=> (await stats()).slice(heldBefore).some(r=>r.client_disconnected_at),'Collector did not close held response transport');
 const cancelElapsed=Date.now()-cancelTime; assert(cancelElapsed<5000);
 checks.push('in-flight real-query return transport closed after UI cancellation');

 await set('budget','0.5'); await preview(); const deadlineBefore=(await stats()).length;
 await start(); const deadline=await capture('deadline-inflight'); assert.equal(deadline.report.status,'budget_exceeded');
 await until(async()=> (await stats()).slice(deadlineBefore).some(r=>r.client_disconnected_at),'Budget did not close transport');
 checks.push('deadline closes held query return transport independently of Cancel');

 const foreign=await fetch(url+'/api/catalog',{headers:{Origin:'http://foreign.invalid'}}); assert.equal(foreign.status,403);
 const noToken=await fetch(url+'/api/plan',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'}); assert.equal(noToken.status,403);
 checks.push('local origin and session checks on shipped server');
 const active=await (await fetch(url+'/api/runs')).json();
 for(const brief of active) {
  const job=await (await fetch(url+'/api/runs/'+encodeURIComponent(brief.id))).json();
  assert(job.report_ready,'An active collection was abandoned');
 }
 checks.push('no abandoned active run after all controls');
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({passed:true,browser_version:browserVersion,node_version:process.version,
  associated_trace_id:manifest.associated_trace_id,download,runs,checks,cancel_elapsed_ms:cancelElapsed,uncaught_exceptions:errors}));
} catch(error) {
 if(call) { try {
  const screen=await call('Page.captureScreenshot',{format:'png'});
  await writeFile(join(output,'failure.png'),Buffer.from(screen.data,'base64'));
  await writeFile(join(output,'failure.txt'),await evaluate('document.body.innerText'));
  await writeFile(join(output,'failure-report.json'),await evaluate("typeof state!=='undefined' && state.raw ? state.raw : '{}'"));
 } catch {} }
 throw error;
} finally {
 if(ws)ws.close();
 if(chrome.exitCode===null&&chrome.signalCode===null) chrome.kill('SIGTERM');
 await Promise.race([closed,pause(2000)]);
 if(chrome.exitCode===null&&chrome.signalCode===null) { chrome.kill('SIGKILL'); await Promise.race([closed,pause(1000)]); }
 await writeFile(join(output,'chrome.stderr'),stderr);
 await rm(profile,{recursive:true,force:true,maxRetries:5,retryDelay:100});
}
