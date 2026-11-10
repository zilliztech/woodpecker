/* Explanations of collection states; this does not assess evidence. */
'use strict';
const workflowHelp={
 configuration:['Invalid source configuration','The source connection settings could not be validated. No diagnosis can be made from this failure.','Review the reported configuration fields, including deployment selectors, service bindings, authentication and TLS settings.'],
 incomplete:['Collection incomplete','At least one check could not collect all required evidence. This describes the diagnostic run, not a confirmed Woodpecker fault.','Open the failed check for its recorded error. Check the source connection, then its query scope, time range and budget.'],
 abnormal:['Abnormal','Collected evidence matched this check’s configured abnormality rule. It is a finding, not a confirmed root cause.','Inspect the measured values, rule and supporting samples.'],
 unassessed:['Unassessed','The check could not establish a verdict. A failed collection, missing samples, low traffic or insufficient coverage may be responsible.','Open its details for the exact reason. Failed checks may also be counted here; these counts are not mutually exclusive.'],
 failed:['Failed','A required collection or execution step failed. This count does not mean application requests failed.','Inspect the recorded error. Connection failures, permissions, timeouts and exceeded budgets need different fixes.'],
 skipped:['Skipped','The check was not executed because its source, required scope or selection was unavailable.','Review the selected sources and required parameters.'],
 not_configured:['Not configured','No enabled connection is selected for this source. No query was made.','Select a server context with this source, or enter its query API address.'],
 not_selected:['Not selected','This atomic check does not use this source.','No configuration is needed for this source for the selected check.'],
 completed:['Checks completed','The selected collection steps finished. Evidence may still be absent or insufficient for a verdict.','Review findings and unassessed checks separately.'],
 source_unavailable:['Source unavailable','A required source query did not return usable evidence. This does not by itself prove a missing configuration or a Woodpecker fault.','Check the connection first. If it passes, inspect the recorded query error, permissions, time window and source budget.'],
 timeout:['Query timed out','The source query or the overall diagnostic budget expired.','Check source responsiveness, then narrow the time range or selectors and retry.'],
 auth:['Authentication / permission failure','The query API denied access, or the configured authentication secret is unavailable.','Review the saved source authentication, server-side environment variables and query permissions.'],
 transport:['Source is unreachable','The process running wp webui could not reach the query API.','Check the address, service, network, TLS and any local port forwarding.'],
 budget:['Collection budget exceeded','Collection stopped at a configured time, trace, sample or response-size limit. An incomplete collection cannot establish a healthy result.','Narrow the time window and selectors. For Trace, use an exact operation, request ID or Trace ID where available.'],
 no_candidates:['No matching candidates','The filtered query completed but found no matching slow/error span candidates in retained data. This does not measure zero latency or establish that the application is healthy.','Review the operation, configured threshold or error-tag filters, time range and retention if you expected evidence.'],
 absent:['No matching data','The query returned no usable series or spans for these selectors and this time range. This is different from measuring zero.','Check filters, traffic, instrumentation and retention. A successful connection test does not guarantee matching historical data.'],
 insufficient:['Insufficient evidence','Some data was returned, but it did not meet the check’s count, duration, continuity or coverage requirements.','Inspect the stated requirements and select a suitable incident interval.'],
 connection:['Check connection','Runs a bounded read-only query against the currently selected API, using the saved authentication and TLS settings when a saved source is selected.','Prometheus uses a small test query; Jaeger lists services. Success does not establish metric/span availability for the incident or Woodpecker health.']
};
function helpKeyForCheck(row){
 const code=row?.code||'';
 if(code==='no_matching_candidates')return 'no_candidates';
 if(/not_configured/.test(code))return 'not_configured';
 if(/configuration_invalid|invalid_connection_settings|invalid_saved_source/.test(code))return 'configuration';
 if(/http_(401|403)|authenticat|secret_unavailable/.test(code))return 'auth';
 if(/timeout|cancelled/.test(code))return 'timeout';
 if(/budget|limit/.test(code))return 'budget';
 if(/transport|tls_/.test(code))return 'transport';
 if(row?.data_state==='no_data'||code==='no_matching_series'||code==='no_matching_spans')return 'absent';
 if(row?.execution==='failed'||row?.data_state==='source_unavailable')return 'source_unavailable';
 if(row?.data_state==='not_configured')return 'not_configured';
 if(row?.execution==='skipped')return 'skipped';
 if(row?.status==='abnormal')return 'abnormal';
 return 'insufficient';
}
function helpIcon(key,row){
 const h=workflowHelp[key]||workflowHelp.source_unavailable;
 return `<span class="workflow-help" role="button" tabindex="0" data-workflow-help="${esc(key)}" ${row?.code?`data-help-code="${esc(row.code)}"`:''} aria-label="Explain ${esc(h[0])}">?</span>`;
}
// Parameter examples use the same accessible popup as evidence and collection help.
function parameterHelpIcon(title,meaning,example){return `<span class="workflow-help parameter-help" role="button" tabindex="0" data-parameter-help="${esc(JSON.stringify([title,meaning,example]))}" aria-label="Example for ${esc(title)}">?</span>`;}
const workflowHelpSelector='[data-workflow-help],[data-parameter-help]';
let workflowHelpAnchor=null,workflowHelpTimer,workflowHelpPopup;
function closeWorkflowHelp(){if(workflowHelpAnchor)workflowHelpAnchor.removeAttribute('aria-describedby');workflowHelpAnchor=null;if(workflowHelpPopup)workflowHelpPopup.hidden=true;}
function openWorkflowHelp(anchor){
 clearTimeout(workflowHelpTimer);closeWorkflowHelp();let parameter=null;try{parameter=JSON.parse(anchor.dataset.parameterHelp||'null');}catch{}const h=parameter||workflowHelp[anchor.dataset.workflowHelp]||workflowHelp.source_unavailable;
 if(!workflowHelpPopup){workflowHelpPopup=document.createElement('div');workflowHelpPopup.id='workflow-help-popup';workflowHelpPopup.className='workflow-help-popup';workflowHelpPopup.role='tooltip';document.body.append(workflowHelpPopup);workflowHelpPopup.onpointerenter=()=>clearTimeout(workflowHelpTimer);workflowHelpPopup.onpointerleave=()=>closeWorkflowHelp();}
 workflowHelpPopup.innerHTML=`<strong>${esc(h[0])}</strong><p>${esc(h[1])}</p><span class="help-next-label">${parameter?'EXAMPLE':'NEXT STEP'}</span>${parameter?`<pre class="parameter-example">${esc(h[2])}</pre><small>Example only; use the value from your target.</small>`:`<p>${esc(h[2])}</p>`}${anchor.dataset.helpCode?`<small>Recorded code: <code>${esc(anchor.dataset.helpCode)}</code></small>`:''}`;
 workflowHelpPopup.hidden=false;workflowHelpAnchor=anchor;anchor.setAttribute('aria-describedby',workflowHelpPopup.id);
 const a=anchor.getBoundingClientRect(),p=workflowHelpPopup.getBoundingClientRect(),pad=12;
 workflowHelpPopup.style.left=Math.max(pad,Math.min(a.left,innerWidth-p.width-pad))+'px';
 workflowHelpPopup.style.top=Math.max(pad,a.bottom+p.height+10<=innerHeight?a.bottom+8:a.top-p.height-8)+'px';
}
document.addEventListener('pointerover',e=>{const a=e.target.closest(workflowHelpSelector);if(a&&a!==workflowHelpAnchor)openWorkflowHelp(a);});
document.addEventListener('pointerout',e=>{if(e.target.closest(workflowHelpSelector))workflowHelpTimer=setTimeout(closeWorkflowHelp,140);});
document.addEventListener('focusin',e=>{const a=e.target.closest(workflowHelpSelector);if(a)openWorkflowHelp(a);});
document.addEventListener('focusout',e=>{if(e.target.closest(workflowHelpSelector))closeWorkflowHelp();});
document.addEventListener('click',e=>{const a=e.target.closest(workflowHelpSelector);if(a){e.preventDefault();e.stopImmediatePropagation();openWorkflowHelp(a);}else if(!e.target.closest('.workflow-help-popup'))closeWorkflowHelp();},true);
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeWorkflowHelp();const a=e.target.closest(workflowHelpSelector);if(a&&(e.key==='Enter'||e.key===' ')){e.preventDefault();e.stopImmediatePropagation();openWorkflowHelp(a);}},true);
window.addEventListener('resize',closeWorkflowHelp);window.addEventListener('scroll',closeWorkflowHelp,true);

// Shared form presentation; each page supplies its own requirements and validation errors.
function decorateRequiredFields(root,{attempted=false,example,issues=null}={}){
 for(const field of root.querySelectorAll('label,.evidence-address,.field')){
  const input=field.querySelector('input:not([type=hidden]),select,textarea');if(!input)continue;
  const mark=field.querySelector('.required-mark'),required=!!mark&&!mark.hasAttribute('data-source-alternative');
  field.classList.toggle('command-field-required',required);
  const missing=attempted&&(issues?issues.has(input):required&&(input.type==='checkbox'?!input.checked:!input.value.trim()));
  field.classList.toggle('command-field-missing',missing);if(missing)input.setAttribute('aria-invalid','true');else input.removeAttribute('aria-invalid');
  if(required)input.setAttribute('aria-required','true');else input.removeAttribute('aria-required');
  if(mark){const [title,meaning,sample]=example(input);let group=mark.parentElement;
   if(!group.classList.contains('command-required-badge')){group=document.createElement('span');group.className='command-required-badge';mark.before(group);group.append(mark);group.insertAdjacentHTML('beforeend',parameterHelpIcon(title,meaning,sample));}
  }
 }
}
function focusMissingInput(root){
 const first=root.querySelector('[aria-invalid="true"]:not(:disabled)');if(!first)return;
 for(let p=first.parentElement;p&&p!==root;p=p.parentElement)if(p.tagName==='DETAILS')p.open=true;
 first.scrollIntoView({block:'center',behavior:'instant'});first.focus({preventScroll:true});
}
