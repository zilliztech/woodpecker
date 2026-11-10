/* Shared measurement-only SVG renderer. No diagnostic rules. */
function renderMetricCurve(curve) {
 const valid=curve.points.filter(p=>Number.isFinite(p.value));
 if(!valid.length){$('#inspection-chart').innerHTML=`<p class="inspection-empty">${curve.kind==='interval'?'P99 data is unavailable for this interval.':'No measurements available to plot.'}</p>`;return;}
 const width=Math.max(320,Math.min(760,$('#inspection-chart').clientWidth||760)),height=245,left=57,right=18,top=20,bottom=44;
 const start=curve.points.reduce((v,p)=>Math.min(v,p.time_ms),Infinity),end=curve.points.reduce((v,p)=>Math.max(v,p.time_ms),-Infinity);
 const low=valid.reduce((v,p)=>Math.min(v,p.value),0);
 let high=valid.reduce((v,p)=>Math.max(v,Number.isFinite(p.upper)?p.upper:p.value),curve.reference||0);if(high<=low)high=low+1;
 high+=(high-low)*.1;
 const x=t=>left+(t-start)/Math.max(1,end-start)*(width-left-right),y=v=>height-bottom-(v-low)/(high-low)*(height-top-bottom);
 const number=v=>Number(v).toLocaleString(undefined,{maximumSignificantDigits:4});
 const line=field=>{let path='',open=false;for(const p of curve.points){const value=p[field];if(!Number.isFinite(value)||p.code){open=false;continue;}path+=(open?' L':' M')+x(p.time_ms).toFixed(2)+','+y(value).toFixed(2);open=true;}return path;};
 const ticks=[0,.25,.5,.75,1].map(f=>{const v=low+(high-low)*f;return `<line x1="${left}" x2="${width-right}" y1="${y(v)}" y2="${y(v)}" class="chart-grid"/><text x="${left-8}" y="${y(v)+4}" text-anchor="end">${esc(number(v))}</text>`;}).join('');
 const ref=Number.isFinite(curve.reference)?`<line x1="${left}" x2="${width-right}" y1="${y(curve.reference)}" y2="${y(curve.reference)}" class="chart-reference"/><text x="${width-right}" y="${y(curve.reference)-6}" text-anchor="end" class="chart-reference-label">Reference ${esc(number(curve.reference))}</text>`:'';
 const stride=Math.max(1,Math.ceil(curve.points.length/600));
 const points=curve.points.map((p,i)=>{
  if(!Number.isFinite(p.value)||(i%stride!==0&&i!==curve.points.length-1))return '';
  const upper=Number.isFinite(p.upper)?p.upper:null;
  const label=`${stamp(p.time_ms)} · ${upper===null?(curve.kind==='interval'?'>= ':'')+number(p.value):number(p.value)+'–'+number(upper)} ${curve.unit}${p.count!==undefined?' · '+p.count+' completions':''}${p.code?' · '+p.code:p.matched_rule?' · matched sustained rule':p.above_reference?' · above reference':''}`;
  return `<g class="chart-point ${p.code?'unqualified':p.matched_rule?'matched':p.above_reference?'above':''}" data-chart-point="${i}" tabindex="0" aria-label="${esc(label)}"><title>${esc(label)}</title>${upper!==null?`<line x1="${x(p.time_ms)}" x2="${x(p.time_ms)}" y1="${y(p.value)}" y2="${y(upper)}" class="chart-range"/>`:''}<circle cx="${x(p.time_ms)}" cy="${y(p.value)}" r="${p.matched_rule?4:3}"/>${upper===null&&curve.kind==='interval'?`<text x="${x(p.time_ms)}" y="${y(p.value)-7}" text-anchor="middle">↑</text>`:''}<circle cx="${x(p.time_ms)}" cy="${y(p.value)}" r="11" class="chart-hit"/></g>`;
 }).join('');
 const axisTime=t=>new Date(t).toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit',timeZone:$('#zone').value==='utc'?'UTC':undefined});
 $('#inspection-chart').innerHTML=`<div class="inspection-chart-caption"><b>${esc(curve.kind==='interval'?'Minute P99 bucket interval':curve.unit==='events / scrape'?'Count increase per scrape':'Measured values')}</b><span>${esc(curve.unit)}</span></div><svg class="inspection-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${esc(curve.metric+' time series')}">${ticks}${ref}<path d="${line('value')}" class="chart-line"/>${curve.kind==='interval'?`<path d="${line('upper')}" class="chart-line upper"/>`:''}${points}<text x="${left}" y="${height-14}">${esc(axisTime(start))}</text><text x="${width-right}" y="${height-14}" text-anchor="end">${esc(axisTime(end))}</text></svg><div class="inspection-chart-legend"><span class="legend-measured">Measured</span>${curve.reference!==undefined?'<span class="legend-above">Above reference</span>':''}${curve.kind==='interval'?'<span class="legend-matched">Matched sustained rule</span><span class="legend-unqualified">Insufficient / unqualified samples</span>':''}</div><output id="inspection-chart-reading">Hover or focus a sample for its time and value.</output>`;
 const show=event=>{const point=event.target.closest('[data-chart-point]');if(point)$('#inspection-chart-reading').textContent=point.getAttribute('aria-label');};
 $('#inspection-chart').onpointerover=show;$('#inspection-chart').onfocusin=show;
}
