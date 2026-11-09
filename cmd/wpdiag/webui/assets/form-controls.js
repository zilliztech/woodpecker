/* Editable recent values. Each field has its own bounded, local history. */
'use strict';
(()=>{
 const fields=new Set(['metadata.etcd','metadata.prefix','metadata.bucket','metadata.root_path','metrics_endpoint','trace_endpoint','log_namespace','log_id','log_name','segment_id','reader_name','node_id','path','trace_id','request_id']);
 let owner=null,menu=null,values=[],active=-1,serial=0,filtering=false,requestSerial=0,candidates={};
 const sourceField=field=>['metrics_endpoint','trace_endpoint'].includes(field);
 function history(field){return (candidates[field]||[]).slice(0,5);}
 async function forget(index){
  const input=owner,value=values[index],filter=filtering;if(!input||!value)return;
  try{const response=await api('/api/workspace/candidates',{key:input.dataset.recentField,value});candidates=response.values||{};if(document.activeElement===input)await show(input,filter);}catch(e){error(e);}
 }
 function close(){
  requestSerial++;
  if(owner){owner.setAttribute('aria-expanded','false');owner.removeAttribute('aria-activedescendant');owner.removeAttribute('aria-controls');}
  menu?.remove();menu=null;owner=null;values=[];active=-1;
 }
 function highlight(index){
  if(!values.length)return;
  active=(index+values.length)%values.length;
  for(const [i,option]of [...menu.querySelectorAll('[role=option]')].entries())option.setAttribute('aria-selected',String(i===active));
  owner.setAttribute('aria-activedescendant',menu.querySelectorAll('[role=option]')[active].id);
 }
 function choose(index){
  const input=owner,value=values[index];if(!input||!value)return;
  close();input.value=value;input.dispatchEvent(new Event('input',{bubbles:true}));input.dispatchEvent(new Event('change',{bubbles:true}));input.focus();close();
 }
 async function show(input,filter=false){
  const request=++requestSerial;
  try{const data=await api('/api/workspace/candidates');if(request!==requestSerial||document.activeElement!==input)return;candidates=data.values||{};}catch{if(request!==requestSerial)return;candidates={};}

  if(!state.info||input.disabled||input.readOnly||input.closest('details:not([open])')||!fields.has(input.dataset.recentField))return;
  input.scrollIntoView({block:'nearest',inline:'nearest',behavior:'instant'});
  close();owner=input;filtering=filter;
  input.setAttribute('aria-description','Use arrow keys to browse, Enter to fill, and Delete to remove a highlighted history item.');input.setAttribute('role','combobox');input.setAttribute('aria-autocomplete','list');input.setAttribute('aria-haspopup','listbox');input.setAttribute('aria-expanded','true');
  const recent=history(input.dataset.recentField).map(item=>item.value);
  values=filter?recent.filter(value=>value.toLowerCase().includes(input.value.trim().toLowerCase())):recent;
  menu=document.createElement('div');menu.className='recent-values';menu.id='recent-values-'+(++serial);menu.setAttribute('role','listbox');menu.setAttribute('aria-label','Recent values');menu.setAttribute('popover','manual');input.setAttribute('aria-controls',menu.id);
  const heading=document.createElement('div');heading.className='recent-values-heading';heading.setAttribute('role','presentation');heading.textContent=sourceField(input.dataset.recentField)?'SUCCESSFUL CONNECTIONS · LAST 5':'SUCCESSFUL INPUTS · LAST 5';menu.append(heading);
  for(const [index,value]of values.entries()){
   const option=document.createElement('div');option.className='recent-value';option.id=menu.id+'-'+index;option.setAttribute('role','option');option.setAttribute('aria-selected','false');option.setAttribute('aria-label',value);option.title=value;const text=document.createElement('span');text.className='recent-value-text';text.textContent=value;option.append(text);const remove=document.createElement('button');remove.type='button';remove.className='recent-value-remove';remove.tabIndex=-1;remove.setAttribute('aria-label','Remove '+value+' from history');remove.title='Remove from history';remove.textContent='×';remove.onpointerdown=event=>event.preventDefault();remove.onclick=event=>{event.preventDefault();event.stopPropagation();forget(index);};option.append(remove);
   option.onpointerdown=event=>event.preventDefault();option.onclick=()=>choose(index);option.onpointermove=()=>highlight(index);menu.append(option);
  }
  if(!values.length){const empty=document.createElement('p');empty.className='recent-values-empty';empty.textContent=recent.length?'No matching recent values. Keep typing to use a new value.':sourceField(input.dataset.recentField)?'No successful connections yet. Run a check to save this address.':'No successful inputs yet. Values are saved after a successful execution.';menu.append(empty);}
  document.body.append(menu);menu.showPopover?.();
  position();
 }
 function position(){
  if(!owner||!menu)return;
  const rect=owner.getBoundingClientRect();if(!owner.isConnected||!rect.width||rect.bottom<0||rect.top>innerHeight){close();return;}
  const width=Math.min(Math.max(rect.width,280),innerWidth-24);menu.style.width=width+'px';menu.style.left=Math.max(12,Math.min(rect.left,innerWidth-width-12))+'px';
  const below=innerHeight-rect.bottom-12,above=rect.top-12,height=Math.min(menu.scrollHeight,280);
  if(below>=height||below>=above){menu.style.maxHeight=Math.max(40,Math.min(below,280))+'px';menu.style.top=(rect.bottom+5)+'px';}
  else{menu.style.maxHeight=Math.max(40,Math.min(above,280))+'px';menu.style.top=Math.max(8,rect.top-5-Math.min(height,above))+'px';}
 }
 const inputFor=event=>event.target instanceof HTMLInputElement&&event.target.matches('[data-recent-field]')?event.target:null;
 document.addEventListener('focusin',event=>{const input=inputFor(event);if(input)show(input);else if(!menu?.contains(event.target))close();});
 document.addEventListener('click',event=>{const input=inputFor(event);if(input)show(input);else if(!menu?.contains(event.target))close();});
 document.addEventListener('input',event=>{const input=inputFor(event);if(input&&document.activeElement===input)show(input,true);});
 document.addEventListener('focusout',event=>{if(inputFor(event))close();});
 document.addEventListener('keydown',event=>{
  const input=inputFor(event);if(!input)return;
  if(['ArrowDown','ArrowUp'].includes(event.key)){event.preventDefault();if(owner!==input){show(input).then(()=>highlight(event.key==='ArrowDown'?0:values.length-1));return;}highlight(active<0?(event.key==='ArrowDown'?0:values.length-1):active+(event.key==='ArrowDown'?1:-1));}
  else if(event.key==='Enter'&&owner===input&&active>=0){event.preventDefault();choose(active);}
  else if(event.key==='Delete'&&owner===input&&active>=0){event.preventDefault();forget(active);}
  else if(event.key==='Escape'&&owner===input){event.preventDefault();close();}
  else if(event.key==='Tab')close();
 });
 document.addEventListener('scroll',event=>{if(menu&&!menu.contains(event.target))position();},true);
 window.addEventListener('resize',close);
})();
