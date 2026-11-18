/* 마음 농원 함께하기 연결층
   농원 코드가 쓰는 claude.use('db' | 'room' | 'user') 를 Firebase 익명 로그인 + Realtime Database 로 대신합니다.
   모든 쓰기는 서버 보안 규칙이 다시 검사합니다. */
(function(){
const cfg=window.FIREBASE_CONFIG||{};
if(!window.firebase||!cfg.apiKey||!cfg.databaseURL){console.warn('Firebase를 불러오지 못해 혼자 하기 모드로 실행합니다.');return}
window.__MF_PUBLIC=1;window.__MF_SLOW=1.7;
firebase.initializeApp(cfg);
const auth=firebase.auth(),rdb=firebase.database(),TS=firebase.database.ServerValue.TIMESTAMP;
let uid=null,offset=0;
const authP=new Promise(res=>{let done=false;const fin=v=>{if(!done){done=true;res(v)}};auth.onAuthStateChanged(u=>{if(u){uid=u.uid;fin(u.uid)}});const tryIn=n=>{if(uid)return;auth.signInAnonymously().catch(e=>{console.warn('익명 로그인 다시 시도',e&&e.code);if(n>0)setTimeout(()=>tryIn(n-1),1500)})};tryIn(3);setTimeout(()=>fin(uid),9000)});
rdb.ref('.info/serverTimeOffset').on('value',s=>{offset=s.val()||0});
const ROOT='mf';
const COLS=['lots','cheers','pebbles','tree'];
const E=e=>Object.assign(new Error(String(e&&e.message||e)),{code:e&&/PERMISSION_DENIED|permission_denied/i.test(String(e.code||e.message))?'invalid_argument':'unavailable'});
const meta={fromCache:false,hasPendingWrites:false};
const toArr=(x,len)=>{if(x==null)return x;let a;if(Array.isArray(x))a=x.slice();else if(typeof x==='object'){a=[];for(const k of Object.keys(x)){const i=+k;if(Number.isInteger(i)&&i>=0&&i<200)a[i]=x[k]}}else return x;const n=len||a.length;for(let i=0;i<n;i++)if(a[i]===undefined)a[i]=null;return a};
function norm(col,v){if(!v||typeof v!=='object')return v;if(col==='lots'){v.cells=toArr(v.cells||[],9)}else if(col==='cheers'){v.toIds=toArr(v.toIds||[])||[];v.sent=toArr(v.sent||[])||[]}else if(col==='pebbles'){v.items=toArr(v.items||[])||[]}return v}
const strip=v=>{if(Array.isArray(v))return v.map(x=>x===undefined?null:strip(x));if(v&&typeof v==='object'){const o={};for(const k of Object.keys(v)){if(v[k]!==undefined&&!/[.$#\[\]\/]/.test(k)&&k!=='')o[k]=strip(v[k])}return o}return v};
const dsnap=(col,id,v)=>{const d=v==null?null:norm(col,v);return {id,exists:d!=null,data:()=>d==null?undefined:d,metadata:meta}};
function parse(path){const s=String(path).split('/');if(s[0]==='data'&&s[1]==='users'&&s.length===4)return {priv:true,owner:s[2],k:s[3],ref:()=>rdb.ref(`${ROOT}/priv/${s[2]}/${s[3]}`)};if(s.length===2&&COLS.includes(s[0])&&/^[A-Za-z0-9_\-]{1,128}$/.test(s[1]))return {col:s[0],id:s[1],ref:()=>rdb.ref(`${ROOT}/${s[0]}/${s[1]}`)};throw Object.assign(new Error('bad path'),{code:'invalid_argument'})}
function doc(path){const p=parse(path);const col=p.priv?'priv':p.col,id=p.priv?p.k:p.id;const noop=p.priv&&p.k==='probe';
 return {id,
  async get(){await authP;if(noop)return dsnap(col,id,null);try{const s=await p.ref().once('value');return dsnap(col,id,s.val())}catch(e){throw E(e)}},
  async set(d){await authP;if(noop)return;try{await p.ref().set(strip(d))}catch(e){throw E(e)}},
  async update(d){await authP;if(noop)return;try{await p.ref().update(strip(d))}catch(e){throw E(e)}},
  async delete(){await authP;if(noop)return;try{await p.ref().remove()}catch(e){throw E(e)}},
  onSnapshot(next,onErr){let alive=true,off=()=>{};authP.then(()=>{if(!alive)return;const r=p.ref(),cb=s=>{if(alive)next(dsnap(col,id,s.val()))};r.on('value',cb,e=>{onErr&&onErr(E(e))});off=()=>r.off('value',cb)});return ()=>{alive=false;off()}}}}
function query(col,m){
 const build=()=>{let q=rdb.ref(`${ROOT}/${col}`);const local=[];let server=false;for(const w of m.where){if(!server&&w[1]==='=='&&col==='lots'&&w[0]==='zone'){q=q.orderByChild('zone').equalTo(w[2]);server=true}else local.push(w)}if(!server&&m.order&&col==='pebbles'&&m.order[0]==='updatedAt'){q=q.orderByChild('updatedAt');server=true;if(m.limit&&!local.length)q=m.order[1]==='desc'?q.limitToLast(m.limit):q.limitToFirst(m.limit)}else if(!server&&!local.length&&m.limit)q=q.limitToFirst(m.limit);return {q,local}};
 const pass=(v,local)=>{for(const [k,op,val] of local){const x=v[k];if(op==='=='){if(x!==val)return false}else if(op==='in'){if(!Array.isArray(val)||!val.includes(x))return false}else if(op==='array-contains'){if(!Array.isArray(x)||!x.includes(val))return false}}return true};
 const out=(snap,local)=>{let docs=[];snap.forEach(ch=>{const v=norm(col,ch.val());if(v&&typeof v==='object'&&pass(v,local))docs.push({id:ch.key,exists:true,data:()=>v,metadata:meta})});if(m.order){const k=m.order[0],d=m.order[1]==='desc'?-1:1;docs.sort((a,b)=>((a.data()[k]||0)-(b.data()[k]||0))*d)}if(m.limit)docs=docs.slice(0,m.limit);return {docs,size:docs.length,empty:!docs.length,metadata:meta}};
 return {
  where(f,op,v){return query(col,{...m,where:m.where.concat([[f,op,v]])})},
  orderBy(f,dir){return query(col,{...m,order:[f,dir||'asc']})},
  limit(n){return query(col,{...m,limit:Math.max(1,Math.min(1000,n|0))})},
  doc(id){return doc(col+'/'+id)},
  async get(){await authP;const {q,local}=build();try{return out(await q.once('value'),local)}catch(e){throw E(e)}},
  onSnapshot(next,onErr){let alive=true,off=()=>{};authP.then(()=>{if(!alive)return;const {q,local}=build();
    if(col==='cheers'){let t=null,last='';const tick=async()=>{if(!alive)return;if(!document.hidden||!last){try{const s=out(await q.once('value'),local);const j=JSON.stringify(s.docs.map(d=>[d.id,d.data().updatedAt||0]));if(j!==last){last=j;if(alive)next(s)}}catch(e){if(!last&&onErr)onErr(E(e))}}t=setTimeout(tick,45000)};tick();off=()=>clearTimeout(t);return}
    const cb=s=>{if(alive)next(out(s,local))};q.on('value',cb,e=>{onErr&&onErr(E(e))});off=()=>q.off('value',cb)});return ()=>{alive=false;off()}}}}
const dbApi={doc,collection:col=>{if(!COLS.includes(col))throw Object.assign(new Error('bad collection'),{code:'invalid_argument'});return query(col,{where:[],order:null,limit:0})}};

/* ---------------- 함께 보기 ---------------- */
const peers=new Map();const hs=new Set();
let mine=null,myW=null,subW=null,subRef=null,myRef=null,lastJ='',lastSend=0,timer=null,connected=false,started=false;
const PKEYS=['w','x','z','ry','m','j','s','e','et','sy','sa','n','c','h','sw','y','pt','pg'];
const clean=p=>{const o={};for(const k of PKEYS){const v=p[k];if(typeof v==='number'&&isFinite(v))o[k]=Math.round(v*100)/100;else if(typeof v==='string')o[k]=v.slice(0,k==='sy'?30:k==='n'?8:8)}return o};
const list=()=>{const a=[{peer:uid||'me',isMe:true,kind:'viewer',presence:mine||{}}];for(const [id,v] of peers)a.push({peer:id,isMe:false,kind:'viewer',presence:v});return a};
let nt=0;const notify=()=>{if(nt)return;nt=setTimeout(()=>{nt=0;const l=list();hs.forEach(h=>{try{h(l)}catch(e){}})},60)};
function subscribe(w){if(subW===w)return;if(subRef){subRef.off()}peers.clear();subW=w;subRef=rdb.ref(`${ROOT}/pres/${w}`);
 const fresh=v=>v&&typeof v==='object'&&typeof v.x==='number'&&(!v.t||Date.now()+offset-v.t<10*60*1000);
 const put=s=>{if(s.key===uid)return;const v=s.val();if(fresh(v)){peers.set(s.key,Object.freeze(v))}else peers.delete(s.key);notify()};
 subRef.on('child_added',put);subRef.on('child_changed',put);subRef.on('child_removed',s=>{peers.delete(s.key);notify()});notify()}
function flush(){timer=null;if(!uid||!mine||!connected)return;const w=String(myW||'farm').slice(0,8);const ref=rdb.ref(`${ROOT}/pres/${w}/${uid}`);if(myRef&&myRef.toString()!==ref.toString()){myRef.onDisconnect().cancel();myRef.remove().catch(()=>{});lastJ=''}if(!myRef||myRef.toString()!==ref.toString()){myRef=ref;ref.onDisconnect().remove()}const c=clean(mine);const j=JSON.stringify(c);if(j===lastJ)return;lastJ=j;lastSend=Date.now();ref.set(Object.assign(c,{t:TS})).catch(e=>console.warn('presence',e&&e.code))}
function schedule(){if(timer)return;timer=setTimeout(flush,Math.max(0,1500-(Date.now()-lastSend)))}
function start(){if(started)return;started=true;authP.then(id=>{if(!id)return;rdb.ref('.info/connected').on('value',s=>{connected=s.val()===true;if(connected){lastJ='';if(myRef)myRef.onDisconnect().remove();schedule()}})})}
const mkRoom=()=>({peers:list,onlineCount:()=>peers.size+1,
 async presence(p){if(!p)return;start();await authP;if(!uid)return;const w=typeof p.w==='string'?p.w:'farm';if(typeof p.x!=='number'||typeof p.z!=='number'){if(w!==myW){myW=w;subscribe(String(w).slice(0,8))}return}mine=Object.assign({},p);myW=w;subscribe(String(w).slice(0,8));schedule()},
 onPeers(h){hs.add(h);start();return ()=>hs.delete(h)},async join(){return mkRoom()},async leave(){},async emit(){},on(){return ()=>{}}});
const room=mkRoom();
let adminP=null;
const user={async id(){return authP},async isOwner(){const id=await authP;if(!id)return false;if(!adminP)adminP=rdb.ref(`${ROOT}/admins/${id}`).once('value').then(s=>s.val()===true).catch(()=>false);return adminP},async canEdit(){return user.isOwner()},async can(){return !!(await authP)}};
window.MF_FB={uid:()=>uid};
window.claude={use:async n=>n==='db'?dbApi:n==='room'?room:n==='user'?user:null};
})();
