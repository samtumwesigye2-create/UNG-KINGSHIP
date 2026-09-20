from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime, timezone
import asyncio, random, sqlite3, json
from pathlib import Path

APP=FastAPI(title="UNG-KINGSHIP Command Node R3",version="3.0")
DB=Path("/tmp/ung_kingship_r3.db"); clients=set(); SIM={"paused":False,"speed":3.0}
def now(): return datetime.now(timezone.utc).isoformat()
def q(sql,args=(),fetch=False):
    with sqlite3.connect(DB) as c:
        c.row_factory=sqlite3.Row; cur=c.execute(sql,args); rows=[dict(r) for r in cur.fetchall()] if fetch else []; c.commit(); return rows
def init_db():
    q("CREATE TABLE IF NOT EXISTS incidents(id TEXT PRIMARY KEY,title TEXT,status TEXT,severity TEXT,facility_id TEXT,owner TEXT,notes TEXT,created_at TEXT,updated_at TEXT)")
    q("CREATE TABLE IF NOT EXISTS audit(id TEXT PRIMARY KEY,ts TEXT,action TEXT,object_type TEXT,object_id TEXT,actor TEXT,details TEXT)")
    q("CREATE TABLE IF NOT EXISTS track_history(id INTEGER PRIMARY KEY AUTOINCREMENT,track_id TEXT,ts TEXT,payload TEXT)")
def audit(a,t,i,actor="system",details=""): q("INSERT INTO audit VALUES(?,?,?,?,?,?,?)",(str(uuid4()),now(),a,t,i,actor,details))
async def emit(kind,payload):
    m={"schema_version":"3.0","message_type":kind,"timestamp":now(),"correlation_id":str(uuid4()),"environment":"TRAINING","payload":payload}
    dead=[]
    for ws in list(clients):
        try: await ws.send_json(m)
        except: dead.append(ws)
    for ws in dead: clients.discard(ws)

class IncidentCreate(BaseModel): title:str; severity:str="medium"; facility_id:str="FACILITY-A01"
class Transition(BaseModel): status:str; actor:str="operator"
class Assign(BaseModel): owner:str; actor:str="operator"
class Note(BaseModel): note:str; actor:str="operator"
class Ack(BaseModel): actor:str="operator"
class SimControl(BaseModel): action:str; speed:float|None=None; event_type:str|None=None

SENSORS={
"SEN-01":{"sensor_id":"SEN-01","facility_id":"FACILITY-A01","type":"Radar","health":"HEALTHY","latency_ms":18},
"SEN-02":{"sensor_id":"SEN-02","facility_id":"FACILITY-A01","type":"Thermal","health":"HEALTHY","latency_ms":24},
"SEN-03":{"sensor_id":"SEN-03","facility_id":"FACILITY-B02","type":"Environmental","health":"DEGRADED","latency_ms":91}}
FACILITIES={"FACILITY-A01":{"facility_id":"FACILITY-A01","status":"OPERATIONAL","region":"North Sector","gateway":"ONLINE"},"FACILITY-B02":{"facility_id":"FACILITY-B02","status":"DEGRADED","region":"South Sector","gateway":"ONLINE"}}
SYSTEMS={"ORION":"ONLINE","NEXUS":"ONLINE","PULSAR":"ONLINE","JANUS":"ONLINE","VAULT":"ONLINE","NEMSIS":"ONLINE","SENTINEL":"ONLINE"}
TRACKS={}; SITUATIONS={}

@APP.on_event("startup")
async def startup(): init_db(); asyncio.create_task(simulator())
@APP.get("/api/v1/health")
async def health(): return {"status":"ok","version":"3.0","environment":"TRAINING","simulation":SIM,"timestamp":now()}
@APP.get("/api/v1/health/dependencies")
async def deps(): return {"database":"ONLINE","websocket":"ONLINE","systems":SYSTEMS}
@APP.get("/api/v1/tracks")
async def tracks(): return list(TRACKS.values())
@APP.get("/api/v1/tracks/{tid}")
async def track(tid:str):
    if tid not in TRACKS: raise HTTPException(404,"track not found")
    return TRACKS[tid]
@APP.get("/api/v1/tracks/{tid}/history")
async def history(tid:str): return q("SELECT ts,payload FROM track_history WHERE track_id=? ORDER BY id DESC LIMIT 100",(tid,),True)
@APP.get("/api/v1/sensors")
async def sensors(): return list(SENSORS.values())
@APP.get("/api/v1/sensors/{sid}")
async def sensor(sid:str):
    if sid not in SENSORS: raise HTTPException(404,"sensor not found")
    return SENSORS[sid]
@APP.get("/api/v1/sensors/{sid}/health")
async def sensor_health(sid:str):
    s=await sensor(sid); return {"sensor_id":sid,"health":s["health"],"latency_ms":s["latency_ms"],"timestamp":now()}
@APP.get("/api/v1/facilities")
async def facilities(): return list(FACILITIES.values())
@APP.get("/api/v1/facilities/{fid}")
async def facility(fid:str):
    if fid not in FACILITIES: raise HTTPException(404,"facility not found")
    return FACILITIES[fid]
@APP.get("/api/v1/systems")
async def systems(): return SYSTEMS
@APP.get("/api/v1/situations")
async def situations(): return list(SITUATIONS.values())
@APP.get("/api/v1/situations/{sid}")
async def situation(sid:str):
    if sid not in SITUATIONS: raise HTTPException(404,"situation not found")
    return SITUATIONS[sid]
@APP.post("/api/v1/situations/{sid}/acknowledge")
async def ack(sid:str,b:Ack):
    s=await situation(sid); s["status"]="ACKNOWLEDGED"; s["acknowledged_by"]=b.actor; audit("SITUATION_ACK","situation",sid,b.actor); await emit("SITUATION_UPDATED",s); return s
@APP.get("/api/v1/incidents")
async def incidents(): return q("SELECT * FROM incidents ORDER BY updated_at DESC",fetch=True)
@APP.get("/api/v1/incidents/{iid}")
async def incident(iid:str):
    r=q("SELECT * FROM incidents WHERE id=?",(iid,),True)
    if not r: raise HTTPException(404,"incident not found")
    return r[0]
@APP.post("/api/v1/incidents")
async def create_incident(b:IncidentCreate):
    iid="INC-"+str(random.randint(1000,9999)); ts=now(); q("INSERT INTO incidents VALUES(?,?,?,?,?,?,?,?,?)",(iid,b.title,"DETECTED",b.severity,b.facility_id,"Unassigned","",ts,ts)); audit("INCIDENT_CREATED","incident",iid); o=await incident(iid); await emit("INCIDENT_CREATED",o); return o
@APP.post("/api/v1/incidents/{iid}/transition")
async def transition(iid:str,b:Transition):
    await incident(iid); q("UPDATE incidents SET status=?,updated_at=? WHERE id=?",(b.status,now(),iid)); audit("INCIDENT_TRANSITION","incident",iid,b.actor,b.status); o=await incident(iid); await emit("INCIDENT_UPDATED",o); return o
@APP.post("/api/v1/incidents/{iid}/assign")
async def assign(iid:str,b:Assign):
    await incident(iid); q("UPDATE incidents SET owner=?,updated_at=? WHERE id=?",(b.owner,now(),iid)); audit("INCIDENT_ASSIGN","incident",iid,b.actor,b.owner); o=await incident(iid); await emit("INCIDENT_UPDATED",o); return o
@APP.post("/api/v1/incidents/{iid}/note")
async def note(iid:str,b:Note):
    o=await incident(iid); notes=(o["notes"]+"\n" if o["notes"] else "")+f"{now()} {b.actor}: {b.note}"; q("UPDATE incidents SET notes=?,updated_at=? WHERE id=?",(notes,now(),iid)); audit("INCIDENT_NOTE","incident",iid,b.actor,b.note); return await incident(iid)
@APP.get("/api/v1/audit")
async def audits(): return q("SELECT * FROM audit ORDER BY ts DESC LIMIT 300",fetch=True)
@APP.get("/api/v1/replay/{iid}")
async def replay(iid:str): return {"incident":await incident(iid),"audit":q("SELECT * FROM audit WHERE object_id=? ORDER BY ts",(iid,),True)}
@APP.post("/api/v1/simulation/control")
async def sim_control(b:SimControl):
    a=b.action.upper()
    if a=="PAUSE": SIM["paused"]=True
    elif a=="RESUME": SIM["paused"]=False
    elif a=="SPEED": SIM["speed"]=max(.5,min(10,b.speed or 3))
    elif a=="RESET": TRACKS.clear(); SITUATIONS.clear()
    elif a=="INJECT": await emit("TRAINING_EVENT",{"event_type":b.event_type or "SENSOR_OUTAGE","timestamp":now()})
    else: raise HTTPException(400,"unsupported action")
    audit("SIMULATION_CONTROL","simulation","TRAINING","operator",a); return SIM
@APP.websocket("/ws/v1/operations")
async def ws(ws:WebSocket):
    await ws.accept(); clients.add(ws)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect: clients.discard(ws)

async def simulator():
    await asyncio.sleep(1); n=0
    while True:
        if SIM["paused"]: await asyncio.sleep(.5); continue
        n+=1; tid=f"TRK-{(n%9)+1:04d}"
        t={"track_id":tid,"state":random.choice(["TENTATIVE","CONFIRMED","CONFIRMED","DEGRADED"]),"classification":random.choice(["UNKNOWN","AIRBORNE OBJECT","GROUND OBJECT"]),"confidence":random.randint(55,98),"track_quality":random.choice(["GOOD","GOOD","FAIR"]),"contributing_sources":random.sample(list(SENSORS),random.randint(1,3)),"last_seen":now(),"position":{"x":random.randint(8,92),"y":random.randint(10,88)}}
        TRACKS[tid]=t; q("INSERT INTO track_history(track_id,ts,payload) VALUES(?,?,?)",(tid,now(),json.dumps(t))); await emit("TRACK_UPDATED",t)
        if n%4==0:
            sid=f"SIT-{n//4:04d}"; s={"situation_id":sid,"status":"NEW","severity":random.choice(["low","medium","high"]),"summary":"Correlated multi-source operational event","track_id":tid,"created_at":now()}; SITUATIONS[sid]=s; await emit("SITUATION_CREATED",s)
        if n%6==0:
            s=random.choice(list(SENSORS.values())); s["latency_ms"]=random.randint(12,140); s["health"]="DEGRADED" if s["latency_ms"]>100 else "HEALTHY"; await emit("SENSOR_HEALTH_CHANGED",s)
        await asyncio.sleep(SIM["speed"])

APP.mount("/static",StaticFiles(directory="app/static"),name="static")
@APP.get("/",response_class=HTMLResponse)
async def home(): return Path("app/static/index.html").read_text()
