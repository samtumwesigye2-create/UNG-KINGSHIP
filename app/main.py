from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime, timezone
import asyncio, random, sqlite3
from pathlib import Path

APP = FastAPI(title="UNG-KINGSHIP Command Node R2", version="2.0")
DB = Path("/tmp/ung_kingship_r2.db")
clients = set()

def now(): return datetime.now(timezone.utc).isoformat()
def db_exec(sql,args=(),fetch=False):
    with sqlite3.connect(DB) as c:
        c.row_factory=sqlite3.Row; cur=c.execute(sql,args)
        rows=[dict(r) for r in cur.fetchall()] if fetch else []
        c.commit(); return rows
def init_db():
    db_exec("CREATE TABLE IF NOT EXISTS incidents(id TEXT PRIMARY KEY,title TEXT,status TEXT,severity TEXT,facility_id TEXT,owner TEXT,created_at TEXT,updated_at TEXT)")
    db_exec("CREATE TABLE IF NOT EXISTS audit(id TEXT PRIMARY KEY,ts TEXT,action TEXT,object_type TEXT,object_id TEXT,actor TEXT,details TEXT)")
def audit(a,t,i,actor="system",details=""):
    db_exec("INSERT INTO audit VALUES(?,?,?,?,?,?,?)",(str(uuid4()),now(),a,t,i,actor,details))
async def broadcast(kind,payload):
    m={"schema_version":"2.0","message_type":kind,"timestamp":now(),"correlation_id":str(uuid4()),"environment":"SIMULATION","payload":payload}
    dead=[]
    for ws in list(clients):
        try: await ws.send_json(m)
        except: dead.append(ws)
    for ws in dead: clients.discard(ws)

class IncidentCreate(BaseModel):
    title:str; severity:str="medium"; facility_id:str="FACILITY-A01"
class IncidentTransition(BaseModel):
    status:str; actor:str="operator"

SENSORS={
"SEN-01":{"sensor_id":"SEN-01","facility_id":"FACILITY-A01","type":"Radar","health":"HEALTHY","latency_ms":18},
"SEN-02":{"sensor_id":"SEN-02","facility_id":"FACILITY-A01","type":"Thermal","health":"HEALTHY","latency_ms":24},
"SEN-03":{"sensor_id":"SEN-03","facility_id":"FACILITY-B02","type":"Environmental","health":"DEGRADED","latency_ms":91}}
TRACKS={}; SITUATIONS={}

@APP.on_event("startup")
async def startup(): init_db(); asyncio.create_task(simulator())
@APP.get("/api/v1/health")
async def health(): return {"status":"ok","version":"2.0","environment":"SIMULATION","timestamp":now()}
@APP.get("/api/v1/tracks")
async def tracks(): return list(TRACKS.values())
@APP.get("/api/v1/tracks/{track_id}")
async def track(track_id:str):
    if track_id not in TRACKS: raise HTTPException(404,"track not found")
    return TRACKS[track_id]
@APP.get("/api/v1/sensors")
async def sensors(): return list(SENSORS.values())
@APP.get("/api/v1/situations")
async def situations(): return list(SITUATIONS.values())
@APP.get("/api/v1/incidents")
async def incidents(): return db_exec("SELECT * FROM incidents ORDER BY updated_at DESC",fetch=True)
@APP.post("/api/v1/incidents")
async def create_incident(body:IncidentCreate):
    iid="INC-"+str(random.randint(1000,9999)); ts=now()
    db_exec("INSERT INTO incidents VALUES(?,?,?,?,?,?,?,?)",(iid,body.title,"DETECTED",body.severity,body.facility_id,"Unassigned",ts,ts))
    audit("INCIDENT_CREATED","incident",iid)
    obj=db_exec("SELECT * FROM incidents WHERE id=?",(iid,),True)[0]
    await broadcast("INCIDENT_CREATED",obj); return obj
@APP.post("/api/v1/incidents/{iid}/transition")
async def transition(iid:str,body:IncidentTransition):
    if not db_exec("SELECT id FROM incidents WHERE id=?",(iid,),True): raise HTTPException(404,"incident not found")
    db_exec("UPDATE incidents SET status=?,updated_at=? WHERE id=?",(body.status,now(),iid))
    audit("INCIDENT_TRANSITION","incident",iid,body.actor,body.status)
    obj=db_exec("SELECT * FROM incidents WHERE id=?",(iid,),True)[0]
    await broadcast("INCIDENT_UPDATED",obj); return obj
@APP.get("/api/v1/audit")
async def audit_log(): return db_exec("SELECT * FROM audit ORDER BY ts DESC LIMIT 200",fetch=True)
@APP.websocket("/ws/v1/operations")
async def ws_ops(ws:WebSocket):
    await ws.accept(); clients.add(ws)
    try:
        while True: await ws.receive_text()
    except WebSocketDisconnect: clients.discard(ws)

async def simulator():
    await asyncio.sleep(1); n=0
    while True:
        n+=1; tid=f"TRK-{(n%7)+1:04d}"
        t={"track_id":tid,"state":random.choice(["TENTATIVE","CONFIRMED","CONFIRMED","DEGRADED"]),"classification":random.choice(["UNKNOWN","AIRBORNE OBJECT","GROUND OBJECT"]),"confidence":random.randint(55,98),"track_quality":random.choice(["GOOD","GOOD","FAIR"]),"contributing_sources":random.sample(list(SENSORS),k=random.randint(1,3)),"last_seen":now(),"position":{"x":random.randint(8,92),"y":random.randint(10,88)}}
        TRACKS[tid]=t; await broadcast("TRACK_UPDATED",t)
        if n%4==0:
            sid=f"SIT-{n//4:04d}"; s={"situation_id":sid,"status":"NEW","severity":random.choice(["low","medium","high"]),"summary":"Correlated multi-source operational event","track_id":tid,"created_at":now()}
            SITUATIONS[sid]=s; await broadcast("SITUATION_CREATED",s)
        if n%6==0:
            sensor=random.choice(list(SENSORS.values())); sensor["latency_ms"]=random.randint(12,140); sensor["health"]="DEGRADED" if sensor["latency_ms"]>100 else "HEALTHY"; await broadcast("SENSOR_HEALTH_CHANGED",sensor)
        await asyncio.sleep(3)

APP.mount("/static",StaticFiles(directory="app/static"),name="static")
@APP.get("/",response_class=HTMLResponse)
async def home(): return Path("app/static/index.html").read_text()
