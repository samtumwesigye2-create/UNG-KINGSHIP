# UNG-KINGSHIP — Command Node Revision 2

Simulation-first operational-awareness and incident-coordination platform.

## Included
- FastAPI /api/v1 contract
- Composite track simulation
- Sensor health
- Situation correlation feed
- Incident create/transition API
- SQLite audit store
- WebSocket /ws/v1/operations
- Desktop Mission Control dashboard
- UNG integration status panel
- Docker/Railway packaging

## Safety boundary
This revision performs sensing simulation, data normalization, visualization, incident coordination and audit logging only. It contains no weapon selection, intercept calculation, weapon guidance, firing, suppression actuation, or certified life-safety control.

## Run
uvicorn app.main:APP --host 0.0.0.0 --port 8000
