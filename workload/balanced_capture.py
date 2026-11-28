"""Bounded, paced browse/cart/checkout traffic for independently checked captures.

Twelve workers, average five journeys/s after a gradual 120s warm-up. Each
epoch-aligned five-second interval starts 25 journeys over 2.8s, then drains.
Every journey checks browse, one-item cart addition, and synthetic checkout.
This is separate from the unchanged five-user overhead benchmark.
"""
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
from pathlib import Path
import random
import secrets
import threading
import time

import requests

RUN=os.environ["CCT_RUN_ID"]
MODE=os.environ["CCT_CAPTURE_MODE"]
if MODE not in ("healthy","fault"):raise ValueError("unknown capture mode")
RESULTS=Path("/results")
HOST="http://frontend"
PRODUCTS=("0PUK6V6EV0","1YMWWN1N4O","2ZYFJ3GM2N","66VCHSJNUP","6E92ZMYYFZ","9SIQT8TOJO","L9ECAV7KIM","LS4PSXUNUM","OLJCESPC7Z")
randomizer=random.Random(42)
local=threading.local();lock=threading.Lock();stop=threading.Event()
state={"phase":"warmup","run_id":RUN,"mode":MODE,"requests":0,"failures":0,"journeys":0,
       "worker_errors":0,"warmup_started_epoch":time.time(),"max_start_lag_ms":0}


def emit(kind,row):
    with lock:print("CCT_"+kind+" "+json.dumps(row,separators=(",",":")),flush=True)


def save(name,row):
    temp=RESULTS/(name+".tmp");temp.write_text(json.dumps(row)+"\n");temp.replace(RESULTS/name)


def http(method,path,expected,body=None):
    trace,parent=secrets.token_hex(16),secrets.token_hex(8)
    started=time.time();tick=time.perf_counter();success=False;error_type=None;response=None
    emit("BEGIN",{"run_id":RUN,"trace_id":trace,"external_parent":parent,"method":method,"path":path,"started_epoch":started})
    try:
        response=local.session.request(method,HOST+path,data=body,
            headers={"traceparent":f"00-{trace}-{parent}-01"},allow_redirects=False,timeout=(2,8))
        success=response.status_code==expected
        if path=="/cart/checkout":success &= b"Your order is complete!" in response.content
        if path=="/cart" and method=="POST":success &= response.headers.get("Location")=="/cart"
        if path=="/":success &= b"/product/" in response.content
        if not success:error_type="UnexpectedShoppingResponse"
    except requests.RequestException as e:error_type=type(e).__name__
    event={"run_id":RUN,"phase":state["phase"],"started_epoch":started,"completed_epoch":time.time(),
           "method":method,"path":path,"latency_ms":(time.perf_counter()-tick)*1000,
           "status":response.status_code if response is not None else 0,"success":bool(success),
           "error_type":error_type,"bytes":len(response.content) if response is not None else 0,
           "trace_id":trace,"external_parent":parent}
    with lock:
        state["requests"]+=1
        if not success:state["failures"]+=1;stop.set()
    emit("EVENT",event)
    return success


def journey(index,product,scheduled):
    if stop.is_set():return
    lag=max(0,(time.monotonic()-scheduled)*1000)
    with lock:state["max_start_lag_ms"]=max(state["max_start_lag_ms"],lag)
    if lag>500:
        stop.set();emit("ERROR",{"reason":"arrival backlog exceeds 500ms","lag_ms":lag});return
    if not hasattr(local,"session"):local.session=requests.Session()
    if not http("GET","/",200):return
    if stop.is_set() or not http("POST","/cart",302,{"product_id":product,"quantity":"1"}):return
    if stop.is_set():return
    checkout={"email":"cct-validation@example.invalid","street_address":"1 Test Street","zip_code":"11794",
              "city":"Test City","state":"NY","country":"United States","credit_card_number":"4111111111111111",
              "credit_card_expiration_month":"12","credit_card_expiration_year":"2030","credit_card_cvv":"123"}
    if http("POST","/cart/checkout",200,checkout):
        with lock:state["journeys"]+=1


def next_slot(epoch,elapsed):
    count=10 if elapsed<30 else 15 if elapsed<60 else 25
    base=math.floor(epoch/5)*5+.1;step=2.8/(count-1)
    slot=max(0,math.ceil((epoch-base)/step-1e-7))
    return base+slot*step if slot<count else base+5


def main():
    start=time.monotonic();epoch_offset=time.time()-start
    next_epoch=next_slot(time.time(),0);next_arrival=next_epoch-epoch_offset
    last_progress=0;futures=[];index=0;reason="completed"
    emit("PHASE",{**state,"observed_epoch":time.time()})
    with ThreadPoolExecutor(max_workers=12) as workers:
        while True:
            now=time.monotonic();elapsed=now-start
            if state["phase"]=="warmup" and elapsed>=120:
                with lock:state.update(phase="measured",measured_started_epoch=time.time())
                emit("PHASE",{**state,"observed_epoch":time.time()})
            if stop.is_set() or (RESULTS/"stop").exists():reason="aborted";stop.set();break
            if state["phase"]=="measured":
                measured=time.time()-state["measured_started_epoch"]
                if MODE=="healthy" and measured>=300:break
                if MODE=="fault" and (RESULTS/"finish").exists():break
                if measured>=360:reason="coordinator deadline exceeded";stop.set();break
            for future in futures:
                if future.done():
                    try:future.result()
                    except Exception as e:
                        reason="worker failure";stop.set()
                        with lock:state["worker_errors"]+=1
                        emit("ERROR",{"reason":"worker failure","type":type(e).__name__})
            futures=[f for f in futures if not f.done()]
            if now>=next_arrival and len(futures)<12:
                futures.append(workers.submit(journey,index,randomizer.choice(PRODUCTS),next_arrival))
                index+=1;next_epoch=next_slot(next_epoch+.00001,elapsed);next_arrival=next_epoch-epoch_offset
            elif now-next_arrival>.5:
                reason="arrival backlog exceeds 500ms";stop.set();break
            if now-last_progress>=5:
                with lock:row={**state,"at_epoch":time.time(),"in_flight":len(futures)}
                save("progress.json",row);last_progress=now
            time.sleep(.005)
        # Requests already in flight finish under their bounded HTTP timeouts.
        for future in futures:
            try:future.result(timeout=25)
            except Exception as e:
                reason="worker failure";stop.set()
                with lock:state["worker_errors"]+=1
                emit("ERROR",{"reason":"worker failure","type":type(e).__name__})
    if state["failures"] or state["worker_errors"] or stop.is_set():reason="aborted" if reason=="completed" else reason
    with lock:state.update(phase="complete",reason=reason,ended_epoch=time.time(),target_journeys_per_second=5,workers=12,
                           arrival_schedule="25 journeys over 2.8s per epoch-aligned 5s interval, then drain")
    save("report.json",state);save("progress.json",state);emit("PHASE",state)
    return 0 if reason=="completed" else 1


if __name__=="__main__":raise SystemExit(main())
