# -*- coding: utf-8 -*-
"""Inspecciona la respuesta CRUDA de Skyscanner poll a poll: compara sortingOptions.cheapest[0]
contra el minimo real de todos los itinerarios, y sigue 3 polls despues de COMPLETE."""
import os, sys, time, json, requests
key=os.environ["SKYSCANNER_API_KEY"]; H={"Content-Type":"application/json","x-api-key":key}
base="https://partners.api.skyscanner.net/apiservices/v3/flights/live/search"
def mxn(p): 
    try: a=float(p.get("amount") or 0)
    except ValueError: return 0.0
    u=(p.get("unit") or "").upper()
    return a/1e6 if "MICRO" in u else a/1e3 if "MILLI" in u else a/100 if "CENTI" in u else a
def analizar(j):
    c=j.get("content",{}) or {}; res=c.get("results",{}) or {}; its=res.get("itineraries",{}) or {}
    so=c.get("sortingOptions",{}) or {}
    def precio_it(it):
        ps=[]
        if it.get("price"): ps.append(mxn(it["price"]))
        for po in it.get("pricingOptions",[]) or []:
            if po.get("price"): ps.append(mxn(po["price"]))
        ps=[p for p in ps if p>0]; return min(ps) if ps else None
    minreal=None; nsinprecio=0; todos=[]; con_price_top=sum(1 for it in its.values() if it.get("price"))
    for iid,it in its.items():
        p=precio_it(it)
        if p is None: nsinprecio+=1; continue
        todos.append((p,iid)); 
        if minreal is None or p<minreal: minreal=p
    todos.sort()
    def so_price(k):
        l=so.get(k) or []
        if not l: return None
        it=its.get(l[0].get("itineraryId")); return precio_it(it) if it else "ID_NO_ESTA_EN_ITINS"
    return dict(status=j.get("status"), action=j.get("action"), itins=len(its), sin_precio=nsinprecio, con_price_top=con_price_top,
                so_cheapest=so_price("cheapest"), so_best=so_price("best"), min_real=minreal,
                top3=[round(p) for p,_ in todos[:3]])
for (o,d,ida,vuelta) in [("QRO","IAH","2026-12-10","2026-12-17"),("QRO","IAH","2026-11-11","2026-11-16")]:
    q={"query":{"market":"MX","locale":"es-MX","currency":"MXN","adults":1,"cabinClass":"CABIN_CLASS_ECONOMY","queryLegs":[
      {"originPlaceId":{"iata":o},"destinationPlaceId":{"iata":d},"date":{"year":int(ida[:4]),"month":int(ida[5:7]),"day":int(ida[8:])}},
      {"originPlaceId":{"iata":d},"destinationPlaceId":{"iata":o},"date":{"year":int(vuelta[:4]),"month":int(vuelta[5:7]),"day":int(vuelta[8:])}}]}}
    print(f"\n##### {o}-{d} {ida} -> {vuelta} #####")
    r=requests.post(base+"/create",json=q,headers=H,timeout=30); j=r.json(); tok=j["sessionToken"]
    print("create :", analizar(j))
    extra=0
    for i in range(1,16):
        time.sleep(2); p=requests.post(base+"/poll/"+tok,json={},headers=H,timeout=45); pj=p.json()
        a=analizar(pj); print(f"poll {i:2}:", a)
        if a["status"]=="RESULT_STATUS_COMPLETE":
            extra+=1
            if extra>=3: break
