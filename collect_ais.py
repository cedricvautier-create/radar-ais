import asyncio
import websockets
import json
import os

async def collect_vessels():
    api_key = os.getenv("AISSTREAM_KEY")
    # Zone CEMAC : On vise large (Nigeria jusqu'à Angola)
    bbox = [[-10.0, 2.0], [10.0, 15.0]] 
    vessels = {} 

    print(f"📡 DÉBUT DE LA COLLECTE (Zone CEMAC)")
    
    try:
        # On augmente le timeout de connexion pour GitHub
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=30) as websocket:
            subscribe_message = {
                "APIKey": api_key,
                "BoundingBoxes": [bbox],
                # FILTRE SUPPRIMÉ : On veut voir TOUS les bateaux pour tester
            }
            await websocket.send(json.dumps(subscribe_message))
            print("✅ Connecté. Écoute intensive de 120 secondes (2 minutes)...")
            
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < 120:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    
                    # Log de diagnostic : pour voir ce qu'on reçoit
                    msg_type = data.get("MessageType", "Unknown")
                    
                    if "MetaData" in data:
                        meta = data["MetaData"]
                        mmsi = meta.get("MMSI")
                        ship_name = meta.get("ShipName", "").strip()
                        
                        # On cherche les coordonnées soit dans le message, soit dans les métadonnées
                        lat = data["Message"].get("PositionReport", {}).get("Latitude")
                        lon = data["Message"].get("PositionReport", {}).get("Longitude")

                        if lat and lon and lat != 0:
                            vessels[mmsi] = {
                                "mmsi": mmsi,
                                "name": ship_name or f"Inconnu ({mmsi})",
                                "lat": lat,
                                "lon": lon,
                                "tooltip_title": f"🚢 {ship_name or 'Navire'}",
                                "tooltip_content": f"MMSI: {mmsi}<br/>Type: {msg_type}"
                            }
                            if len(vessels) % 5 == 0:
                                print(f"📥 {len(vessels)} navires en mémoire...")
                except asyncio.TimeoutError:
                    print("... silence satellite ...")
                    continue
        
        output = list(vessels.values())
        with open("vessels.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4)
        print(f"💾 TERMINÉ : {len(output)} navires sauvegardés.")

    except Exception as e:
        print(f"❌ ERREUR : {e}")

if __name__ == "__main__":
    asyncio.run(collect_vessels())
