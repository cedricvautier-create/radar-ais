import asyncio
import websockets
import json
import os

async def collect_vessels():
    api_key = os.getenv("AISSTREAM_KEY")
    # TEST : Détroit de Gibraltar (Zone ultra-fréquentée garantie)
    # Si ça marche ici, le code est bon.
    bbox = [[35.0, -7.0], [37.0, -4.0]] 
    vessels = {} 

    print(f"📡 TEST DE VALIDATION : Zone Gibraltar")
    
    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=30) as websocket:
            subscribe_message = {
                "APIKey": api_key,
                "BoundingBoxes": [bbox] # Format liste de listes
            }
            await websocket.send(json.dumps(subscribe_message))
            print("✅ Connecté. Écoute de 60 secondes...")
            
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < 60:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    
                    if "MetaData" in data:
                        meta = data["MetaData"]
                        mmsi = meta.get("MMSI")
                        # Extraction robuste de la position
                        msg = data.get("Message", {})
                        # Certains messages n'ont pas de position, on cherche dans les différents types
                        pos = msg.get("PositionReport", msg.get("StandardClassBPositionReport", {}))
                        
                        lat = pos.get("Latitude")
                        lon = pos.get("Longitude")

                        if lat and lon:
                            vessels[mmsi] = {
                                "mmsi": mmsi,
                                "name": meta.get("ShipName", "Inconnu"),
                                "lat": lat,
                                "lon": lon,
                                "tooltip_title": f"🚢 {meta.get('ShipName', 'Navire')}",
                                "tooltip_content": f"MMSI: {mmsi}"
                            }
                            print(f"📥 Capté : {meta.get('ShipName')} ({len(vessels)} au total)")
                except asyncio.TimeoutError:
                    continue
        
        output = list(vessels.values())
        with open("vessels.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4)
        print(f"💾 TEST TERMINÉ : {len(output)} navires sauvegardés.")

    except Exception as e:
        print(f"❌ ERREUR : {e}")

if __name__ == "__main__":
    asyncio.run(collect_vessels())
