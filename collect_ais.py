import asyncio
import websockets
import json
import os

async def collect_vessels():
    # Récupération de la clé API depuis les variables d'environnement de GitHub
    api_key = os.getenv("AISSTREAM_KEY")
    
    # Zone : Golfe de Guinée (Cameroun, Gabon, Congo)
    bbox = [[-6.0, 5.0], [6.0, 15.0]] 
    vessels = []
    
    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=10) as websocket:
            subscribe_message = {
                "APIKey": api_key,
                "BoundingBoxes": [bbox],
                "FiltersShipType": [80, 81, 82, 89] # Focus Pétroliers
            }
            await websocket.send(json.dumps(subscribe_message))
            
            # On écoute pendant 20 secondes pour capter un maximum de navires
            start_time = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start_time < 20:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    data = json.loads(message)
                    if "MetaData" in data:
                        meta = data["MetaData"]
                        pos = data["Message"]["PositionReport"]
                        vessels.append({
                            "mmsi": meta.get("MMSI"),
                            "name": meta.get("ShipName").strip(),
                            "lat": pos["Latitude"],
                            "lon": pos["Longitude"],
                            "tooltip_title": f"🚢 {meta.get('ShipName')}",
                            "tooltip_content": f"MMSI: {meta.get('MMSI')}<br/>Destination: {meta.get('destination', 'N/A')}"
                        })
                except Exception:
                    continue
        
        # Sauvegarde au format JSON (nettoyage des doublons)
        df_final = list({v['mmsi']: v for v in vessels}.values())
        with open("vessels.json", "w", encoding="utf-8") as f:
            json.dump(df_final, f, indent=4)
        print(f"✅ {len(df_final)} navires collectés.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    asyncio.run(collect_vessels())