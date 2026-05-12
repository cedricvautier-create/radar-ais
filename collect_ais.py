import asyncio
import websockets
import json
import os

async def collect_vessels():
    api_key = os.getenv("AISSTREAM_KEY")
    # Zone CEMAC élargie : Golfe de Guinée
    bbox = [[-10.0, 0.0], [10.0, 20.0]] 
    vessels = {} # Utilisation d'un dictionnaire pour fusionner par MMSI

    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=20) as websocket:
            subscribe_message = {
                "APIKey": api_key,
                "BoundingBoxes": [bbox],
                "FiltersShipType": [80, 81, 82, 83, 84, 89] # Uniquement les Tankers (Pétrole/Chimie)
            }
            await websocket.send(json.dumps(subscribe_message))
            
            start_time = asyncio.get_event_loop().time()
            # On écoute pendant 60 secondes pour laisser le temps aux messages complets d'arriver
            while asyncio.get_event_loop().time() - start_time < 60:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    
                    if "MetaData" in data and "PositionReport" in data["Message"]:
                        meta = data["MetaData"]
                        pos = data["Message"]["PositionReport"]
                        mmsi = meta.get("MMSI")
                        lat = pos.get("Latitude")
                        lon = pos.get("Longitude")

                        # --- FILTRAGE CRUCIAL ---
                        # On ignore Null Island (0,0) et les noms vides
                        if lat != 0 and lon != 0 and meta.get("ShipName"):
                            vessels[mmsi] = {
                                "mmsi": mmsi,
                                "name": meta.get("ShipName").strip(),
                                "lat": lat,
                                "lon": lon,
                                "tooltip_title": f"🚢 {meta.get('ShipName')}",
                                "tooltip_content": f"<b>Type:</b> Tanker Pétrolier<br/><b>Vitesse:</b> {pos.get('Sog', 0)} kts"
                            }
                except asyncio.TimeoutError:
                    continue
        
        # Sauvegarde
        output = list(vessels.values())
        if output:
            with open("vessels.json", "w", encoding="utf-8") as f:
                json.dump(output, f, indent=4)
            print(f"✅ {len(output)} pétroliers réels détectés et sauvegardés.")
        else:
            print("⚠️ Aucun pétrolier actif détecté avec un fix GPS valide.")
            # On laisse le fichier vide d'objets pour que l'app sache qu'il n'y a rien
            with open("vessels.json", "w") as f:
                json.dump([], f)

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    asyncio.run(collect_vessels())
