import asyncio
import websockets
import json
import os

async def collect_vessels():
    api_key = os.getenv("AISSTREAM_KEY")
    if not api_key:
        print("❌ ERREUR : La clé AISSTREAM_KEY est introuvable dans les Secrets GitHub.")
        return

    # Zone élargie pour maximiser les chances (Golfe de Guinée étendu)
    bbox = [[-10.0, 0.0], [10.0, 20.0]] 
    vessels = []
    
    print(f"📡 Connexion à AISStream avec la clé : {api_key[:5]}***")
    
    try:
        async with websockets.connect("wss://stream.aisstream.io/v0/stream", open_timeout=20) as websocket:
            subscribe_message = {
                "APIKey": api_key,
                "BoundingBoxes": [bbox]
                # On retire temporairement le filtre ShipType pour voir si on capte N'IMPORTE QUOI
            }
            await websocket.send(json.dumps(subscribe_message))
            print("✅ Requête envoyée, écoute du flux pendant 40 secondes...")
            
            start_time = asyncio.get_event_loop().time()
            # On écoute plus longtemps (40s) pour laisser le temps aux satellites de passer
            while asyncio.get_event_loop().time() - start_time < 40:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    
                    if "MetaData" in data and "PositionReport" in data["Message"]:
                        meta = data["MetaData"]
                        pos = data["Message"]["PositionReport"]
                        ship_name = meta.get("ShipName", "").strip() or "Navire Inconnu"
                        
                        vessels.append({
                            "mmsi": meta.get("MMSI"),
                            "name": ship_name,
                            "lat": pos["Latitude"],
                            "lon": pos["Longitude"],
                            "tooltip_title": f"🚢 {ship_name}",
                            "tooltip_content": f"MMSI: {meta.get('MMSI')}<br/>Vitesse: {pos.get('Sog', 'N/A')} nœuds"
                        })
                        if len(vessels) % 5 == 0:
                            print(f"📥 {len(vessels)} navires collectés jusqu'à présent...")
                except asyncio.TimeoutError:
                    print("... en attente de données ...")
                    continue
                except Exception as e:
                    print(f"⚠️ Erreur message : {e}")
        
        # Sauvegarde et dédoublonnage
        if vessels:
            unique_vessels = list({v['mmsi']: v for v in vessels}.values())
            with open("vessels.json", "w", encoding="utf-8") as f:
                json.dump(unique_vessels, f, indent=4)
            print(f"💾 Succès : {len(unique_vessels)} navires uniques sauvegardés dans vessels.json")
        else:
            print("⚠️ Aucun navire n'a été capté durant la session.")
            # On crée un fichier avec un message pour éviter le fichier vide
            with open("vessels.json", "w") as f:
                json.dump([{"name": "Système en attente", "lat": 0, "lon": 0}], f)

    except Exception as e:
        print(f"❌ Erreur critique de connexion : {e}")

if __name__ == "__main__":
    asyncio.run(collect_vessels())
