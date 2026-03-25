# data_engineering/ingestion/ais_telemetry_stream.py
import asyncio
import websockets
import json
import os
from datetime import datetime

import dotenv
dotenv.load_dotenv()

class GulfOfGuineaAISStreamer:
    """
    Subscribes to aisstream.io to monitor real-time vessel traffic
    in the Gulf of Guinea and Lekki Port approaches.

    The service is free; you need a free API key from https://aisstream.io/authenticate
    (sign in with GitHub), then create a key at https://aisstream.io/apikeys.
    Set it in the environment: export AIS_STREAM_API_KEY=your_key
    """
    def __init__(self, api_key: str, storage_dir: str):
        self.ws_url = "wss://stream.aisstream.io/v0/stream"
        self.api_key = api_key
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.file_path = os.path.join(self.storage_dir, "maritime_telemetry.jsonl")
        
        # Bounding box covering the Nigerian Coastline and Gulf of Guinea
        # Format: [[Lat_Min, Lon_Min], [Lat_Max, Lon_Max]]
        self.bounding_box = [[2.0, 2.0], [7.5, 9.5]] 

    async def stream_data(self):
        # Longer open_timeout: handshake often exceeds default 10s (slow server/network).
        # Retry connect up to 3 times on timeout or connection error.
        subscription = {
            "APIKey": self.api_key,
            "BoundingBoxes": [self.bounding_box],
            "FilterMessageTypes": ["PositionReport"],
        }
        last_error = None
        for attempt in range(3):
            try:
                async with websockets.connect(
                    self.ws_url,
                    open_timeout=60,
                    close_timeout=10,
                ) as websocket:
                    await websocket.send(json.dumps(subscription))
                    print(f"Connected to AIS Stream. Monitoring bounding box: {self.bounding_box}")
                    event_buffer = []
                    recv_timeout = 300.0  # 5 min; no traffic in box can be normal
                    while True:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=recv_timeout)
                            data = json.loads(message)
                            if data.get("MessageType") == "PositionReport":
                                msg_info = data.get("Message", {}).get("PositionReport", {})
                                mmsi = data.get("MetaData", {}).get("MMSI")
                                ship_name = data.get("MetaData", {}).get("ShipName", "UNKNOWN")
                                record = {
                                    "timestamp": datetime.utcnow().isoformat(),
                                    "mmsi": mmsi,
                                    "ship_name": ship_name.strip(),
                                    "latitude": msg_info.get("Latitude"),
                                    "longitude": msg_info.get("Longitude"),
                                    "speed_over_ground": msg_info.get("Cog"),
                                    "navigational_status": msg_info.get("NavigationalStatus"),
                                }
                                event_buffer.append(record)
                                if len(event_buffer) >= 50:
                                    self._flush_to_disk(event_buffer)
                                    event_buffer.clear()
                        except asyncio.TimeoutError:
                            # No message in recv_timeout s; bounding box may have no traffic — keep listening
                            print(f"No AIS messages in {int(recv_timeout)}s (box may have no traffic). Still listening...")
                            continue
                        except Exception as e:
                            print(f"WebSocket error: {e}")
                            break
                    return
            except TimeoutError as e:
                last_error = e
                print(f"Handshake timeout (attempt {attempt + 1}/3). Retrying in 5s...")
                await asyncio.sleep(5)
            except OSError as e:
                last_error = e
                print(f"Connect error (attempt {attempt + 1}/3): {e}. Retrying in 5s...")
                await asyncio.sleep(5)
        print("Could not connect after 3 attempts. Check network/firewall and try again later.")
        raise last_error
                    
    def _flush_to_disk(self, buffer: list):
        # Append JSON lines to a local file for later vectorization
        with open(self.file_path, "a") as f:
            for item in buffer:
                f.write(json.dumps(item) + "\n")
        print(f"Flushed {len(buffer)} AIS position records to storage.")
        

if __name__ == "__main__":
    api_key = os.getenv("AIS_STREAM_API_KEY")
    if not api_key:
        print("AIS_STREAM_API_KEY is not set. The stream is free but requires a free API key.")
        print("  1. Sign in: https://aisstream.io/authenticate (e.g. with GitHub)")
        print("  2. Create an API key: https://aisstream.io/apikeys")
        print("  3. Run: export AIS_STREAM_API_KEY=your_key")
        raise SystemExit(1)
    streamer = GulfOfGuineaAISStreamer(api_key=api_key, storage_dir="data_engineering/ingestion/data")
    asyncio.run(streamer.stream_data())