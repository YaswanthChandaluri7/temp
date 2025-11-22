import uuid
import json
from pathlib import Path
from typing import Dict, Any
from os import getenv

DATA_FILE = Path('/app/data/directory.json')
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

class DirectoryMeta:
    # def __init__(self):
    #     self._store = {}  # photo_id -> metadata
    #     self.photos = {}  # photo_id -> list of replicas (used for replication helpers)
    #     if DATA_FILE.exists():
    #         try:
    #             self._store = json.loads(DATA_FILE.read_text())
    #         except Exception:
    #             self._store = {}
    def __init__(self):
        # existing metadata
        self._store = {}
        
        # replication helpers
        self.photos = {}  # photo_id -> list of replicas
        self.volumes = [{"volume": f"V{i+1}", "free": True} for i in range(10)]

        # load existing store from file
        if DATA_FILE.exists():
            try:
                self._store = json.loads(DATA_FILE.read_text())
                # populate photos from store if any
                for pid, entry in self._store.items():
                    self.photos[pid] = entry.get("replicas", [])
            except Exception:
                self._store = {}

    def _persist(self):
        DATA_FILE.write_text(json.dumps(self._store))

    def alloc_replicas(self, photo_size: int):
        # simple allocation: choose two store ids (store-service is single node here)
        photo_id = f"P{uuid.uuid4().hex[:12]}"
        # For demo, create two replica entries pointing to the same store node.
        replicas = [
            {"store_id":"store-service","volume":"V1"},
            {"store_id":"store-service","volume":"V2"}
        ]
        self._store[photo_id] = {"photo_id":photo_id,"replicas":replicas,"deleted":False}
        self._persist()
        return {"photo_id":photo_id,"replica_locations":replicas}

    def confirm_upload(self, photo_id: str, replicas: list):
        if photo_id in self._store:
            self._store[photo_id]['replicas'] = replicas
            self._persist()

    def get(self, photo_id: str):
        return self._store.get(photo_id)

    def mark_delete(self, photo_id: str):
        entry = self._store.get(photo_id)
        if not entry:
            return None
        entry['deleted'] = True
        self._persist()
        return entry

    def confirm_delete(self, photo_id: str):
        if photo_id in self._store:
            del self._store[photo_id]
            self._persist()

     # -----------------------------
    # REPLICATION HELPERS
    # -----------------------------

    def get_free_locations(self, count):
        free_vols = [v for v in self.volumes if v["free"]][:count]
        for v in free_vols:
            v["free"] = False
        return {"locations": free_vols}

    def add_replicas(self, photo_id, replicas):
        if photo_id not in self.photos:
            self.photos[photo_id] = []
        self.photos[photo_id].extend(replicas)

    def remove_half_replicas(self, photo_id):
        """
        Directory decides which replicas to remove.
        Returns list of replicas to remove.
        """
        curr_replicas = self.photos.get(photo_id, [])
        half_count = len(curr_replicas) // 2
        to_remove = curr_replicas[:half_count]
        self.photos[photo_id] = curr_replicas[half_count:]
        for r in to_remove:
            r["free"] = True
        return to_remove