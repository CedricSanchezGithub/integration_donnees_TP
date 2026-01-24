import json
import time
from datetime import datetime
from pathlib import Path


class MetricsCollector:
    def __init__(self):
        self.start_time = time.time()
        self.data = {
            "run_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "timestamp": datetime.now().isoformat(),
            "status": "RUNNING",
            "duration_seconds": 0.0,
            "metrics": {
                "rows_read": 0,
                "rows_filtered": 0,
                "rows_processed": 0,
                "dim_products_inserted": 0,
                "dim_products_closed": 0,
                "facts_loaded": 0
            },
            "anomalies": []
        }

    def set_metric(self, key: str, value: int):
        """Définit une valeur absolue pour une métrique."""
        self.data["metrics"][key] = value

    def increment_metric(self, key: str, value: int = 1):
        """Incrémente une métrique."""
        self.data["metrics"][key] = self.data["metrics"].get(key, 0) + value

    def add_anomaly(self, rule: str, description: str, sample_id: str = None):
        """Enregistre une anomalie de qualité de données."""
        self.data["anomalies"].append({
            "rule": rule,
            "description": description,
            "sample_id": sample_id
        })

    def finalize(self, success: bool = True):
        """Clôture le run (calcul durée, statut)."""
        self.data["duration_seconds"] = round(time.time() - self.start_time, 2)
        self.data["status"] = "SUCCESS" if success else "FAILED"

    def save_report(self, output_dir: Path):
        """Sauvegarde le rapport au format JSON."""
        output_dir.mkdir(parents=True, exist_ok=True)
        filename = f"report_{self.data['run_id']}.json"
        file_path = output_dir / filename

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=4, ensure_ascii=False)

        print(f"📝 Rapport de métriques sauvegardé : {file_path}")