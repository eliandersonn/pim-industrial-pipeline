"""Gera somente segredos locais. Não cria recursos ou acessa GCP."""

import json
import base64
import os
from pathlib import Path
import secrets

def main():
    """Prepara .env e senha da UI uma única vez, sem exibir os segredos."""
    root = Path(__file__).resolve().parents[1]
    target = root / ".env"
    if target.exists():
        raise SystemExit(".env já existe; edite-o localmente. Nenhum arquivo foi alterado.")
    runtime = root / "runtime"
    runtime.mkdir(exist_ok=True)
    password_file = runtime / "airflow-passwords.json"
    if password_file.exists():
        raise SystemExit("Arquivo de senha já existe; não será sobrescrito.")
    password_file.write_text(json.dumps({"admin": secrets.token_urlsafe(24)}), encoding="utf-8")
    adc = Path(os.environ.get("APPDATA", str(Path.home() / ".config"))) / "gcloud/application_default_credentials.json"
    values = {"POSTGRES_PASSWORD": secrets.token_urlsafe(24),
              "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(), "AIRFLOW_JWT_SECRET": secrets.token_urlsafe(48),
              "AIRFLOW_UID": "50000", "GOOGLE_ADC_PATH": adc.as_posix(),
              "GOOGLE_CLOUD_PROJECT": "", "GCS_BRONZE_BUCKET": "", "GCS_SILVER_BUCKET": "",
              "BQ_DATASET": "pim", "BQ_LOCATION": "southamerica-east1"}
    target.write_text("\n".join(f"{key}={value}" for key, value in values.items()) + "\n", encoding="utf-8")
    print("Configuração local criada. Edite .env com os recursos autorizados; senha da UI em runtime/airflow-passwords.json.")


if __name__ == "__main__":
    main()
