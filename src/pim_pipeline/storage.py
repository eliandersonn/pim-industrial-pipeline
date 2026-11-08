"""Persistência imutável em diretório local de testes ou bucket GCS existente."""

from pathlib import Path
from urllib.parse import urlsplit


class BronzeStore:
    def __init__(self, destination):
        self.destination = destination.rstrip("/")
        self.bucket = None
        if destination.startswith("gs://"):
            from google.cloud import storage

            parsed = urlsplit(destination)
            if not parsed.netloc or parsed.query or parsed.fragment:
                raise ValueError("Destino GCS inválido")
            self.bucket = storage.Client().bucket(parsed.netloc)
            self.prefix = parsed.path.strip("/")
        else:
            self.root = Path(destination).resolve()

    def put(self, key, data, content_type="application/json"):
        """Criar uma vez; repetição só aceita o mesmo conteúdo."""
        if self.bucket is not None:
            from google.api_core.exceptions import PreconditionFailed

            name = "/".join(part for part in (self.prefix, key) if part)
            blob = self.bucket.blob(name)
            try:
                # A condição zero permite criar, mas impede substituir um objeto existente.
                blob.upload_from_string(data, content_type=content_type, if_generation_match=0, timeout=120)
            except PreconditionFailed:
                if blob.download_as_bytes(timeout=120) != data:
                    raise ValueError(f"Objeto já existe com conteúdo diferente: {key}") from None
            return f"gs://{self.bucket.name}/{name}"
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("Objeto fora do diretório de destino")
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("xb") as stream:
                stream.write(data)
        except FileExistsError:
            if path.read_bytes() != data:
                raise ValueError(f"Arquivo já existe com conteúdo diferente: {key}") from None
        return str(path)

    def get(self, key):
        """Ler um artefato existente para retomar uma execução; None se ausente."""
        if self.bucket is not None:
            from google.api_core.exceptions import NotFound

            name = "/".join(part for part in (self.prefix, key) if part)
            try:
                return self.bucket.blob(name).download_as_bytes(timeout=120)
            except NotFound:
                return None
        path = (self.root / key).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("Objeto fora do diretório de destino")
        return path.read_bytes() if path.exists() else None
