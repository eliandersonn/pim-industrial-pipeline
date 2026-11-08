"""Empacota fontes, documentação complementar local e histórico Git, sem segredos."""

from pathlib import Path
import re
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    """Inclui arquivos versionados e, quando presente, o documento local complementar."""
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)
    if status.strip():
        raise SystemExit("Há alterações sem commit. Registre-as antes de preparar a entrega.")
    files = [ROOT / item.decode("utf-8") for item in subprocess.check_output(
        ["git", "ls-files", "-z"], cwd=ROOT).split(b"\0") if item]
    # Este documento acompanha o ZIP, mas não é publicado pelo Git.
    report = ROOT / "docs/relatorio_projeto.md"
    if report.is_file() and report not in files:
        files.append(report)
    files.extend(p for p in (ROOT / ".git").rglob("*") if p.is_file() and not p.name.endswith(".lock"))
    output = ROOT / "dist/pim-industrial.zip"
    output.parent.mkdir(exist_ok=True)
    for path in files:
        relative = path.relative_to(ROOT)
        if relative.parts[0] in {".venv", "runtime", "data", ".tools"} or relative.name == ".env":
            raise SystemExit(f"Arquivo local proibido na entrega: {relative}")
        if path.suffix in {".json", ".py", ".md", ".txt", ".yml"} or relative.as_posix() == ".git/config":
            content = path.read_bytes()
            if re.search(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\"private_key\"\s*:", content):
                raise SystemExit(f"Possível segredo em {relative}")
            if relative.as_posix() == ".git/config" and re.search(rb"extraheader|https://[^\s/]+:[^\s/]+@", content, re.I):
                raise SystemExit("Configuração Git contém autenticação; remova-a do pacote.")
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        # O Git exige esta pasta mesmo quando as referências estão em packed-refs.
        archive.writestr(".git/refs/", b"")
        for path in sorted(files):
            archive.write(path, path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(output) as archive:
        if archive.testzip() is not None or not {".git/HEAD", ".git/refs/"} <= set(archive.namelist()):
            raise SystemExit("ZIP inválido ou sem histórico Git")
        if report.is_file() and archive.read("docs/relatorio_projeto.md") != report.read_bytes():
            raise SystemExit("Documento complementar ausente ou divergente no ZIP")
    print(output)
    print(f"Arquivos: {len(files)}; bytes: {output.stat().st_size}")


if __name__ == "__main__":
    main()
