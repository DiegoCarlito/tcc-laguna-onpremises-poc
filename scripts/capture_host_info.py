#!/usr/bin/env python3
"""
Captura o manifesto de reprodutibilidade do hospedeiro:
SO, versão do kernel, CPU, RAM, discos (tipo/modelo), Docker, Docker Compose
e digests imutáveis das imagens utilizadas.
"""
import argparse
import datetime
import json
import os
import platform
import subprocess


def run_cmd(cmd: list[str]) -> str:
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=15)
        return out.strip()
    except Exception as exc:
        return f"unavailable ({exc})"


def main() -> int:
    parser = argparse.ArgumentParser(description="Gera manifesto de reprodutibilidade do host")
    parser.add_argument("--output", default="results/host_manifest.json")
    args = parser.parse_args()

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    manifest = {
        "captured_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "os": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "pretty_name": run_cmd(["bash", "-c", ". /etc/os-release && echo $PRETTY_NAME"]),
        },
        "hardware": {
            "cpu_model": run_cmd(["bash", "-c", "lscpu | grep -m1 'Model name' | cut -d: -f2"]),
            "cpu_physical_cores": run_cmd([
                "bash", "-c",
                "lscpu | awk -F: '/^Core\\(s\\) per socket:/ {c=$2} /^Socket\\(s\\):/ {s=$2} END {gsub(/ /,\"\",c); gsub(/ /,\"\",s); print c*s}'"
            ]),
            "cpu_logical_cores": os.cpu_count(),
            "memory_total": run_cmd(["bash", "-c", "free -h | awk '/^Mem:/ {print $2}'"]),
            "workspace_filesystem": run_cmd(["df", "-Th", repo_root]),
            "block_devices": run_cmd(["lsblk", "-d", "-e", "7", "-o", "NAME,SIZE,ROTA,TRAN,MODEL"]),
        },
        "runtime": {
            "docker_version": run_cmd(["docker", "--version"]),
            "compose_version": run_cmd(["docker", "compose", "version"]),
            "git_commit": run_cmd(["git", "-C", repo_root, "rev-parse", "HEAD"]),
            "image_digests": [
                line for line in run_cmd([
                    "docker", "images", "--digests",
                    "--format", "{{.Repository}}:{{.Tag}} {{.Digest}}"
                ]).splitlines()
                if any(k in line for k in ("spark", "seaweedfs", "laguna"))
            ],
        },
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"[host-manifest] Salvo em {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
