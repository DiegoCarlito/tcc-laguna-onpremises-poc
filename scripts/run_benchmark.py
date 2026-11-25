#!/usr/bin/env python3
"""
Orquestra execuções repetidas do pipeline ETL na PoC, amostrando uso de CPU (%),
pico de memória RAM (GB e %) e I/O de disco dos contêineres via `docker stats`,
e consolidando todas as métricas em CSV para análise experimental.
"""
import argparse
import csv
import json
import os
import re
import subprocess
import threading
import time

CONTAINERS = [
    "laguna-seaweedfs",
    "laguna-spark-master",
    "laguna-spark-worker-1",
    "laguna-spark-worker-2",
    "laguna-spark-driver",
]


def parse_size_to_mib(val: str) -> float:
    m = re.match(r"([\d.]+)\s*([KMGTP]?i?B)", val.strip(), re.I)
    if not m:
        return 0.0
    num = float(m.group(1))
    unit = m.group(2).lower()
    factors = {
        "b": 1 / (1024 * 1024),
        "kb": 1e3 / (1024 * 1024),
        "kib": 1 / 1024,
        "mb": 1e6 / (1024 * 1024),
        "mib": 1.0,
        "gb": 1e9 / (1024 * 1024),
        "gib": 1024.0,
    }
    return num * factors.get(unit, 0.0)


def sample_docker_stats(stop_evt: threading.Event, samples: list) -> None:
    while not stop_evt.is_set():
        try:
            out = subprocess.check_output(
                [
                    "docker", "stats", "--no-stream",
                    "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}|{{.BlockIO}}",
                    *CONTAINERS,
                ],
                stderr=subprocess.DEVNULL,
                text=True,
                timeout=10,
            )
            total_cpu = 0.0
            total_mem_mib = 0.0
            total_blk_r_mib = 0.0
            total_blk_w_mib = 0.0
            for line in out.strip().splitlines():
                parts = line.split("|")
                if len(parts) != 5:
                    continue
                cpu_p = float(parts[1].replace("%", "").strip() or 0.0)
                mem_used = parts[2].split("/")[0].strip()
                blk_r, blk_w = [x.strip() for x in parts[4].split("/")]
                total_cpu += cpu_p
                total_mem_mib += parse_size_to_mib(mem_used)
                total_blk_r_mib += parse_size_to_mib(blk_r)
                total_blk_w_mib += parse_size_to_mib(blk_w)
            samples.append({
                "ts": time.perf_counter(),
                "cpu_pct_sum": total_cpu,
                "mem_gb": total_mem_mib / 1024.0,
                "blk_read_mb": total_blk_r_mib,
                "blk_write_mb": total_blk_w_mib,
            })
        except Exception:
            pass
        time.sleep(1.0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa protocolo de benchmark da PoC")
    parser.add_argument("--runs", type=int, default=3, help="Número de repetições independentes")
    parser.add_argument("--input-prefix", default="works/subset")
    parser.add_argument("--output-prefix", default="works/clean")
    parser.add_argument("--output", default="results/benchmark_runs.csv")
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    rows = []

    for run_id in range(1, args.runs + 1):
        print(f"[benchmark] Iniciando repetição {run_id}/{args.runs} ...")
        samples = []
        stop_evt = threading.Event()
        monitor = threading.Thread(target=sample_docker_stats, args=(stop_evt, samples), daemon=True)
        monitor.start()

        metrics_file = f"/opt/laguna/results/etl_run_{run_id}.json"
        host_metrics_file = f"results/etl_run_{run_id}.json"

        res = subprocess.run(
            [
                "docker", "compose", "exec", "-T", "spark-driver",
                "/opt/spark/bin/spark-submit",
                "/opt/laguna/jobs/etl_works.py",
                "--input-prefix", args.input_prefix,
                "--output-prefix", args.output_prefix,
                "--metrics-out", metrics_file,
            ],
            check=False,
        )

        stop_evt.set()
        monitor.join(timeout=5)

        if res.returncode != 0:
            print(f"[benchmark] ERRO na repetição {run_id} (exit={res.returncode}).")
            rows.append({"run_id": run_id, "status": "FAILED"})
            continue

        with open(host_metrics_file, encoding="utf-8") as f:
            m = json.load(f)

        peak_mem_gb = round(max((s["mem_gb"] for s in samples), default=0.0), 3)
        avg_cpu_pct = round(
            sum(s["cpu_pct_sum"] for s in samples) / len(samples), 2
        ) if samples else 0.0
        peak_cpu_pct = round(max((s["cpu_pct_sum"] for s in samples), default=0.0), 2)
        disk_io_mb = 0.0
        if len(samples) >= 2:
            disk_io_mb = max(
                0.0,
                (samples[-1]["blk_read_mb"] - samples[0]["blk_read_mb"])
                + (samples[-1]["blk_write_mb"] - samples[0]["blk_write_mb"]),
            )
        elapsed_s = m["execution_time_seconds"]
        disk_io_mb_s = round(disk_io_mb / elapsed_s, 2) if elapsed_s > 0 else 0.0

        rows.append({
            "run_id": run_id,
            "status": "OK",
            "execution_time_s": elapsed_s,
            "output_records": m["output_records"],
            "throughput_rec_s": m["throughput_records_per_second"],
            "peak_ram_gb": peak_mem_gb,
            "avg_cpu_pct": avg_cpu_pct,
            "peak_cpu_pct": peak_cpu_pct,
            "disk_io_mb_s": disk_io_mb_s,
        })

    fieldnames = [
        "run_id", "status", "execution_time_s", "output_records",
        "throughput_rec_s", "peak_ram_gb", "avg_cpu_pct", "peak_cpu_pct", "disk_io_mb_s",
    ]
    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[benchmark] Resultados consolidados em {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
