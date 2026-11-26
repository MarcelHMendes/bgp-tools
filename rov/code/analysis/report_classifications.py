#!/usr/bin/env python3
import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Tuple


EXPECTED_CLASSES = [
    "ignore-roa",
    "unknown",
    "unknown-protected",
    "prefer-valid",
    "drop-invalid",
    "prefer-ignore",
    "prefer-peer/prefer-valid",
]


def resolve_input_file(name: str) -> Path:
    base = name.strip()
    if not base:
        raise ValueError("Nome do arquivo nao pode ser vazio.")

    base_path = Path(base)
    with_suffix = base_path if base_path.suffix == ".json" else base_path.with_suffix(".json")

    script_dir = Path(__file__).resolve().parent
    candidates = [
        Path.cwd() / with_suffix,
        Path.cwd() / "dump" / with_suffix.name,
        script_dir.parent / "dump" / with_suffix.name,
    ]

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    checked = "\n - ".join(str(p) for p in candidates)
    raise FileNotFoundError(
        "Arquivo nao encontrado. Verifique o nome informado.\n"
        f"Caminhos testados:\n - {checked}"
    )


def load_data(path: Path) -> Dict[str, Dict[str, str]]:
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if not isinstance(payload, dict):
        raise ValueError("Formato invalido: topo do JSON deve ser um objeto com chave por mux/cidade.")

    return payload


def count_classes(per_mux_map: Dict[str, str]) -> Tuple[Counter, Counter]:
    known = Counter({key: 0 for key in EXPECTED_CLASSES})
    extra = Counter()

    for classification in per_mux_map.values():
        label = str(classification)
        if label in known:
            known[label] += 1
        else:
            extra[label] += 1

    return known, extra


def render_report(data: Dict[str, Dict[str, str]], source: Path) -> str:
    lines = []
    lines.append(f"Arquivo: {source}")
    lines.append(f"Total de mux/cidades: {len(data)}")
    lines.append("")

    total_known = Counter({key: 0 for key in EXPECTED_CLASSES})
    total_extra = Counter()

    for mux in sorted(data.keys()):
        value = data[mux]
        if not isinstance(value, dict):
            lines.append(f"[{mux}] formato invalido (esperado objeto ASN->classificacao)")
            lines.append("")
            continue

        known, extra = count_classes(value)
        total_known.update(known)
        total_extra.update(extra)

        lines.append(f"[{mux}]")
        lines.append(f"  total_identificacoes: {sum(known.values()) + sum(extra.values())}")
        for cls in EXPECTED_CLASSES:
            lines.append(f"  {cls}: {known[cls]}")

        if extra:
            lines.append("  outras_classificacoes_encontradas:")
            for cls, qty in sorted(extra.items(), key=lambda x: (-x[1], x[0])):
                lines.append(f"    - {cls}: {qty}")
        lines.append("")

    lines.append("[RESUMO_GERAL]")
    lines.append(f"  total_identificacoes: {sum(total_known.values()) + sum(total_extra.values())}")
    for cls in EXPECTED_CLASSES:
        lines.append(f"  {cls}: {total_known[cls]}")

    if total_extra:
        lines.append("  outras_classificacoes_encontradas:")
        for cls, qty in sorted(total_extra.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"    - {cls}: {qty}")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera relatorio por mux/cidade para arquivos de experimento. "
            "Passe apenas o nome do arquivo, ex: seventh_experiment_amsterdam01"
        )
    )
    parser.add_argument(
        "arquivo",
        help="Nome base do arquivo (com ou sem .json).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        file_path = resolve_input_file(args.arquivo)
        data = load_data(file_path)
        print(render_report(data, file_path))
        return 0
    except Exception as exc:
        print(f"Erro: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())