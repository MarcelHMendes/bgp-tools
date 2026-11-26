"""Agrupa a classificação de cada ASN por cidade e mede a consistência entre cidades.

Uso: python3 asn_consistency.py [diretório_ou_json ...]
Para cada diretório, processa os seventh_experiment_*.json encontrados nele.
Sem argumentos, usa rov/dump.
"""
import html
import json
import sys
from collections import Counter
from datetime import datetime
from itertools import combinations
from pathlib import Path
from string import Template

DUMP_DIR = Path(__file__).resolve().parents[2] / "dump"
# Um diretório por execução, para não sobrescrever resultados anteriores.
OUTPUT_DIR = DUMP_DIR / f"by_asn_{datetime.now():%Y-%m-%d_%H-%M}"
MARKDOWN_DIR = OUTPUT_DIR / "markdown"
HTML_DIR = OUTPUT_DIR / "html"
TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"

# Rótulos do dump que devem ser tratados sob outro nome nas regras abaixo.
LABEL_ALIASES = {"prefer-peer/prefer-valid": "prefer-peer"}

# Do mais ao menos prevalente, para ASNs consistentes (aplicado após LABEL_ALIASES).
LABEL_PRIORITY = [
    "drop-invalid", "ignore-roa", "prefer-valid",
    "prefer-ignore", "prefer-peer",
    "unknown-protected", "unknown",
]

# Pares de classificações que se opõem; para adicionar uma regra, inclua um novo par.
INCONSISTENT_PAIRS = {
    frozenset(pair)
    for pair in [
        ("drop-invalid", "ignore-roa"),
        ("drop-invalid", "prefer-valid"),
        ("drop-invalid", "prefer-ignore"),
        ("drop-invalid", "prefer-peer"),
        ("ignore-roa", "prefer-valid"),
        ("ignore-roa", "prefer-peer"),
    ]
}


def group_by_asn(experiment):
    """{cidade: {asn: label}} -> {asn: [label, ...]} (uma entrada por cidade que classificou o ASN)."""
    by_asn = {}
    for asn_labels in experiment.values():
        for asn, label in asn_labels.items():
            by_asn.setdefault(asn, []).append(label)
    return by_asn


def find_conflicts(labels):
    """Retorna os pares inconsistentes presentes na lista de classificações."""
    normalized = {LABEL_ALIASES.get(label, label) for label in labels}
    return [
        tuple(sorted(pair))
        for pair in combinations(normalized, 2)
        if frozenset(pair) in INCONSISTENT_PAIRS
    ]


def analyze(by_asn):
    """Retorna {asn: [pares conflitantes]} apenas para os ASNs inconsistentes."""
    conflicts = {asn: find_conflicts(labels) for asn, labels in by_asn.items()}
    return {asn: pairs for asn, pairs in conflicts.items() if pairs}


def final_label(labels):
    """Classificação que prevalece para um ASN consistente, mantendo o rótulo original do dump."""
    def rank(label):
        normalized = LABEL_ALIASES.get(label, label)
        return LABEL_PRIORITY.index(normalized) if normalized in LABEL_PRIORITY else len(LABEL_PRIORITY)
    return min(labels, key=rank)


def summarize(by_asn, inconsistent):
    """Calcula os números usados por todos os formatos de saída."""
    total = len(by_asn)
    n_bad = len(inconsistent)
    pair_counts = Counter(pair for pairs in inconsistent.values() for pair in pairs)
    return {
        "total": total,
        "n_bad": n_bad,
        "n_ok": total - n_bad,
        "pct_ok": (total - n_bad) / total if total else 0,
        "pct_bad": n_bad / total if total else 0,
        "pairs": pair_counts.most_common(),
        "label_totals": Counter(label for labels in by_asn.values() for label in labels).most_common(),
        "final_totals": Counter(
            final_label(labels) for asn, labels in by_asn.items() if asn not in inconsistent
        ).most_common(),
        # ASN -> (pares conflitantes, contagem de rótulos), ordenado por número de ASN
        "details": [
            (asn, inconsistent[asn], Counter(by_asn[asn]).most_common())
            for asn in sorted(inconsistent, key=int)
        ],
    }


def format_counts(counts):
    return ", ".join(f"{label} ({n})" for label, n in counts)


def print_report(name, s):
    print(f"== {name} ==")
    print(f"ASNs: {s['total']}")
    print(f"Consistentes:   {s['n_ok']} ({s['pct_ok']:.2%})")
    print(f"Inconsistentes: {s['n_bad']} ({s['pct_bad']:.2%})")
    print("Pares conflitantes mais frequentes:")
    for pair, count in s["pairs"]:
        print(f"  {pair[0]} x {pair[1]}: {count} ASNs")
    print()


def md_rows(rows):
    return "\n".join(f"| {' | '.join(map(str, row))} |" for row in rows)


def html_rows(rows):
    return "".join(f"<tr>{''.join(f'<td>{html.escape(str(c))}</td>' for c in row)}</tr>" for row in rows)


def render(template_file, format_rows, name, s):
    """Preenche templates/<template_file> com os dados do resumo; format_rows monta as linhas de tabela."""
    template = Template((TEMPLATE_DIR / template_file).read_text())
    return template.substitute(
        title=html.escape(name),
        total=s["total"], n_ok=s["n_ok"], n_bad=s["n_bad"],
        pct_ok=f"{s['pct_ok']:.2%}", pct_bad=f"{s['pct_bad']:.2%}",
        pair_rows=format_rows((f"{a} x {b}", n) for (a, b), n in s["pairs"]),
        label_rows=format_rows(s["label_totals"]),
        final_rows=format_rows(s["final_totals"]),
        asn_rows=format_rows(
            (asn, "; ".join(f"{a} x {b}" for a, b in pairs), format_counts(counts))
            for asn, pairs, counts in s["details"]
        ),
    )


def save_and_report(name, by_asn):
    for directory in (MARKDOWN_DIR, HTML_DIR):
        directory.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / f"{name}_by_asn.json").write_text(json.dumps(by_asn, indent=4))
    summary = summarize(by_asn, analyze(by_asn))
    (MARKDOWN_DIR / f"{name}_report.md").write_text(render("report.md", md_rows, name, summary))
    (HTML_DIR / f"{name}_report.html").write_text(render("report.html", html_rows, name, summary))
    print_report(name, summary)


def merge(by_asn_list):
    """Junta as listas de cada ASN de todos os experimentos."""
    merged = {}
    for by_asn in by_asn_list:
        for asn, labels in by_asn.items():
            merged.setdefault(asn, []).extend(labels)
    return merged


def find_dumps(args):
    """Expande diretórios em seus seventh_experiment_*.json; arquivos são mantidos como estão."""
    paths = []
    for arg in args or [DUMP_DIR]:
        path = Path(arg)
        paths.extend(sorted(path.glob("seventh_experiment_*.json")) if path.is_dir() else [path])
    return paths


def main():
    paths = find_dumps(sys.argv[1:])
    if not paths:
        sys.exit("Nenhum seventh_experiment_*.json encontrado.")
    per_experiment = []
    for path in paths:
        by_asn = group_by_asn(json.loads(path.read_text()))
        per_experiment.append(by_asn)
        save_and_report(path.stem, by_asn)

    if len(per_experiment) > 1:
        save_and_report("all_experiments", merge(per_experiment))


if __name__ == "__main__":
    main()
