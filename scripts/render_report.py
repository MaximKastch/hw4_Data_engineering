"""Render a view of actual experiment evidence; Chrome/Edge adds PNG and PDF."""

import argparse
import html
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def escape(value):
    return html.escape(str(value))


def log_excerpt(run):
    selected = {"became_leader", "restored", "published", "publish_rejected"}
    rows = []
    for identity in ("A", "B"):
        for line in (ROOT / "logs" / run / f"{identity}.jsonl").read_text(encoding="utf-8").splitlines():
            record = json.loads(line)
            if record["event"] in selected:
                rows.append(record)
    lines = []
    for row in sorted(rows, key=lambda value: value["time_ns"]):
        fields = " ".join(f"{key}={row[key]}" for key in
                          ("epoch", "next_index", "result_version", "error") if key in row)
        lines.append(f"{row['timestamp'][11:23]} {row['coordinator']} {row['event']} {fields}")
    return escape("\n".join(lines))


def render(summary_path):
    runs = read_json(summary_path)
    if len(runs) != 2 or not all(all(run["checks"].values()) for run in runs):
        raise ValueError("Expected two successful experiments")
    ensemble = read_json(ROOT / "evidence/ensemble/summary.json")
    manifest = read_json(ROOT / "data/manifest.json")
    if not ensemble["all_checks_passed"]:
        raise ValueError("Ensemble verification did not pass")
    modes = {bool(run["fencing"]): run for run in runs}
    rows = []
    for label, key in (("Смена лидера, с", "failover_seconds"),
                       ("Восстановлено строк из чекпоинта", "restored_index"),
                       ("Успешных публикаций", "published_count"),
                       ("Отклонённых публикаций", "rejected_count"),
                       ("Версия /result", "result_version"),
                       ("Владелец /result", "result_owner")):
        values = [modes[mode][key] for mode in (False, True)]
        if key == "failover_seconds":
            values = [f"{value:.3f}" for value in values]
        rows.append("<tr><td>" + label + "</td>" + "".join("<td>" + escape(v) + "</td>" for v in values) + "</tr>")
    cards = []
    for mode, title in ((False, "FENCING=0 · две записи"), (True, "FENCING=1 · старая эпоха отклонена")):
        run = modes[mode]
        cards.append(f"<section><h2>{title}</h2><p class='path'>{escape(run['run'])}</p>"
                     f"<pre>{log_excerpt(run['run'])}</pre></section>")
    initial = ", ".join(f"{node}: {role}" for node, role in ensemble["initial_roles"].items())
    ephemeral = ensemble["ephemeral"]
    document = f"""<!doctype html>
<html lang="ru"><meta charset="utf-8"><title>ZooKeeper — результаты экспериментов</title>
<style>
*{{box-sizing:border-box}} body{{margin:0;background:#eef2f6;color:#172539;font:18px/1.45 Arial,sans-serif}}
main{{max-width:1380px;margin:28px auto;padding:32px 42px;background:white;border-radius:12px}}
h1{{font-size:34px;line-height:1.2;margin:0 0 12px}} h2{{font-size:23px;margin:0 0 12px}}
p{{margin:8px 0}} .muted,.path{{color:#586577}} .path{{font:14px/1.4 Consolas,monospace}}
.badge{{display:inline-block;padding:6px 12px;background:#e3f4eb;color:#17653d;border-radius:6px;font-weight:bold}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:20px}} section{{padding:20px 22px;border:1px solid #dce3eb;border-radius:8px;margin-top:20px}}
table{{width:100%;border-collapse:collapse;font-size:18px}} td,th{{padding:10px 12px;border-bottom:1px solid #dce3eb;text-align:left}}
th{{background:#f1f5f9}} td:nth-child(n+2),th:nth-child(n+2){{width:23%;text-align:center}}
pre{{font:14px/1.65 Consolas,monospace;background:#152334;color:#edf4ff;padding:16px;border-radius:6px;white-space:pre-wrap;overflow-wrap:anywhere;margin:10px 0 0}}
.hash{{font:15px/1.5 Consolas,monospace;overflow-wrap:anywhere}} .small{{font-size:15px}} footer{{margin-top:20px;font-size:14px;color:#586577}}
@media print{{body{{background:white}}main{{margin:0;padding:8px;max-width:none}}section{{break-inside:avoid}}@page{{size:A4 landscape;margin:10mm}}}}
</style><main>
<h1>Apache ZooKeeper: отказ лидера и защита по эпохе</h1>
<p class="muted">ДЗ № 4 · ZooKeeper 3.9.6 / Java 17 · три сервера · сессия 10 секунд</p>
<p><span class="badge">Все проверки экспериментов пройдены</span></p>
<div class="grid">
<section><h2>Кворум</h2><p>{escape(initial)}</p>
<p><b>2 из 3:</b> create → set → get выполнены.</p>
<p><b>1 из 3:</b> сервер не обслуживает запросы; запись не выполнена.</p>
<p>После восстановления подтверждённая запись сохранена.</p></section>
<section><h2>Эфемерный узел</h2><p class="path">{escape(ephemeral['path'])}</p>
<p>При открытой сессии виден второму клиенту.</p><p class="path">ephemeralOwner = {escape(ephemeral['ephemeral_owner'])}</p>
<p>После <b>quit</b> владельца: <b>Node does not exist</b>.</p></section>
</div>
<section><h2>Настоящая пауза контейнера A: около 40 секунд</h2>
<table><tr><th>Наблюдение</th><th>Без защиты</th><th>С защитой</th></tr>{''.join(rows)}</table>
<p class="small muted">Смена лидера: от события Docker pause до записи became_leader координатора B; часы одной Linux VM.</p></section>
<div class="grid">{''.join(cards)}</div>
<section><h2>Результаты совпали с ДЗ № 3</h2>
<p><b>3 894</b> вакансии · <b>2 986</b> с навыками · <b>17 801</b> пара · <b>81 344</b> суммарная частота</p>
<p>Оба координатора в обоих режимах получили CSV с тем же SHA-256:</p>
<p class="hash">{escape(manifest['counts_csv_sha256'])}</p></section>
<footer>Снимок HTML-представления фактических журналов. В выдержках JSONL показаны время UTC, координатор, событие и его поля.
Полные доказательства: evidence/ensemble, evidence/&lt;run&gt;, logs/&lt;run&gt; и results/&lt;run&gt;.
Источник сводки: {escape(summary_path.relative_to(ROOT).as_posix())}.</footer>
</main></html>"""
    destination = ROOT / "docs/results.html"
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(document, encoding="utf-8")
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path)
    parser.add_argument("--browser", type=Path)
    parser.add_argument("--html-only", action="store_true")
    args = parser.parse_args()
    summaries = sorted((ROOT / "evidence").glob("*-summary.json"), key=lambda p: p.stat().st_mtime)
    summary = args.summary.resolve() if args.summary else summaries[-1]
    page = render(summary)
    if args.html_only:
        print(page)
        return
    candidates = [args.browser, shutil.which("google-chrome"), shutil.which("chromium"),
                  "C:/Program Files/Google/Chrome/Application/chrome.exe",
                  "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"]
    browser = next((Path(p) for p in candidates if p and Path(p).is_file()), None)
    if browser is None:
        raise SystemExit("HTML ready; use --browser PATH for PNG/PDF")
    base = [str(browser), "--headless=new", "--disable-gpu", "--disable-extensions",
            "--no-first-run", "--no-default-browser-check", "--hide-scrollbars",
            "--user-data-dir=" + str(ROOT / "runtime/report-browser")]
    options = {"capture_output": True, "timeout": 60, "check": True}
    if os.name == "nt":
        options["creationflags"] = subprocess.CREATE_NO_WINDOW
    for output_flags in (["--window-size=1500,1510", "--screenshot=" + str(ROOT / "docs/results.png")],
                         ["--no-pdf-header-footer", "--print-to-pdf=" + str(ROOT / "docs/results.pdf")]):
        subprocess.run(base + output_flags + [page.as_uri()], **options)
    for name in ("results.html", "results.png", "results.pdf"):
        path = ROOT / "docs" / name
        print(f"{path.relative_to(ROOT)}: {path.stat().st_size} bytes")


if __name__ == "__main__":
    main()
