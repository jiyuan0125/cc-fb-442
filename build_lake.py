#!/usr/bin/env python3
"""渠道交易文件落仓:ods_raw -> dwd_clean -> dws_mchday。

断点续跑:load_state.json 的 done 清单为准;done 里没有、但库里已有
该 src 残留行的批视为半截批,整批删掉重来,batch_log 记 redone。
"""
import csv
import json
import os
import sqlite3
import sys
from decimal import Decimal, ROUND_HALF_UP

BASE = os.path.dirname(os.path.abspath(__file__))
FILES_DIR = os.path.join(BASE, "files")
DB_PATH = os.path.join(BASE, "lake.db")
STATE_PATH = os.path.join(BASE, "load_state.json")
LOG_PATH = os.path.join(BASE, "batch_log.csv")
NOTES_PATH = os.path.join(BASE, "lake_notes.txt")

NOTES = """\
落仓口径说明(build_lake.py)
============================

1. 笔数怎么扣
   dws_mchday.cnt 记当日成功支付笔数:pay 行计入,void 当日作废整笔不算
   (被作废的 pay 与其 void 行都不计笔数也不计金额),refund 不扣减笔数。

2. 退款记哪天
   退款记在退款实际发生的日子(即退款行所在文件的日子),当日 net 扣减。
   net = 当日成功支付金额 - 当日退款金额,以分记。
   个别原单累计退款超过支付金额的,不冲回、不摊到别日,net 允许为负。

3. 三位小数怎么取整
   chb 金额以元记,统一乘 100 转分;遇三位小数按四舍五入(ROUND_HALF_UP)
   取整到分。cha / chc 金额本就以分记整数,照抄。

4. 其他归一
   chb 时间 2026-11-03T09:12:33 统一为 2026-11-03 09:12:33(T 换空格)。
   dwd_clean.amount 全表以分记整数;pay 行 ref 留空。
   dws_mchday 一行一商户一有交易的日子:当日有成功支付或有退款即成行
   (cnt 可为 0);ods_raw 只抄数据行,表头不入仓,row_no 从 1 起。
"""


def norm_row(channel, cols):
    txn_id, mch, ty, amt, ts, ref = cols
    if channel == "chb":
        fen = int(Decimal(amt) * 100) if "." not in amt else int(
            (Decimal(amt) * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
        ts = ts.replace("T", " ")
    else:
        fen = int(amt)
    return (txn_id, mch, ty, fen, ts, ref)


def load_batch(con, path, src):
    """把一份文件写进 ods_raw 与 dwd_clean,返回数据行数。"""
    channel = src[:3]
    with open(path, newline="", encoding="utf-8") as f:
        lines = f.read().splitlines()
    rows = lines[1:]  # 跳过表头
    con.execute("DELETE FROM ods_raw WHERE src = ?", (src,))
    con.execute("DELETE FROM dwd_clean WHERE src = ?", (src,))
    con.executemany(
        "INSERT INTO ods_raw (src, row_no, raw) VALUES (?, ?, ?)",
        [(src, i + 1, raw) for i, raw in enumerate(rows)],
    )
    con.executemany(
        "INSERT INTO dwd_clean (src, txn_id, mch, type, amount, ts, ref)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(src,) + norm_row(channel, r.split(",")) for r in rows],
    )
    return len(rows)


def rebuild_dws(con):
    """由 dwd_clean 全量重算 dws_mchday。"""
    voided = {r[0] for r in con.execute("SELECT ref FROM dwd_clean WHERE type = 'void'")}
    agg = {}  # (mch, day) -> [cnt, net]
    for mch, ty, amt, ts, txn_id in con.execute(
        "SELECT mch, type, amount, ts, txn_id FROM dwd_clean"
    ):
        day = ts[:10]
        if ty == "pay":
            if txn_id in voided:
                continue
            slot = agg.setdefault((mch, day), [0, 0])
            slot[0] += 1
            slot[1] += amt
        elif ty == "refund":
            slot = agg.setdefault((mch, day), [0, 0])
            slot[1] -= amt
    con.execute("DELETE FROM dws_mchday")
    con.executemany(
        "INSERT INTO dws_mchday (mch, day, cnt, net) VALUES (?, ?, ?, ?)",
        [(m, d, c, n) for (m, d), (c, n) in sorted(agg.items())],
    )


def reorder(con, table, order_by):
    cols = {
        "ods_raw": "src, row_no, raw",
        "dwd_clean": "src, txn_id, mch, type, amount, ts, ref",
    }[table]
    con.execute(
        f"CREATE TABLE {table}_sorted AS SELECT {cols} FROM {table} ORDER BY {order_by}"
    )
    con.execute(f"DROP TABLE {table}")
    con.execute(f"ALTER TABLE {table}_sorted RENAME TO {table}")


def main():
    with open(STATE_PATH, encoding="utf-8") as f:
        state = json.load(f)
    done = list(state["done"])
    done_set = set(done)

    with open(LOG_PATH, newline="", encoding="utf-8") as f:
        log_rows = list(csv.reader(f))
    header, entries = log_rows[0], log_rows[1:]

    files = sorted(
        fn for fn in os.listdir(FILES_DIR)
        if fn.endswith(".csv") and fn[:3] in ("cha", "chb", "chc")
    )

    con = sqlite3.connect(DB_PATH)
    processed = False
    try:
        for fn in files:
            src = fn[:-4]
            if src in done_set:
                continue
            leftover = con.execute(
                "SELECT (SELECT COUNT(*) FROM ods_raw WHERE src = ?)"
                " + (SELECT COUNT(*) FROM dwd_clean WHERE src = ?)",
                (src, src),
            ).fetchone()[0]
            outcome = "redone" if leftover else "loaded"
            with con:  # 每批一个事务
                n = load_batch(con, os.path.join(FILES_DIR, fn), src)
            entries.append([src, outcome, str(n)])
            done.append(src)
            done_set.add(src)
            processed = True
            print(f"{src}: {outcome} ({n} rows)")

        if processed:
            with con:
                rebuild_dws(con)
                reorder(con, "ods_raw", "src, row_no")
                reorder(con, "dwd_clean", "src, txn_id")
    finally:
        con.close()

    with open(LOG_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(header)
        w.writerows(entries)

    with open(STATE_PATH, "w", encoding="utf-8") as f:
        f.write(json.dumps({"done": done}, indent=1) + "\n")

    with open(NOTES_PATH, "w", encoding="utf-8") as f:
        f.write(NOTES)


if __name__ == "__main__":
    sys.exit(main())
