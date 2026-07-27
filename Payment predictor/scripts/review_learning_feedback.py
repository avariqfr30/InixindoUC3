#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from learning_feedback import create_feedback_store

parser = argparse.ArgumentParser(); sub = parser.add_subparsers(dest="command", required=True); sub.add_parser("list")
approve = sub.add_parser("approve"); approve.add_argument("reason_code"); approve.add_argument("--reviewer", required=True)
retire = sub.add_parser("retire"); retire.add_argument("reason_code")
args = parser.parse_args(); store = create_feedback_store()
if args.command == "list": print(json.dumps(store.review_snapshot(), ensure_ascii=False, indent=2))
elif args.command == "approve": store.approve_reason(args.reason_code, approved_by=args.reviewer); print("Panduan feedback diaktifkan untuk UC3.")
else: store.retire_reason(args.reason_code); print("Panduan feedback dinonaktifkan untuk UC3.")
