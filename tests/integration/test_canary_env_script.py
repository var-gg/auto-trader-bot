from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / 'scripts' / 'check_canary_env.py'


def _run(env):
    proc = subprocess.run([sys.executable, str(SCRIPT)], env=env, capture_output=True, text=True)
    return proc.returncode, proc.stdout


def test_canary_env_script_fails_on_placeholder_account():
    env = os.environ.copy()
    env.update({
        'KIS_APPKEY': 'key',
        'KIS_APPSECRET': 'secret',
        'KIS_CANO': '00000000',
        'KIS_ACNT_PRDT_CD': '01',
        'KIS_VIRTUAL': 'false',
    })
    code, out = _run(env)
    assert code == 2
    assert 'KIS_CANO is missing or placeholder' in out


def test_canary_env_script_passes_on_valid_minimum_env():
    env = os.environ.copy()
    env.update({
        'KIS_APPKEY': 'key',
        'KIS_APPSECRET': 'secret',
        'KIS_CANO': '12345678',
        'KIS_ACNT_PRDT_CD': '01',
        'KIS_VIRTUAL': 'false',
        'KIS_VIRTUAL_CANO': '',
    })
    code, out = _run(env)
    assert code == 0
    assert '"ok": true' in out
