LANGUAGE_CONFIGS = {
    "python": {
        "suffix": "py",
        "compile": None,
        "run": "python3 /tmp/script.py",
        "test": "pytest /tmp/script_test.py",
        "kill": "pkill -9 python3"
    }
}