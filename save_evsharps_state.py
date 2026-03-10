from playwright.sync_api import sync_playwright

OUT_PATH = "evsharps_storage_state.json"

with sync_playwright() as pw:
    browser = pw.chromium.connect_over_cdp("http://127.0.0.1:9222")
    contexts = browser.contexts
    if not contexts:
        raise RuntimeError("No browser contexts found")
    context = contexts[0]
    context.storage_state(path=OUT_PATH)
    print(f"Saved storage state to: {OUT_PATH}")