#!/usr/bin/env python3
"""Exercise real shopping journeys using only synthetic customer data."""

import datetime
import http.cookiejar
import json
from pathlib import Path
import re
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:8080"
PRODUCT = "0PUK6V6EV0"


def main():
    directory = ROOT / "outputs/boutique-smoke" / datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    directory.mkdir(parents=True)
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    evidence = []

    def request(label, path, values=None, contains=None):
        data = urllib.parse.urlencode(values).encode() if values is not None else None
        started = time.monotonic()
        with opener.open(BASE + path, data=data, timeout=30) as response:
            page = response.read().decode()
            assert response.status == 200, label
        if contains:
            assert contains in page, "Expected content missing: " + label
        (directory / (label + ".html")).write_text(page)
        evidence.append({"step": label, "path": path, "method": "POST" if data is not None else "GET",
                         "status": 200, "elapsed_ms": round((time.monotonic() - started) * 1000, 2),
                         "content_assertion": contains})
        return page

    request("browse", "/", contains="Online Boutique")
    request("product", "/product/" + PRODUCT, contains=PRODUCT)
    request("clear-cart", "/cart/empty", {})
    first = request("cart-one", "/cart", {"product_id": PRODUCT, "quantity": 1})
    second = request("cart-three", "/cart", {"product_id": PRODUCT, "quantity": 2})
    assert re.search(r"Quantity:\s*(?:<[^>]+>\s*)*1\b", first, re.I), "Cart quantity 1 not confirmed"
    assert re.search(r"Quantity:\s*(?:<[^>]+>\s*)*3\b", second, re.I), "Cart quantity 3 not confirmed"
    request("checkout", "/cart/checkout", {
        "email": "cct-test@example.com", "street_address": "123 Test Street", "zip_code": "11790",
        "city": "Stony Brook", "state": "NY", "country": "United States",
        "credit_card_number": "4111111111111111", "credit_card_expiration_month": "12",
        "credit_card_expiration_year": str(datetime.datetime.now().year + 1), "credit_card_cvv": "123",
    }, contains="Your order is complete!")
    request("cart-after-checkout", "/cart", contains="Your shopping cart is empty!")
    (directory / "summary.json").write_text(json.dumps({"passed": True, "synthetic_customer": True,
                                                       "steps": evidence}, indent=2) + "\n")
    print("Shopping journeys passed. Evidence: " + str(directory))


if __name__ == "__main__":
    main()
