# -----------------------------------------------
# LOAD EXISTING PRICES
# -----------------------------------------------

existing_price_rows = noco_get_all(
    TABLE_PRICES,
    where=f"(company_id,eq,{company_id})"
)

price_map = {}

for r in existing_price_rows:

    key = f"{r['variant_id']}_{r['price_list_id']}"

    price_map[key] = {
        "Id": r["Id"],
        "price_net": r.get("price_net"),
        "price_gross": r.get("price_gross")
    }

print("EXISTING PRICE ROWS:", len(existing_price_rows))


# -----------------------------------------------
# SYNC PRICES
# -----------------------------------------------

insert_prices = []
update_prices = []

for pl in price_lists:

    price_list_id = pl["id"]

    print("SYNC PRICE LIST:", price_list_id)

    offset = 0

    while True:

        data = safe_get(
            f"{BASE}/price_lists/{price_list_id}/details.json",
            HEAD_BSALE,
            {"limit": BSALE_LIMIT, "offset": offset}
        )

        items = data.get("items", [])

        if not items:
            break

        for d in items:

            variant_id = d["variant"]["id"]

            price_net = d.get("variantValue")
            price_gross = d.get("variantValueWithTaxes")

            key = f"{variant_id}_{price_list_id}"

            payload = {
                "company_id": company_id,
                "variant_id": variant_id,
                "price_list_id": price_list_id,
                "price_net": price_net,
                "price_gross": price_gross
            }

            if key in price_map:

                current = price_map[key]

                if (
                    current["price_net"] != price_net
                    or current["price_gross"] != price_gross
                ):

                    payload["Id"] = current["Id"]
                    update_prices.append(payload)

            else:

                insert_prices.append(payload)

        offset += BSALE_LIMIT

        print("PRICE OFFSET", offset)

# write batches
batch_insert(TABLE_PRICES, insert_prices)
batch_update(TABLE_PRICES, update_prices)

print("PRICE INSERTED:", len(insert_prices))
print("PRICE UPDATED:", len(update_prices))
