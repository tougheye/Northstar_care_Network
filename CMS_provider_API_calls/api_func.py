import requests

cms_api_link = "https://data.cms.gov/data-api/v1/dataset/92396110-2aed-4d63-a6a2-5d6207d46a29/data"

def fetch_cms(provider_type):
    PAGE_SIZE = 5000
    records, offset = [], 0
    while True:
        resp = requests.get(
            cms_api_link,
            params={
                "filter[Rndrng_Prvdr_Type]": provider_type,  # requests URL-encodes brackets and spaces
                "size": PAGE_SIZE,
                "offset": offset,
            },
            timeout=60,
        )
        resp.raise_for_status()
        page = resp.json()
        if not page:                 # empty page = no more data
            break
        records.extend(page)
        if len(page) < PAGE_SIZE:    # short page = last page
            break
        offset += PAGE_SIZE
    return records


data = fetch_cms("Hospitalist")
print(len(data))